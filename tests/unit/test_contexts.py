"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.api.controller import KarapaceSchemaRegistryController
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from karapace.api.middlewares.contexts import ContextPathMiddleware, rewrite_context_path
from karapace.api.routers.contexts import contexts_get
from karapace.api.routers.raw_path_router import RawPathRoute
from karapace.core.config import Config
from karapace.core.contexts import context_of, normalize_context, qualify, split_subject
from karapace.core.in_memory_database import InMemoryDatabase
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.typing import Mode, SchemaId, Subject, Version
from unittest.mock import Mock
from urllib.parse import unquote_plus

import asyncio
import json
import pytest

SCHEMA_X = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps({"type": "record", "name": "Order", "namespace": "a", "fields": [{"name": "orderId", "type": "long"}]}),
)
SCHEMA_Y = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps({"type": "record", "name": "Order", "namespace": "b", "fields": [{"name": "id", "type": "string"}]}),
)


def insert(db: InMemoryDatabase, subject: str, schema_id: int, schema: ValidatedTypedSchema, version: int = 1) -> None:
    """Insert as the schema reader does when it replays the schemas topic."""
    db.insert_schema_version(
        subject=Subject(subject),
        schema_id=SchemaId(schema_id),
        version=Version(version),
        deleted=False,
        schema=schema,
        references=None,
    )


def collision_db() -> InMemoryDatabase:
    """Id 1 in the default context and id 1 in context .A, two different schemas."""
    db = InMemoryDatabase()
    insert(db, "orders-value", 1, SCHEMA_Y)
    insert(db, ":.A:A.orders-value", 1, SCHEMA_X)
    return db


def make_registry(db: InMemoryDatabase) -> tuple[Mock, list[dict]]:
    sent: list[dict] = []
    registry = Mock(spec=KarapaceSchemaRegistry)
    registry.database = db
    registry.schema_lock = asyncio.Lock()
    registry.config = Config()
    registry.send_schema_message = Mock(side_effect=lambda **kw: sent.append(kw))
    for name in (
        "get_global_mode",
        "get_subject_mode",
        "get_live_versions_sorted",
        "_write_new_schema_import_mode",
        "write_new_schema_local",
        "schemas_get",
        "resolve_schema_context",
        "_subject_holds_schema_id",
    ):
        setattr(registry, name, getattr(KarapaceSchemaRegistry, name).__get__(registry))
    return registry, sent


class TestSubjectNames:
    @pytest.mark.parametrize(
        ("subject", "expected"),
        [
            ("orders-value", (".", "orders-value")),
            (":.A:A.orders-value", (".A", "A.orders-value")),
            (":.A:", (".A", "")),
            (":.:orders", (".", "orders")),
            (":.A.B:x", (".A.B", "x")),
            # A malformed prefix is no context: the whole string is the subject.
            (":.A", (".", ":.A")),
            (":A:x", (".", ":A:x")),
            ("", (".", "")),
        ],
    )
    def test_split_subject(self, subject: str, expected: tuple[str, str]) -> None:
        assert split_subject(subject) == expected
        assert context_of(subject) == expected[0]

    @pytest.mark.parametrize(
        ("context", "expected"), [("A", ".A"), (".A", ".A"), (":.A:", ".A"), (".", "."), ("", "."), (":.:", ".")]
    )
    def test_normalize_context(self, context: str, expected: str) -> None:
        assert normalize_context(context) == expected

    def test_qualify(self) -> None:
        assert qualify(".A", "orders-value") == ":.A:orders-value"
        assert qualify("A", "") == ":.A:"
        assert qualify(".", "orders-value") == "orders-value"


class TestInMemoryDatabaseContexts:
    def test_same_id_in_two_contexts_keeps_both_schemas(self) -> None:
        db = collision_db()
        assert db.find_schema(schema_id=SchemaId(1)) == SCHEMA_Y
        assert db.find_schema(schema_id=SchemaId(1), context=".A") == SCHEMA_X
        assert db.find_schema(schema_id=SchemaId(1), context=".B") is None
        assert db.num_schemas() == 2

    def test_id_counters_are_per_context(self) -> None:
        db = collision_db()
        insert(db, ":.A:A.orders-value", 5, SCHEMA_Y, version=2)
        assert db.global_schema_id == 1
        assert db.max_schema_id() == 1
        assert db.max_schema_id(".A") == 5
        # New ids continue after the highest id of their own context only.
        assert db.get_schema_id(ValidatedTypedSchema.parse(SchemaType.AVRO, '"string"')) == 2
        assert db.get_schema_id(ValidatedTypedSchema.parse(SchemaType.AVRO, '"string"'), context=".A") == 6

    def test_get_schema_id_reuses_ids_within_a_context_only(self) -> None:
        db = collision_db()
        assert db.get_schema_id(SCHEMA_Y) == 1
        assert db.get_schema_id(SCHEMA_X, context=".A") == 1
        # The schema of .A is unknown in the default context: it gets a new id there.
        assert db.get_schema_id(SCHEMA_X) == 2
        assert db.find_schema_id(schema=SCHEMA_X) is None
        assert db.find_schema_id(schema=SCHEMA_X, context=".A") == 1

    def test_subjects_and_versions_by_id_are_per_context(self) -> None:
        db = collision_db()
        assert db.subjects_for_schema(SchemaId(1)) == ["orders-value"]
        assert db.subjects_for_schema(SchemaId(1), context=".A") == [":.A:A.orders-value"]
        versions = db.find_schema_versions_by_schema_id(schema_id=SchemaId(1), include_deleted=False, context=".A")
        assert [v.subject for v in versions] == [":.A:A.orders-value"]

    def test_contexts(self) -> None:
        assert InMemoryDatabase().contexts() == ["."]
        db = collision_db()
        insert(db, ":.B:x", 1, SCHEMA_X)
        assert db.contexts() == [".", ".A", ".B"]


class TestResolveSchemaContext:
    @pytest.mark.parametrize(
        ("subject", "expected"),
        [
            (None, "."),
            ("orders-value", "."),
            # Unqualified, unknown in the default context: found by its name in .A.
            ("A.orders-value", ".A"),
            (":.A:A.orders-value", ".A"),
            # A context alone, as sent for /contexts/.A/schemas/ids/{id}.
            (":.A:", ".A"),
            # A named context is never left, even if the id is not there.
            (":.B:A.orders-value", ".B"),
            # Unknown everywhere: the default context.
            ("unknown-value", "."),
            ("", "."),
        ],
    )
    def test_resolution(self, subject: str | None, expected: str) -> None:
        registry, _ = make_registry(collision_db())
        assert registry.resolve_schema_context(SchemaId(1), subject) == expected

    def test_default_context_wins_when_the_subject_holds_the_id_there(self) -> None:
        db = collision_db()
        insert(db, ":.A:orders-value", 1, SCHEMA_X)
        registry, _ = make_registry(db)
        assert registry.resolve_schema_context(SchemaId(1), "orders-value") == "."

    def test_schemas_get_in_context(self) -> None:
        db = collision_db()
        insert(db, ":.A:A.orders-value", 7, SCHEMA_Y, version=2)
        registry, _ = make_registry(db)
        assert registry.schemas_get(SchemaId(1)) == SCHEMA_Y
        schema = registry.schemas_get(SchemaId(1), fetch_max_id=True, context=".A")
        assert schema == SCHEMA_X
        assert schema.max_id == 7


class TestWriteNewSchemaInContext:
    async def test_new_ids_are_allocated_per_context(self) -> None:
        registry, sent = make_registry(InMemoryDatabase())
        assert await registry.write_new_schema_local(Subject("orders-value"), SCHEMA_Y, None) == 1
        assert await registry.write_new_schema_local(Subject(":.A:A.orders-value"), SCHEMA_X, None) == 1
        assert [(m["subject"], m["schema_id"]) for m in sent] == [("orders-value", 1), (":.A:A.orders-value", 1)]

    async def test_import_with_an_id_taken_in_another_context(self) -> None:
        db = InMemoryDatabase()
        insert(db, "orders-value", 5, SCHEMA_Y)
        db.insert_subject(subject=Subject(":.A:A.orders-value"))
        db.set_subject_mode(subject=Subject(":.A:A.orders-value"), mode=Mode.import_mode)
        registry, sent = make_registry(db)

        result = await registry.write_new_schema_local(
            Subject(":.A:A.orders-value"), SCHEMA_X, None, explicit_schema_id=SchemaId(5)
        )
        assert result == 5
        assert sent[0]["schema_id"] == 5


class TestContextPathRewrite:
    @pytest.mark.parametrize(
        ("raw_path", "query", "expected"),
        [
            (
                "/contexts/.A/subjects/A.orders-value/versions",
                "normalize=false",
                ("/subjects/:.A:A.orders-value/versions", "normalize=false"),
            ),
            ("/contexts/A/subjects/s", "", ("/subjects/:.A:s", "")),
            ("/contexts/.A/config/s", "", ("/config/:.A:s", "")),
            ("/contexts/.A/mode/s", "", ("/mode/:.A:s", "")),
            # The default context leaves the subject unqualified.
            ("/contexts/:.:/subjects/s/versions", "", ("/subjects/s/versions", "")),
            ("/contexts/.A/schemas/ids/1", "fetchMaxId=false", ("/schemas/ids/1", "fetchMaxId=false&subject=%3A.A%3A")),
            ("/contexts/.A/schemas/ids/1", "subject=s", ("/schemas/ids/1", "subject=%3A.A%3As")),
            ("/contexts/.A/schemas/ids/1/schema", "", ("/schemas/ids/1/schema", "subject=%3A.A%3A")),
        ],
    )
    def test_rewritten(self, raw_path: str, query: str, expected: tuple[str, str]) -> None:
        assert rewrite_context_path(raw_path, query) == expected

    @pytest.mark.parametrize(
        "raw_path",
        ["/subjects/s", "/contexts", "/contexts/.A", "/contexts/.A/subjects", "/contexts/.A/mode", "/contexts/.A/schemas"],
    )
    def test_not_rewritten(self, raw_path: str) -> None:
        assert rewrite_context_path(raw_path, "") is None

    async def test_middleware_rewrites_path_and_raw_path(self) -> None:
        seen: list[dict] = []

        async def app(scope, receive, send) -> None:
            seen.append(scope)

        middleware = ContextPathMiddleware(app)
        await middleware(
            {
                "type": "http",
                "path": "/contexts/.A/subjects/a%2Fb",
                "raw_path": b"/contexts/.A/subjects/a%2Fb",
                "query_string": b"",
            },
            None,
            None,
        )
        assert seen[0]["raw_path"] == b"/subjects/:.A:a%2Fb"
        assert seen[0]["path"] == "/subjects/:.A:a/b"

        await middleware({"type": "http", "path": "/subjects", "raw_path": b"/subjects", "query_string": b""}, None, None)
        assert seen[1]["raw_path"] == b"/subjects"


def test_context_paths_reach_the_existing_routes() -> None:
    """End to end through the middleware and RawPathRoute, which matches on the raw path."""
    router = APIRouter(route_class=RawPathRoute)

    @router.post("/subjects/{subject}/versions")
    async def subject_versions(subject: str) -> dict:
        return {"subject": unquote_plus(subject)}

    @router.get("/schemas/ids/{schema_id}")
    async def schema_by_id(schema_id: int, subject: str | None = None) -> dict:
        return {"id": schema_id, "subject": subject}

    app = FastAPI()
    app.include_router(router)
    app.add_middleware(ContextPathMiddleware)
    client = TestClient(app)

    res = client.post("/contexts/.A/subjects/subj%2Fect/versions")
    assert res.json() == {"subject": ":.A:subj/ect"}
    res = client.get("/contexts/.A/schemas/ids/1", params={"fetchMaxId": "false"})
    assert res.json() == {"id": 1, "subject": ":.A:"}
    res = client.get("/schemas/ids/1", params={"subject": "A.orders-value"})
    assert res.json() == {"id": 1, "subject": "A.orders-value"}


async def test_contexts_route() -> None:
    registry = Mock(spec=KarapaceSchemaRegistry)
    registry.database = collision_db()
    assert await contexts_get(user=None, schema_registry=registry) == [".", ".A"]


async def test_controller_schemas_get_uses_the_subject() -> None:
    registry, _ = make_registry(collision_db())
    controller = KarapaceSchemaRegistryController(config=Config(), schema_registry=registry, stats=Mock())

    async def get(subject: str | None, include_subjects: bool = False):
        return await controller.schemas_get(
            schema_id="1",
            fetch_max_id=False,
            include_subjects=include_subjects,
            format_serialized="",
            user=None,
            subject=subject,
            authorizer=None,
        )

    assert json.loads((await get(None)).schema_str)["namespace"] == "b"
    assert json.loads((await get("A.orders-value")).schema_str)["namespace"] == "a"
    response = await get(":.A:", include_subjects=True)
    assert json.loads(response.schema_str)["namespace"] == "a"
    assert response.subjects == [":.A:A.orders-value"]
