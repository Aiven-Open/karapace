"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import json
import pytest

from unittest.mock import AsyncMock, MagicMock, Mock

from karapace.api.routers.requests import ModeUpdateRequest, SchemaRequest
from karapace.core.errors import InvalidMode, OperationNotPermittedInMode, SchemaIdConflict
from karapace.core.in_memory_database import InMemoryDatabase
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema
from karapace.core.schema_reader import KafkaSchemaReader, MessageType
from karapace.core.typing import Mode, SchemaId, Subject, Version


AVRO_SCHEMA_STR = json.dumps({"type": "record", "name": "Test", "fields": [{"name": "f", "type": "string"}]})


def make_validated_schema(schema_str: str = AVRO_SCHEMA_STR) -> ValidatedTypedSchema:
    return ValidatedTypedSchema.parse(SchemaType.AVRO, schema_str)


class TestInMemoryDatabaseModes:
    def test_global_mode_defaults_to_readwrite(self) -> None:
        db = InMemoryDatabase()
        assert db.get_global_mode() == Mode.readwrite

    def test_set_and_get_global_mode(self) -> None:
        db = InMemoryDatabase()
        for mode in Mode:
            db.set_global_mode(mode=mode)
            assert db.get_global_mode() == mode

    def test_subject_mode_is_none_when_not_set(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("subj"))
        assert db.get_subject_mode(subject=Subject("subj")) is None

    def test_set_and_get_subject_mode(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("subj"))
        db.set_subject_mode(subject=Subject("subj"), mode=Mode.import_mode)
        assert db.get_subject_mode(subject=Subject("subj")) == Mode.import_mode

    def test_delete_subject_mode_reverts_to_none(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("subj"))
        db.set_subject_mode(subject=Subject("subj"), mode=Mode.import_mode)
        db.delete_subject_mode(subject=Subject("subj"))
        assert db.get_subject_mode(subject=Subject("subj")) is None

    def test_set_subject_mode_noop_for_unknown_subject(self) -> None:
        """Setting mode on an unknown subject should not raise."""
        db = InMemoryDatabase()
        db.set_subject_mode(subject=Subject("ghost"), mode=Mode.import_mode)
        assert db.get_subject_mode(subject=Subject("ghost")) is None

    def test_delete_subject_mode_noop_for_unknown_subject(self) -> None:
        db = InMemoryDatabase()
        db.delete_subject_mode(subject=Subject("ghost"))  # must not raise

    def test_subject_modes_are_independent(self) -> None:
        db = InMemoryDatabase()
        s1, s2 = Subject("s1"), Subject("s2")
        db.insert_subject(subject=s1)
        db.insert_subject(subject=s2)
        db.set_subject_mode(subject=s1, mode=Mode.import_mode)
        db.set_subject_mode(subject=s2, mode=Mode.readwrite)
        assert db.get_subject_mode(subject=s1) == Mode.import_mode
        assert db.get_subject_mode(subject=s2) == Mode.readwrite

    def test_global_mode_does_not_affect_subject_mode_storage(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("subj"))
        db.set_global_mode(mode=Mode.import_mode)
        # Subject has no explicit mode — storage is still None
        assert db.get_subject_mode(subject=Subject("subj")) is None


class TestMessageTypeEnum:
    def test_mode_message_type_exists(self) -> None:
        assert MessageType.mode.value == "MODE"

    def test_all_expected_types_present(self) -> None:
        values = {mt.value for mt in MessageType}
        assert values >= {"CONFIG", "SCHEMA", "DELETE_SUBJECT", "NOOP", "MODE"}


class TestHandleMsgMode:
    """Tests for the MODE Kafka message handler in KafkaSchemaReader."""

    def _make_reader(self) -> tuple[Mock, InMemoryDatabase]:
        db = InMemoryDatabase()
        reader = Mock(spec=KafkaSchemaReader)
        reader.database = db
        return reader, db

    def test_sets_global_mode(self) -> None:
        reader, db = self._make_reader()
        KafkaSchemaReader._handle_msg_mode(reader, {"keytype": "MODE", "subject": None, "magic": 0}, {"mode": "IMPORT"})
        assert db.get_global_mode() == Mode.import_mode

    def test_sets_subject_mode(self) -> None:
        reader, db = self._make_reader()
        db.insert_subject(subject=Subject("my-topic"))
        KafkaSchemaReader._handle_msg_mode(
            reader, {"keytype": "MODE", "subject": "my-topic", "magic": 0}, {"mode": "IMPORT"}
        )
        assert db.get_subject_mode(subject=Subject("my-topic")) == Mode.import_mode

    def test_deletes_subject_mode_on_null_value(self) -> None:
        reader, db = self._make_reader()
        db.insert_subject(subject=Subject("my-topic"))
        db.set_subject_mode(subject=Subject("my-topic"), mode=Mode.import_mode)
        KafkaSchemaReader._handle_msg_mode(reader, {"keytype": "MODE", "subject": "my-topic", "magic": 0}, None)
        assert db.get_subject_mode(subject=Subject("my-topic")) is None

    def test_auto_inserts_subject_if_missing(self) -> None:
        """A MODE message for an unknown subject should create it."""
        reader, db = self._make_reader()
        KafkaSchemaReader._handle_msg_mode(
            reader, {"keytype": "MODE", "subject": "new-subj", "magic": 0}, {"mode": "IMPORT"}
        )
        assert db.find_subject(subject=Subject("new-subj")) is not None
        assert db.get_subject_mode(subject=Subject("new-subj")) == Mode.import_mode

    def test_handle_msg_dispatches_mode_keytype(self, karapace_container) -> None:
        """handle_msg must route MODE keytype to _handle_msg_mode."""
        db = InMemoryDatabase()
        reader = KafkaSchemaReader(
            config=karapace_container.config(),
            offset_watcher=MagicMock(),
            key_formatter=MagicMock(),
            master_coordinator=None,
            database=db,
            stats=Mock(),
        )
        reader.handle_msg({"keytype": "MODE", "subject": None, "magic": 0}, {"mode": "IMPORT"})
        assert db.get_global_mode() == Mode.import_mode

    def test_sets_all_valid_mode_values_globally(self) -> None:
        for mode in Mode:
            reader, db = self._make_reader()
            KafkaSchemaReader._handle_msg_mode(
                reader, {"keytype": "MODE", "subject": None, "magic": 0}, {"mode": mode.value}
            )
            assert db.get_global_mode() == mode


def _make_registry_stub() -> tuple:
    """Return (registry_mock, db) with real mode methods bound."""
    from karapace.core.schema_registry import KarapaceSchemaRegistry

    db = InMemoryDatabase()
    producer_mock = Mock()
    producer_mock.send_message = Mock()

    registry = Mock(spec=KarapaceSchemaRegistry)
    registry.database = db
    registry.producer = producer_mock

    registry.get_global_mode = KarapaceSchemaRegistry.get_global_mode.__get__(registry)
    registry.get_subject_mode = KarapaceSchemaRegistry.get_subject_mode.__get__(registry)
    registry.send_mode_message = KarapaceSchemaRegistry.send_mode_message.__get__(registry)
    registry.send_mode_delete_message = KarapaceSchemaRegistry.send_mode_delete_message.__get__(registry)

    return registry, db, producer_mock


class TestSchemaRegistryModeMethods:
    def test_get_global_mode_returns_database_value(self) -> None:
        registry, db, _ = _make_registry_stub()
        assert registry.get_global_mode() == Mode.readwrite
        db.set_global_mode(mode=Mode.import_mode)
        assert registry.get_global_mode() == Mode.import_mode

    def test_get_subject_mode_falls_back_to_global(self) -> None:
        registry, db, _ = _make_registry_stub()
        db.insert_subject(subject=Subject("s"))
        db.set_global_mode(mode=Mode.import_mode)
        assert registry.get_subject_mode(Subject("s")) == Mode.import_mode

    def test_get_subject_mode_returns_subject_override(self) -> None:
        registry, db, _ = _make_registry_stub()
        db.insert_subject(subject=Subject("s"))
        db.set_global_mode(mode=Mode.readwrite)
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)
        assert registry.get_subject_mode(Subject("s")) == Mode.import_mode

    def test_send_mode_message_global(self) -> None:
        registry, _, producer_mock = _make_registry_stub()
        registry.send_mode_message(Mode.import_mode)
        producer_mock.send_message.assert_called_once_with(
            key={"subject": None, "magic": 0, "keytype": "MODE"},
            value={"mode": "IMPORT"},
        )

    def test_send_mode_message_subject(self) -> None:
        registry, _, producer_mock = _make_registry_stub()
        registry.send_mode_message(Mode.import_mode, subject=Subject("s"))
        producer_mock.send_message.assert_called_once_with(
            key={"subject": Subject("s"), "magic": 0, "keytype": "MODE"},
            value={"mode": "IMPORT"},
        )

    def test_send_mode_delete_message(self) -> None:
        registry, _, producer_mock = _make_registry_stub()
        registry.send_mode_delete_message(Subject("s"))
        producer_mock.send_message.assert_called_once_with(
            key={"subject": Subject("s"), "magic": 0, "keytype": "MODE"},
            value=None,
        )


def _make_registry_for_set_mode():
    from karapace.core.schema_registry import KarapaceSchemaRegistry
    import asyncio

    db = InMemoryDatabase()
    sent: list = []

    registry = Mock(spec=KarapaceSchemaRegistry)
    registry.database = db
    registry.schema_lock = asyncio.Lock()
    registry.send_schema_message = Mock(side_effect=lambda **kw: sent.append(("schema", kw)))
    registry.send_mode_message = Mock(side_effect=lambda mode, subject=None: sent.append(("mode", mode, subject)))
    registry.get_subject_mode = KarapaceSchemaRegistry.get_subject_mode.__get__(registry)
    registry.get_global_mode = KarapaceSchemaRegistry.get_global_mode.__get__(registry)

    set_mode_local = KarapaceSchemaRegistry.set_mode_local.__get__(registry)
    return registry, db, sent, set_mode_local


class TestSetModeLocal:
    async def test_set_global_mode(self) -> None:
        _, _, sent, set_mode_local = _make_registry_for_set_mode()
        result = await set_mode_local(Mode.import_mode)
        assert result == Mode.import_mode
        assert ("mode", Mode.import_mode, None) in sent

    async def test_set_subject_mode(self) -> None:
        _, db, sent, set_mode_local = _make_registry_for_set_mode()
        db.insert_subject(subject=Subject("s"))
        result = await set_mode_local(Mode.import_mode, subject=Subject("s"))
        assert result == Mode.import_mode
        assert ("mode", Mode.import_mode, Subject("s")) in sent

    async def test_import_mode_fails_if_schemas_exist_without_force(self) -> None:
        _, db, _, set_mode_local = _make_registry_for_set_mode()
        db.insert_subject(subject=Subject("s"))
        db.insert_schema_version(
            subject=Subject("s"),
            schema_id=SchemaId(1),
            version=Version(1),
            deleted=False,
            schema=make_validated_schema(),
            references=None,
        )
        with pytest.raises(OperationNotPermittedInMode):
            await set_mode_local(Mode.import_mode, subject=Subject("s"), force=False)

    async def test_import_mode_with_force_hard_deletes_existing_schemas(self) -> None:
        _, db, sent, set_mode_local = _make_registry_for_set_mode()
        db.insert_subject(subject=Subject("s"))
        db.insert_schema_version(
            subject=Subject("s"),
            schema_id=SchemaId(1),
            version=Version(1),
            deleted=False,
            schema=make_validated_schema(),
            references=None,
        )
        await set_mode_local(Mode.import_mode, subject=Subject("s"), force=True)
        schema_deletes = [m for m in sent if m[0] == "schema"]
        mode_msgs = [m for m in sent if m[0] == "mode"]
        assert len(schema_deletes) >= 1
        assert len(mode_msgs) == 1

    async def test_import_mode_with_force_on_empty_subject_skips_deletes(self) -> None:
        _, db, sent, set_mode_local = _make_registry_for_set_mode()
        db.insert_subject(subject=Subject("s"))
        await set_mode_local(Mode.import_mode, subject=Subject("s"), force=True)
        assert [m for m in sent if m[0] == "schema"] == []

    async def test_invalid_mode_raises_invalid_mode(self) -> None:
        _, _, _, set_mode_local = _make_registry_for_set_mode()
        with pytest.raises(InvalidMode):
            await set_mode_local("BOGUS_MODE")  # type: ignore[arg-type]

    async def test_readwrite_mode_allowed_even_with_schemas(self) -> None:
        _, db, sent, set_mode_local = _make_registry_for_set_mode()
        db.insert_subject(subject=Subject("s"))
        db.insert_schema_version(
            subject=Subject("s"),
            schema_id=SchemaId(1),
            version=Version(1),
            deleted=False,
            schema=make_validated_schema(),
            references=None,
        )
        result = await set_mode_local(Mode.readwrite, subject=Subject("s"))
        assert result == Mode.readwrite


def _make_registry_for_delete_mode():
    from karapace.core.schema_registry import KarapaceSchemaRegistry
    import asyncio

    db = InMemoryDatabase()
    sent: list = []

    registry = Mock(spec=KarapaceSchemaRegistry)
    registry.database = db
    registry.schema_lock = asyncio.Lock()

    def _delete_msg(subject: Subject) -> None:
        sent.append(("delete_mode", subject))

    registry.send_mode_delete_message = Mock(side_effect=_delete_msg)
    registry.get_global_mode = KarapaceSchemaRegistry.get_global_mode.__get__(registry)

    delete_mode_local = KarapaceSchemaRegistry.delete_mode_local.__get__(registry)
    return registry, db, sent, delete_mode_local


class TestDeleteModeLocal:
    async def test_returns_current_global_mode(self) -> None:
        _, db, _, delete_mode_local = _make_registry_for_delete_mode()
        db.insert_subject(subject=Subject("s"))
        db.set_global_mode(mode=Mode.import_mode)
        result = await delete_mode_local(Subject("s"))
        assert result == Mode.import_mode

    async def test_sends_mode_delete_message(self) -> None:
        _, db, sent, delete_mode_local = _make_registry_for_delete_mode()
        db.insert_subject(subject=Subject("s"))
        await delete_mode_local(Subject("s"))
        assert ("delete_mode", Subject("s")) in sent


def _make_registry_for_write():
    from karapace.core.schema_registry import KarapaceSchemaRegistry
    import asyncio

    db = InMemoryDatabase()
    sent: list = []

    registry = Mock(spec=KarapaceSchemaRegistry)
    registry.database = db
    registry.schema_lock = asyncio.Lock()
    registry.send_schema_message = Mock(side_effect=lambda **kw: sent.append(kw))
    registry.get_global_mode = KarapaceSchemaRegistry.get_global_mode.__get__(registry)
    registry.get_subject_mode = KarapaceSchemaRegistry.get_subject_mode.__get__(registry)
    registry._write_new_schema_import_mode = KarapaceSchemaRegistry._write_new_schema_import_mode.__get__(registry)

    write_new = KarapaceSchemaRegistry.write_new_schema_local.__get__(registry)
    return registry, db, sent, write_new


class TestWriteNewSchemaLocalImportMode:
    async def test_import_mode_uses_explicit_schema_id(self) -> None:
        registry, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s"))
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)

        result = await write_new(Subject("s"), make_validated_schema(), None, explicit_schema_id=SchemaId(999))
        assert result == 999
        assert sent[0]["schema_id"] == 999

    async def test_import_mode_uses_explicit_version(self) -> None:
        registry, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s"))
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)

        await write_new(Subject("s"), make_validated_schema(), None, explicit_version=Version(42))
        assert sent[0]["version"] == Version(42)

    async def test_import_mode_skips_compatibility_check(self) -> None:
        """Incompatible schemas are accepted in IMPORT mode without raising."""
        registry, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s"))
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)

        schema_v1 = make_validated_schema(AVRO_SCHEMA_STR)
        db.insert_schema_version(
            subject=Subject("s"),
            schema_id=SchemaId(1),
            version=Version(1),
            deleted=False,
            schema=schema_v1,
            references=None,
        )

        # Incompatible schema (removed field, no default)
        incompatible_str = json.dumps({"type": "record", "name": "Test", "fields": []})
        result = await write_new(
            Subject("s"),
            make_validated_schema(incompatible_str),
            None,
            explicit_schema_id=SchemaId(2),
            explicit_version=Version(2),
        )
        assert result == 2

    async def test_normal_mode_auto_assigns_id_and_version(self) -> None:
        from avro.compatibility import SchemaCompatibilityResult, SchemaCompatibilityType

        registry, db, sent, write_new = _make_registry_for_write()
        registry.check_schema_compatibility = Mock(
            return_value=SchemaCompatibilityResult(SchemaCompatibilityType.compatible)
        )

        result = await write_new(Subject("new-s"), make_validated_schema(), None)
        assert result == sent[0]["schema_id"]
        assert sent[0]["version"] == Version(1)

    async def test_import_mode_without_explicit_id_auto_assigns(self) -> None:
        _, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s"))
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)

        result = await write_new(Subject("s"), make_validated_schema(), None)
        assert result == sent[0]["schema_id"]
        assert sent[0]["version"] == Version(1)

    async def test_import_mode_reimport_same_id_identical_content_is_idempotent(self) -> None:
        """Re-importing an id already bound to identical content overwrites without error."""
        _, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s"))
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)
        db.insert_schema_version(
            subject=Subject("s"),
            schema_id=SchemaId(5),
            version=Version(1),
            deleted=False,
            schema=make_validated_schema(AVRO_SCHEMA_STR),
            references=None,
        )

        result = await write_new(
            Subject("s"),
            make_validated_schema(AVRO_SCHEMA_STR),
            None,
            explicit_schema_id=SchemaId(5),
            explicit_version=Version(1),
        )
        assert result == 5

    async def test_import_mode_same_id_different_content_raises_conflict(self) -> None:
        """Re-binding an existing id to different content must be rejected."""
        _, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s"))
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)
        db.insert_schema_version(
            subject=Subject("s"),
            schema_id=SchemaId(5),
            version=Version(1),
            deleted=False,
            schema=make_validated_schema(AVRO_SCHEMA_STR),
            references=None,
        )

        different_str = json.dumps({"type": "record", "name": "Other", "fields": []})
        with pytest.raises(SchemaIdConflict):
            await write_new(
                Subject("s"),
                make_validated_schema(different_str),
                None,
                explicit_schema_id=SchemaId(5),
                explicit_version=Version(2),
            )

    async def test_import_mode_conflict_checked_globally_across_subjects(self) -> None:
        """An id bound under one subject cannot be rebound to different content under another."""
        _, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s1"))
        db.insert_subject(subject=Subject("s2"))
        db.set_subject_mode(subject=Subject("s2"), mode=Mode.import_mode)
        db.insert_schema_version(
            subject=Subject("s1"),
            schema_id=SchemaId(7),
            version=Version(1),
            deleted=False,
            schema=make_validated_schema(AVRO_SCHEMA_STR),
            references=None,
        )

        different_str = json.dumps({"type": "record", "name": "Other", "fields": []})
        with pytest.raises(SchemaIdConflict):
            await write_new(
                Subject("s2"),
                make_validated_schema(different_str),
                None,
                explicit_schema_id=SchemaId(7),
                explicit_version=Version(1),
            )

    async def test_import_mode_with_multiple_versions(self) -> None:
        """Explicit version numbers can be non-sequential in IMPORT mode."""
        _, db, sent, write_new = _make_registry_for_write()
        db.insert_subject(subject=Subject("s"))
        db.set_subject_mode(subject=Subject("s"), mode=Mode.import_mode)

        schema1 = make_validated_schema(AVRO_SCHEMA_STR)
        await write_new(Subject("s"), schema1, None, explicit_schema_id=SchemaId(10), explicit_version=Version(10))

        schema2_str = json.dumps(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "string"}, {"name": "g", "type": ["null", "string"], "default": None}],
            }
        )
        await write_new(
            Subject("s"),
            make_validated_schema(schema2_str),
            None,
            explicit_schema_id=SchemaId(20),
            explicit_version=Version(20),
        )

        assert sent[0]["version"] == Version(10)
        assert sent[1]["version"] == Version(20)


class TestSchemaRequestImportFields:
    def test_id_and_version_default_to_none(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}'})
        assert req.schema_id is None
        assert req.schema_version is None

    def test_id_field_parsed_via_alias(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "id": 42})
        assert req.schema_id == 42

    def test_version_field_parsed_via_alias(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "version": 7})
        assert req.schema_version == 7

    def test_id_and_version_together(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "id": 100, "version": 3})
        assert req.schema_id == 100
        assert req.schema_version == 3

    def test_extra_fields_still_ignored(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "id": 1, "version": 1, "unknown": "x"})
        assert req.schema_id == 1


class TestModeUpdateRequest:
    def test_valid_mode_string(self) -> None:
        req = ModeUpdateRequest.model_validate({"mode": "IMPORT"})
        assert req.mode == "IMPORT"

    def test_all_mode_values_accepted(self) -> None:
        for mode in Mode:
            req = ModeUpdateRequest.model_validate({"mode": mode.value})
            assert req.mode == mode.value

    def test_missing_mode_raises(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ModeUpdateRequest.model_validate({})


def _make_controller(db: InMemoryDatabase | None = None):
    from karapace.api.controller import KarapaceSchemaRegistryController
    from karapace.core.schema_registry import KarapaceSchemaRegistry
    from karapace.core.config import Config

    if db is None:
        db = InMemoryDatabase()

    registry = Mock(spec=KarapaceSchemaRegistry)
    registry.database = db
    registry.get_global_mode = Mock(return_value=Mode.readwrite)
    registry.get_subject_mode = Mock(return_value=Mode.readwrite)
    registry.set_mode_local = AsyncMock(return_value=Mode.import_mode)
    registry.delete_mode_local = AsyncMock(return_value=Mode.readwrite)

    controller = KarapaceSchemaRegistryController(
        config=Mock(spec=Config),
        schema_registry=registry,
        stats=Mock(),
    )
    return controller, registry, db


class TestControllerGetGlobalMode:
    async def test_returns_readwrite_by_default(self) -> None:
        controller, registry, _ = _make_controller()
        registry.get_global_mode.return_value = Mode.readwrite
        resp = await controller.get_global_mode()
        assert resp.mode == "READWRITE"

    async def test_reflects_import_mode(self) -> None:
        controller, registry, _ = _make_controller()
        registry.get_global_mode.return_value = Mode.import_mode
        resp = await controller.get_global_mode()
        assert resp.mode == "IMPORT"


class TestControllerGetSubjectMode:
    async def test_returns_subject_mode(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("s"))
        controller, registry, _ = _make_controller(db)
        registry.get_subject_mode.return_value = Mode.import_mode
        resp = await controller.get_subject_mode(subject="s")
        assert resp.mode == "IMPORT"

    async def test_404_for_unknown_subject(self) -> None:
        from fastapi import HTTPException

        controller, _, _ = _make_controller()
        with pytest.raises(HTTPException) as exc_info:
            await controller.get_subject_mode(subject="nonexistent")
        assert exc_info.value.status_code == 404

    async def test_default_to_global_returns_global_for_missing_subject(self) -> None:
        controller, registry, _ = _make_controller()
        registry.get_global_mode.return_value = Mode.import_mode
        resp = await controller.get_subject_mode(subject="nonexistent", default_to_global=True)
        assert resp.mode == "IMPORT"


class TestControllerSetGlobalMode:
    async def test_returns_new_mode(self) -> None:
        controller, registry, _ = _make_controller()
        registry.set_mode_local = AsyncMock(return_value=Mode.import_mode)
        resp = await controller.set_global_mode(mode_request=ModeUpdateRequest(mode="IMPORT"))
        assert resp.mode == "IMPORT"

    async def test_invalid_mode_raises_422(self) -> None:
        from fastapi import HTTPException

        controller, registry, _ = _make_controller()
        registry.set_mode_local = AsyncMock(side_effect=InvalidMode("bad mode"))
        with pytest.raises(HTTPException) as exc_info:
            await controller.set_global_mode(mode_request=ModeUpdateRequest(mode="BOGUS"))
        assert exc_info.value.status_code == 422
        assert exc_info.value.detail["error_code"] == 42204


class TestControllerSetSubjectMode:
    async def test_returns_new_mode(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("s"))
        controller, registry, _ = _make_controller(db)
        registry.set_mode_local = AsyncMock(return_value=Mode.import_mode)
        resp = await controller.set_subject_mode(subject="s", mode_request=ModeUpdateRequest(mode="IMPORT"))
        assert resp.mode == "IMPORT"

    async def test_404_for_unknown_subject(self) -> None:
        from fastapi import HTTPException

        controller, _, _ = _make_controller()
        with pytest.raises(HTTPException) as exc_info:
            await controller.set_subject_mode(subject="ghost", mode_request=ModeUpdateRequest(mode="IMPORT"))
        assert exc_info.value.status_code == 404

    async def test_operation_not_permitted_raises_422(self) -> None:
        from fastapi import HTTPException

        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("s"))
        controller, registry, _ = _make_controller(db)
        registry.set_mode_local = AsyncMock(side_effect=OperationNotPermittedInMode("has schemas"))
        with pytest.raises(HTTPException) as exc_info:
            await controller.set_subject_mode(subject="s", mode_request=ModeUpdateRequest(mode="IMPORT"))
        assert exc_info.value.status_code == 422
        assert exc_info.value.detail["error_code"] == 42205

    async def test_force_flag_forwarded_to_registry(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("s"))
        controller, registry, _ = _make_controller(db)
        registry.set_mode_local = AsyncMock(return_value=Mode.import_mode)
        await controller.set_subject_mode(subject="s", mode_request=ModeUpdateRequest(mode="IMPORT"), force=True)
        registry.set_mode_local.assert_awaited_once_with(mode="IMPORT", subject=Subject("s"), force=True)


class TestControllerDeleteSubjectMode:
    async def test_returns_global_mode(self) -> None:
        db = InMemoryDatabase()
        db.insert_subject(subject=Subject("s"))
        controller, registry, _ = _make_controller(db)
        registry.delete_mode_local = AsyncMock(return_value=Mode.readwrite)
        resp = await controller.delete_subject_mode(subject="s")
        assert resp.mode == "READWRITE"

    async def test_404_for_unknown_subject(self) -> None:
        from fastapi import HTTPException

        controller, _, _ = _make_controller()
        with pytest.raises(HTTPException) as exc_info:
            await controller.delete_subject_mode(subject="ghost")
        assert exc_info.value.status_code == 404


class TestModeRouterEndpoints:
    def test_put_and_delete_routes_exist(self) -> None:
        from karapace.api.routers.mode import mode_router

        route_map: dict[str, set[str]] = {}
        for route in mode_router.routes:
            route_map.setdefault(route.path, set()).update(route.methods or [])

        assert "GET" in route_map.get("/mode", set()), "GET /mode missing"
        assert "PUT" in route_map.get("/mode", set()), "PUT /mode missing"
        assert "GET" in route_map.get("/mode/{subject}", set()), "GET /mode/{subject} missing"
        assert "PUT" in route_map.get("/mode/{subject}", set()), "PUT /mode/{subject} missing"
        assert "DELETE" in route_map.get("/mode/{subject}", set()), "DELETE /mode/{subject} missing"
