"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

Tests for bin/sr_migrate.py, against an in-memory registry that follows Karapace's IMPORT mode rules.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pytest
import urllib.parse

from pathlib import Path
from types import ModuleType
from typing import Any

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "sr_migrate.py"


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("sr_migrate", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sr_migrate = _load_script()

RESERVE = "_migration_id_reservation"
ORDER_V1 = json.dumps({"type": "record", "name": "Order", "fields": [{"name": "id", "type": "string"}]})
ORDER_V2 = json.dumps(
    {
        "type": "record",
        "name": "Order",
        "fields": [{"name": "id", "type": "string"}, {"name": "note", "type": "string", "default": ""}],
    }
)
CUSTOMER = json.dumps({"type": "record", "name": "Customer", "fields": [{"name": "id", "type": "string"}]})
INVOICE = json.dumps({"type": "record", "name": "Invoice", "fields": [{"name": "customer", "type": "Customer"}]})
CUSTOMER_REF = [{"name": "Customer", "subject": "customer", "version": 1}]


class FakeRegistry:
    """The part of the Schema Registry API the script uses, with Karapace's IMPORT mode rules."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.versions: dict[str, dict[int, dict[str, Any]]] = {}
        self.ids: dict[int, dict[str, Any]] = {}
        self.max_id = 0
        self.global_mode = "READWRITE"
        self.subject_modes: dict[str, str] = {}
        self.global_config = "BACKWARD"
        self.subject_config: dict[str, str] = {}
        self.calls: list[tuple[str, str, Any]] = []

    def describe_auth(self) -> str:
        return "none"

    def add(
        self,
        subject: str,
        schema: str,
        *,
        schema_id: int | None = None,
        references: list[dict[str, Any]] | None = None,
        deleted: bool = False,
    ) -> int:
        """Register as an ordinary write would, bypassing the API."""
        if schema_id is None:
            schema_id = next((i for i, s in self.ids.items() if s["schema"] == schema), self.max_id + 1)
        version = max(self.versions.get(subject, {}), default=0) + 1
        self._store(subject, version, schema_id, schema, "AVRO", references or [], deleted)
        return schema_id

    def _store(
        self, subject: str, version: int, schema_id: int, schema: str, schema_type: str, refs: list, deleted: bool
    ) -> None:
        self.ids[schema_id] = {"schema": schema, "schemaType": schema_type, "references": refs}
        self.versions.setdefault(subject, {})[version] = {
            "id": schema_id,
            "schema": schema,
            "schemaType": schema_type,
            "references": refs,
            "deleted": deleted,
        }
        self.max_id = max(self.max_id, schema_id)

    def live(self, subject: str) -> list[int]:
        return sorted(v for v, s in self.versions.get(subject, {}).items() if not s["deleted"])

    def mode_of(self, subject: str) -> str:
        return self.subject_modes.get(subject, self.global_mode)

    def call(self, method: str, path: str, body: Any = None, allow: tuple[int, ...] = ()) -> Any:
        self.calls.append((method, path, body))
        status, payload = self._route(method, path, body)
        if status == 200:
            return payload
        if status in allow:
            return None
        raise sr_migrate.RegistryError(method, path, status, json.dumps(payload))

    def _route(self, method: str, path: str, body: Any) -> tuple[int, Any]:
        parts = urllib.parse.urlsplit(path)
        query = urllib.parse.parse_qs(parts.query)

        def flag(name: str) -> bool:
            return query.get(name, ["false"])[0] == "true"

        segments = [urllib.parse.unquote(s) for s in parts.path.strip("/").split("/")]
        head = segments[0]

        if head == "subjects" and len(segments) == 1:
            subjects = self.versions if flag("deleted") else [s for s in self.versions if self.live(s)]
            return 200, sorted(subjects)
        if head == "subjects" and len(segments) == 3 and method == "GET":
            subject = segments[1]
            versions = sorted(self.versions.get(subject, {})) if flag("deleted") else self.live(subject)
            return (200, versions) if versions else (404, {"error_code": 40401, "message": "Subject not found."})
        if head == "subjects" and len(segments) == 3 and method == "POST":
            return self._register(segments[1], body)
        if head == "subjects" and len(segments) == 4:
            subject, version = segments[1], int(segments[3])
            entry = self.versions.get(subject, {}).get(version)
            if method == "DELETE":
                if entry is None or entry["deleted"]:
                    return 404, {"error_code": 40406, "message": "Version not found or already soft deleted."}
                entry["deleted"] = True
                return 200, version
            if entry is None or (entry["deleted"] and not flag("deleted")):
                return 404, {"error_code": 40402, "message": f"Version {version} not found."}
            response = {"subject": subject, "version": version, "id": entry["id"], "schema": entry["schema"]}
            if entry["references"]:
                response["references"] = entry["references"]
            return 200, response
        if head == "schemas":
            schema_id = int(segments[2])
            if schema_id not in self.ids:
                return 404, {"error_code": 40403, "message": "Schema not found"}
            response = {"schema": self.ids[schema_id]["schema"]}
            if flag("fetchMaxId"):
                response["maxId"] = self.max_id
            return 200, response
        if head == "mode":
            subject = segments[1] if len(segments) > 1 else None
            return self._mode(method, subject, body, flag("force"), flag("defaultToGlobal"))
        if head == "config":
            subject = segments[1] if len(segments) > 1 else None
            if method == "PUT":
                if subject is None:
                    self.global_config = body["compatibility"]
                else:
                    self.subject_config[subject] = body["compatibility"]
                return 200, {"compatibility": body["compatibility"]}
            level = self.global_config if subject is None else self.subject_config.get(subject)
            if level is None:
                return 404, {"error_code": 40408, "message": "Subject does not have subject-level compatibility."}
            return 200, {"compatibilityLevel": level}
        raise AssertionError(f"unexpected request {method} {path}")

    def _mode(self, method: str, subject: str | None, body: Any, force: bool, default_to_global: bool) -> tuple[int, Any]:
        if method == "GET":
            if subject is None:
                return 200, {"mode": self.global_mode}
            if subject in self.subject_modes or subject in self.versions or default_to_global:
                return 200, {"mode": self.mode_of(subject)}
            return 404, {"error_code": 40401, "message": f"Subject '{subject}' not found."}
        if method == "DELETE":
            if subject not in self.subject_modes:
                return 404, {"error_code": 40401, "message": f"Subject '{subject}' not found."}
            del self.subject_modes[subject]
            return 200, {"mode": self.global_mode}
        busy = [s for s in self.versions if self.live(s)] if subject is None else self.live(subject)
        if body["mode"] == "IMPORT" and busy and not force:
            return 409, {"error_code": 40901, "message": "Cannot import since found existing subjects"}
        if subject is None:
            self.global_mode = body["mode"]
        else:
            self.subject_modes[subject] = body["mode"]
        return 200, {"mode": body["mode"]}

    def _register(self, subject: str, body: dict[str, Any]) -> tuple[int, Any]:
        mode = self.mode_of(subject)
        if "id" not in body:
            raise AssertionError("the script only registers with an explicit id")
        if mode != "IMPORT":
            return 422, {"error_code": 42205, "message": f"Subject '{subject}' is not in IMPORT mode."}
        schema_id, version = body["id"], body["version"]
        if schema_id in self.ids and self.ids[schema_id]["schema"] != body["schema"]:
            return 409, {"error_code": 40901, "message": f"Overwrite new schema with id {schema_id} is not permitted."}
        existing = self.versions.get(subject, {}).get(version)
        if existing and existing["id"] != schema_id:
            return 409, {"error_code": 40901, "message": f"Subject '{subject}' version {version} is already registered."}
        for ref in body.get("references") or []:
            if ref["version"] not in self.versions.get(ref["subject"], {}):
                return 422, {"error_code": 42201, "message": "Invalid schema: reference not found"}
        self._store(subject, version, schema_id, body["schema"], body["schemaType"], body.get("references") or [], False)
        return 200, {"id": schema_id}


def make_source() -> FakeRegistry:
    """Ids 1..5 with a reference and a soft delete; id 6 was handed out and then hard deleted."""
    src = FakeRegistry("http://source")
    src.add("orders", ORDER_V1)
    src.add("payments", '"string"')
    src.add("customer", CUSTOMER)
    src.add("invoice", INVOICE, references=CUSTOMER_REF)
    src.add("orders", ORDER_V2)
    src.versions["payments"][1]["deleted"] = True
    src.max_id = 6
    src.subject_config["orders"] = "FULL"
    src.global_config = "BACKWARD_TRANSITIVE"
    return src


def migrate(
    tmp_path: Path,
    src: FakeRegistry | None,
    dst: FakeRegistry,
    *,
    scope: str = "global",
    reserve: str | None = RESERVE,
    yes: bool = True,
    force: bool = False,
    keep_import_mode: bool = False,
    file: Path | None = None,
) -> int:
    args = argparse.Namespace(
        target=dst.url,
        source=src.url if src else None,
        file=str(file or tmp_path / "export.json"),
        scope=scope,
        force=force,
        reserve_subject=reserve,
        keep_import_mode=keep_import_mode,
        yes=yes,
    )
    migration = sr_migrate.Migration(args)
    migration.dst = dst
    migration.src = src
    return migration.run()


def answer(monkeypatch: pytest.MonkeyPatch, *answers: str) -> list[str]:
    """Answer the prompts in order; returns the prompts asked, since input() does not print them."""
    replies, prompts = iter(answers), []

    def fake_input(prompt: str) -> str:
        prompts.append(prompt)
        return next(replies)

    monkeypatch.setattr("builtins.input", fake_input)
    return prompts


def ids_of(registry: FakeRegistry) -> dict[tuple[str, int], tuple[int, bool]]:
    return {
        (subject, version): (entry["id"], entry["deleted"])
        for subject, versions in registry.versions.items()
        for version, entry in versions.items()
    }


def puts_to_mode(registry: FakeRegistry, mode: str) -> list[str]:
    return [
        path
        for method, path, body in registry.calls
        if method == "PUT" and path.startswith("/mode") and body == {"mode": mode}
    ]


class TestExport:
    def test_api_export_includes_soft_deleted_versions_and_source_max_id(self) -> None:
        exported_from_api = sr_migrate.export_api(make_source())

        assert {(s["subject"], s["version"]): (s["id"], s["deleted"]) for s in exported_from_api["schemas"]} == {
            ("customer", 1): (3, False),
            ("invoice", 1): (4, False),
            ("orders", 1): (1, False),
            ("orders", 2): (5, False),
            ("payments", 1): (2, True),
        }
        assert exported_from_api["source_max_id"] == 6
        assert exported_from_api["subject_config"] == {"orders": "FULL"}
        assert exported_from_api["global_config"] == "BACKWARD_TRANSITIVE"
        assert next(s for s in exported_from_api["schemas"] if s["subject"] == "invoice")["references"] == CUSTOMER_REF

    def test_topic_export_replays_records_like_the_registry(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        def record(key: dict[str, Any], value: Any = "") -> str:
            return json.dumps(key) + "\t" + (value if isinstance(value, str) else json.dumps(value))

        def schema(subject: str, version: int, schema_id: int, deleted: bool = False) -> str:
            key = {"keytype": "SCHEMA", "subject": subject, "version": version, "magic": 1}
            value = {"subject": subject, "version": version, "id": schema_id, "schema": '"string"', "deleted": deleted}
            return record(key, value)

        def config(subject: str, value: Any) -> str:
            return record({"keytype": "CONFIG", "subject": subject, "magic": 0}, value)

        lines = [
            record({"keytype": "NOOP", "magic": 0}),
            schema("a", 1, 1),
            schema("a", 2, 2),
            schema("b", 1, 3),
            schema("b", 1, 3, deleted=True),
            schema("c", 1, 4),
            record({"keytype": "DELETE_SUBJECT", "subject": "c", "magic": 0}, {"subject": "c", "version": 1}),
            schema("d", 1, 5),
            record({"keytype": "SCHEMA", "subject": "d", "version": 1, "magic": 1}, "NULL"),
            config("a", {"compatibilityLevel": "FULL"}),
            config("b", {"compatibilityLevel": "NONE"}),
            config("b", "null"),
            record({"keytype": "MODE", "subject": "a", "magic": 0}, {"mode": "IMPORT"}),
            record({"keytype": "CONTEXT", "magic": 0}, "{}"),
        ]
        schemas_topic_file = tmp_path / "schemas.log"
        schemas_topic_file.write_text("\n".join(lines) + "\n")

        exported_from_topic = sr_migrate.export_topic_dump(str(schemas_topic_file), "BACKWARD")

        assert {(s["subject"], s["version"]): (s["id"], s["deleted"]) for s in exported_from_topic["schemas"]} == {
            ("a", 1): (1, False),
            ("a", 2): (2, False),
            ("b", 1): (3, True),
            ("c", 1): (4, True),
        }
        assert exported_from_topic["hard_deleted_ids"] == [5]
        assert exported_from_topic["source_max_id"] == 5
        assert exported_from_topic["subject_config"] == {"a": "FULL"}
        assert exported_from_topic["global_config"] == "BACKWARD"
        assert "skipping unknown keytype 'CONTEXT'" in capsys.readouterr().err


class TestImportOrder:
    def test_referenced_versions_come_first_even_with_a_higher_id(self) -> None:
        schemas = [
            {"subject": "user", "version": 1, "id": 2, "references": [{"name": "a", "subject": "address", "version": 1}]},
            {"subject": "address", "version": 1, "id": 7, "references": []},
            {"subject": "other", "version": 1, "id": 1, "references": []},
        ]
        assert [s["subject"] for s in sr_migrate.import_order(schemas)] == ["other", "address", "user"]

    def test_reference_missing_from_the_export_fails(self) -> None:
        missing = [{"name": "a", "subject": "gone", "version": 3}]
        schemas = [{"subject": "user", "version": 1, "id": 1, "references": missing}]
        with pytest.raises(sr_migrate.StepFailed, match="gone v3"):
            sr_migrate.import_order(schemas)


class TestMigration:
    def test_global_import_into_empty_target(self, tmp_path: Path) -> None:
        src, dst = make_source(), FakeRegistry("http://target")

        assert migrate(tmp_path, src, dst) == 0

        assert {key: value for key, value in ids_of(dst).items() if key[0] != RESERVE} == ids_of(src)
        assert ids_of(dst)[(RESERVE, 1)] == (6, True)
        assert dst.ids[6]["schema"] == sr_migrate.PLACEHOLDER
        assert dst.max_id == 6
        assert dst.subject_config == {"orders": "FULL"}
        assert dst.global_config == "BACKWARD_TRANSITIVE"
        assert dst.global_mode == "READWRITE"

    def test_no_reservation_without_a_gap(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        src.max_id = 5

        assert migrate(tmp_path, src, dst) == 0

        assert RESERVE not in dst.versions
        assert "nothing to reserve" in capsys.readouterr().out

    def test_gap_without_reserve_subject_warns(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        src, dst = make_source(), FakeRegistry("http://target")

        assert migrate(tmp_path, src, dst, reserve=None) == 0

        assert dst.max_id == 5
        assert "may hand out ids 6..6 again" in capsys.readouterr().out

    def test_yes_never_assumes_force(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        dst.add("existing", '"boolean"', schema_id=100)

        assert migrate(tmp_path, src, dst) == 1

        out = capsys.readouterr().out
        assert "n (not assumed by --yes)" in out
        assert "Re-run with --force" in out
        assert dst.global_mode == "READWRITE"
        assert not [path for _, path, _ in dst.calls if "force=true" in path]

    def test_accepting_force_replays_a_completed_migration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        assert migrate(tmp_path, src, dst) == 0
        capsys.readouterr()
        before = ids_of(dst)

        prompts = answer(monkeypatch, *["y"] * 20)
        assert migrate(tmp_path, src, dst, yes=False) == 0

        out = capsys.readouterr().out
        assert any("Retry with force=true" in prompt for prompt in prompts)
        assert "refused: PUT /mode -> 409 40901" in out
        assert "force on" in out
        assert "6 same content (replayed unchanged), 0 different content" in out
        assert "already has every compatibility level" in out
        assert ids_of(dst) == before
        assert dst.global_mode == "READWRITE"

    def test_resumes_a_run_left_in_import_mode(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        assert migrate(tmp_path, src, dst, keep_import_mode=True) == 0
        assert dst.global_mode == "IMPORT"
        capsys.readouterr()
        dst.calls.clear()

        assert migrate(tmp_path, src, dst) == 0

        out = capsys.readouterr().out
        assert "already in IMPORT" in out
        assert "0 entered, 1 already in IMPORT" in out
        assert not puts_to_mode(dst, "IMPORT")
        assert dst.global_mode == "READWRITE"

    def test_subject_scope_leaves_the_rest_of_the_target_alone(self, tmp_path: Path) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        dst.add("existing", '"boolean"', schema_id=100)

        assert migrate(tmp_path, src, dst, scope="subject") == 0

        assert ids_of(dst)[("existing", 1)] == (100, False)
        assert ids_of(dst)[("orders", 2)] == (5, False)
        assert dst.global_config == "BACKWARD"
        assert dst.global_mode == "READWRITE"
        assert dst.subject_modes == {}

    def test_id_bound_to_other_content_is_reported_and_fails(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        dst.add("unrelated", '"boolean"', schema_id=1)

        assert migrate(tmp_path, src, dst, scope="subject", force=True) == 1

        out = capsys.readouterr().out
        assert "clash: ids bound to different content on the target: 1" in out
        assert "40901" in out
        assert "--force does not help" in out
        assert "The target is still in IMPORT" in out
        assert dst.ids[1]["schema"] == '"boolean"'

    def test_declining_writes_nothing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        answer(monkeypatch, "y", "y", "n")

        assert migrate(tmp_path, src, dst, yes=False) == 1

        assert not [call for call in dst.calls if call[0] != "GET"]

    def test_import_from_an_export_file(self, tmp_path: Path) -> None:
        src, dst = make_source(), FakeRegistry("http://target")
        export_file = tmp_path / "saved.json"
        sr_migrate.write_dump(str(export_file), sr_migrate.export_api(src))

        assert migrate(tmp_path, None, dst, file=export_file) == 0

        assert {key: value for key, value in ids_of(dst).items() if key[0] != RESERVE} == ids_of(src)


class TestHints:
    @pytest.mark.parametrize(
        "status, body, expected",
        [
            (None, "cannot reach", "could not be reached"),
            (401, {"message": "unauthorized"}, "SRC_AUTH / DST_AUTH"),
            (409, {"error_code": 40901, "message": "Cannot import since found existing subjects"}, "Re-run with --force"),
            (409, {"error_code": 40901, "message": "Overwrite new schema with id 1 is not permitted."}, "does not help"),
            (422, {"error_code": 42205, "message": "Mode changes are not allowed"}, "mode_mutability"),
            (422, {"error_code": 42205, "message": "Subject 's' is not in IMPORT mode."}, "not in IMPORT mode"),
            (422, {"error_code": 42207, "message": "Schema already registered with id 1"}, "allow_duplicate_schema_ids"),
        ],
    )
    def test_hint_for_each_error(self, status: int | None, body: Any, expected: str) -> None:
        error = sr_migrate.RegistryError("PUT", "/mode", status, body if isinstance(body, str) else json.dumps(body))
        migration = sr_migrate.Migration(
            argparse.Namespace(
                target="http://t",
                source=None,
                file="x",
                scope="global",
                force=False,
                reserve_subject=None,
                keep_import_mode=False,
                yes=True,
            )
        )
        hints = migration.hints(error)
        assert any(expected in hint for hint in hints), hints
        assert "re-running the same command resumes" in hints[-1]


def test_same_schema_compares_json_by_value_and_text_by_content() -> None:
    assert sr_migrate.same_schema('{"type": "string"}', '{"type":"string"}')
    assert not sr_migrate.same_schema('"string"', '"int"')
    assert sr_migrate.same_schema('syntax = "proto3";\n', 'syntax = "proto3";')
