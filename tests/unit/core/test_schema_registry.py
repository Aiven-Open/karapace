"""
Tests for the schema registry core logic (``karapace.core.schema_registry``).

These tests exercise the registry's behaviour against a real ``InMemoryDatabase``
while replacing the I/O-bound collaborators (``KarapaceProducer``,
``MasterCoordinator``, ``KafkaSchemaReader``) with mocks so no Kafka broker is
required to run them.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from karapace.core.compatibility import CompatibilityModes
from karapace.core.container import KarapaceContainer
from karapace.core.coordinator.master_coordinator import MasterCoordinator
from karapace.core.errors import (
    IncompatibleSchema,
    ReferenceExistsException,
    SchemasNotFoundException,
    SchemaVersionNotSoftDeletedException,
    SchemaVersionSoftDeletedException,
    SubjectNotFoundException,
    SubjectNotSoftDeletedException,
    SubjectSoftDeletedException,
    VersionNotFoundException,
)
from karapace.core.schema_models import SchemaVersion, TypedSchema, ValidatedTypedSchema
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.schema_type import SchemaType
from karapace.core.typing import PrimaryInfo, SchemaId, Subject, Version


@pytest.fixture(name="registry")
def fixture_registry(karapace_container: KarapaceContainer) -> KarapaceSchemaRegistry:
    """Build a KarapaceSchemaRegistry with all Kafka-bound collaborators mocked.

    The patches are scoped to the registry constructor: the resulting
    ``registry.producer`` / ``registry.mc`` / ``registry.schema_reader`` are
    already the mock instances injected during ``__init__``. We expose them
    via attributes on the fixture only as a convenience for tests.
    """
    stats = MagicMock()
    with (
        patch("karapace.core.schema_registry.KarapaceProducer"),
        # spec=MasterCoordinator guards against signature drift — e.g. if
        # get_master_info ever becomes async, an attempted sync return-value
        # assignment will surface here rather than as a coroutine-warning at
        # runtime.
        patch("karapace.core.schema_registry.MasterCoordinator", spec=MasterCoordinator),
        patch("karapace.core.schema_registry.KafkaSchemaReader") as mock_reader,
    ):
        mock_reader.return_value.get_referenced_by.return_value = []
        mock_reader.return_value.ready.return_value = True

        registry = KarapaceSchemaRegistry(config=karapace_container.config(), stats=stats)

    return registry


def _avro_schema(name: str = "Obj", extra_field: str | None = None) -> ValidatedTypedSchema:
    """Build a tiny, valid Avro TypedSchema for tests that need to insert one."""
    fields: list[dict] = [{"name": "age", "type": "int"}]
    if extra_field is not None:
        # ``extra_field`` is appended with a default so it stays BACKWARD-compatible
        # with the base schema when used as a *newer* version.
        fields.append({"name": extra_field, "type": "string", "default": ""})
    schema = {"type": "record", "name": name, "fields": fields}
    return ValidatedTypedSchema.parse(schema_type=SchemaType.AVRO, schema_str=json.dumps(schema))


def _insert_subject_with_schema(
    registry: KarapaceSchemaRegistry,
    *,
    subject: str,
    schema_id: int = 1,
    version: int = 1,
    deleted: bool = False,
    schema: TypedSchema | None = None,
) -> None:
    """Populate the in-memory DB with a single (subject, version, schema)."""
    typed_schema = schema or _avro_schema()
    subject_obj = Subject(subject)
    registry.database.insert_subject(subject=subject_obj)
    registry.database.insert_schema_version(
        subject=subject_obj,
        schema_id=SchemaId(schema_id),
        version=Version(version),
        deleted=deleted,
        schema=typed_schema,
        references=None,
    )


class TestSubjectsAndSchemasLookups:
    def test_subjects_list_returns_inserted_subjects(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="topic-a-value")
        _insert_subject_with_schema(registry, subject="topic-b-value")

        subjects = registry.subjects_list()
        assert sorted(subjects) == ["topic-a-value", "topic-b-value"]

    def test_subjects_list_excludes_soft_deleted_by_default(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="alive")
        _insert_subject_with_schema(registry, subject="gone", deleted=True)

        live = registry.subjects_list()
        all_subjects = registry.subjects_list(include_deleted=True)

        assert "alive" in live
        assert "gone" not in live
        assert "gone" in all_subjects

    def test_subject_get_raises_when_subject_missing(self, registry: KarapaceSchemaRegistry) -> None:
        with pytest.raises(SubjectNotFoundException):
            registry.subject_get(Subject("does-not-exist"))

    def test_subject_get_raises_when_subject_has_no_live_schemas(self, registry: KarapaceSchemaRegistry) -> None:
        # Insert a subject but only a soft-deleted version: the default
        # ``include_deleted=False`` lookup should return nothing -> SchemasNotFound.
        _insert_subject_with_schema(registry, subject="s", deleted=True)
        with pytest.raises(SchemasNotFoundException):
            registry.subject_get(Subject("s"))

    def test_subject_get_returns_schemas_when_present(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", schema_id=7, version=1)
        schemas = registry.subject_get(Subject("s"))
        assert Version(1) in schemas
        assert schemas[Version(1)].schema_id == SchemaId(7)

    def test_schemas_get_returns_none_for_unknown_id(self, registry: KarapaceSchemaRegistry) -> None:
        assert registry.schemas_get(SchemaId(9999)) is None


class TestCompatibilityMode:
    def test_falls_back_to_global_when_no_subject_override(self, registry: KarapaceSchemaRegistry) -> None:
        # The container default sets ``compatibility = "BACKWARD"`` — assert that
        # specific value rather than just "is a CompatibilityModes member", which
        # would pass for any enum value.
        _insert_subject_with_schema(registry, subject="s")
        mode = registry.get_compatibility_mode(Subject("s"))
        assert mode is CompatibilityModes.BACKWARD

    def test_uses_subject_override_when_set(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("s")
        _insert_subject_with_schema(registry, subject="s")
        registry.database.set_subject_compatibility(subject=subject, compatibility="NONE")

        mode = registry.get_compatibility_mode(subject)
        assert mode is CompatibilityModes.NONE

    def test_unknown_compatibility_value_raises(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("s")
        _insert_subject_with_schema(registry, subject="s")
        registry.database.set_subject_compatibility(subject=subject, compatibility="WILD-MODE")

        with pytest.raises(ValueError, match="Unknown compatibility mode"):
            registry.get_compatibility_mode(subject)


class TestGetLiveVersionsSorted:
    def test_filters_out_deleted_and_sorts(self) -> None:
        schema = _avro_schema()
        versions = {
            Version(2): SchemaVersion(
                subject=Subject("s"),
                version=Version(2),
                deleted=False,
                schema_id=SchemaId(2),
                schema=schema,
                references=None,
            ),
            Version(1): SchemaVersion(
                subject=Subject("s"),
                version=Version(1),
                deleted=False,
                schema_id=SchemaId(1),
                schema=schema,
                references=None,
            ),
            Version(3): SchemaVersion(
                subject=Subject("s"),
                version=Version(3),
                deleted=True,  # must be filtered out
                schema_id=SchemaId(3),
                schema=schema,
                references=None,
            ),
        }

        result = KarapaceSchemaRegistry.get_live_versions_sorted(versions)
        assert result == [Version(1), Version(2)]


class TestSendMessages:
    def test_send_config_message_sends_compatibility(self, registry: KarapaceSchemaRegistry) -> None:
        registry.send_config_message(CompatibilityModes.FULL, subject=None)

        registry.producer.send_message.assert_called_once()
        sent_kwargs = registry.producer.send_message.call_args.kwargs
        assert sent_kwargs["key"] == {"subject": None, "magic": 0, "keytype": "CONFIG"}
        assert sent_kwargs["value"] == {"compatibilityLevel": "FULL"}

    def test_send_delete_subject_message_includes_version(self, registry: KarapaceSchemaRegistry) -> None:
        registry.send_delete_subject_message(Subject("s"), Version(3))
        sent_kwargs = registry.producer.send_message.call_args.kwargs
        assert sent_kwargs["key"] == {"subject": "s", "magic": 0, "keytype": "DELETE_SUBJECT"}
        assert sent_kwargs["value"] == {"subject": "s", "version": 3}

    def test_send_schema_message_sends_none_value_for_hard_delete(self, registry: KarapaceSchemaRegistry) -> None:
        registry.send_schema_message(
            subject=Subject("s"),
            schema=None,
            schema_id=42,
            version=Version(1),
            deleted=True,
            references=None,
        )
        sent_kwargs = registry.producer.send_message.call_args.kwargs
        # A tombstone for a hard delete: value must be None.
        assert sent_kwargs["value"] is None


class TestGetMaster:
    async def test_returns_not_primary_when_reader_not_ready(self, registry: KarapaceSchemaRegistry) -> None:
        registry.schema_reader.ready.return_value = False
        registry.mc.get_master_info.return_value = PrimaryInfo(primary=True, primary_url="http://other:8081")

        result = await registry.get_master()
        # When reader isn't ready we must report not-primary regardless of mc state.
        assert result.primary is False
        assert result.primary_url == "http://other:8081"

    async def test_returns_master_info_when_reader_is_ready(self, registry: KarapaceSchemaRegistry) -> None:
        registry.schema_reader.ready.return_value = True
        expected = PrimaryInfo(primary=True, primary_url="http://self:8081")
        registry.mc.get_master_info.return_value = expected

        result = await registry.get_master()
        assert result == expected


class TestSubjectDelete:
    async def test_soft_delete_on_live_subject_emits_one_delete_subject_message(
        self, registry: KarapaceSchemaRegistry
    ) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1)
        _insert_subject_with_schema(registry, subject="s", schema_id=2, version=2)

        versions = await registry.subject_delete_local(Subject("s"), permanent=False)

        assert versions == [Version(1), Version(2)]
        registry.producer.send_message.assert_called_once()
        sent_kwargs = registry.producer.send_message.call_args.kwargs
        # Soft delete: a single DELETE_SUBJECT message pinned to the latest version.
        assert sent_kwargs["key"]["keytype"] == "DELETE_SUBJECT"
        assert sent_kwargs["value"] == {"subject": "s", "version": 2}

    async def test_permanent_delete_on_fully_soft_deleted_subject_emits_tombstone_per_version(
        self, registry: KarapaceSchemaRegistry
    ) -> None:
        _insert_subject_with_schema(registry, subject="s", schema_id=1, version=1, deleted=True)
        _insert_subject_with_schema(registry, subject="s", schema_id=2, version=2, deleted=True)

        await registry.subject_delete_local(Subject("s"), permanent=True)

        assert registry.producer.send_message.call_count == 2
        # Each call must be a SCHEMA tombstone (value=None).
        for call in registry.producer.send_message.call_args_list:
            assert call.kwargs["key"]["keytype"] == "SCHEMA"
            assert call.kwargs["value"] is None

    async def test_soft_delete_on_already_soft_deleted_subject_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", deleted=True)
        with pytest.raises(SubjectSoftDeletedException):
            await registry.subject_delete_local(Subject("s"), permanent=False)

    async def test_permanent_delete_on_live_subject_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", deleted=False)
        with pytest.raises(SubjectNotSoftDeletedException):
            await registry.subject_delete_local(Subject("s"), permanent=True)

    async def test_soft_delete_raises_when_latest_version_is_referenced(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1)
        registry.schema_reader.get_referenced_by.return_value = [("other-subject", 1)]

        with pytest.raises(ReferenceExistsException):
            await registry.subject_delete_local(Subject("s"), permanent=False)


class TestSubjectVersionDelete:
    async def test_unknown_version_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1)
        with pytest.raises(VersionNotFoundException):
            await registry.subject_version_delete_local(Subject("s"), Version(99), permanent=False)

    async def test_soft_delete_of_already_soft_deleted_version_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1, deleted=True)
        with pytest.raises(SchemaVersionSoftDeletedException):
            await registry.subject_version_delete_local(Subject("s"), Version(1), permanent=False)

    async def test_soft_delete_of_live_version_sends_schema_message(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", schema_id=1, version=1)

        resolved = await registry.subject_version_delete_local(Subject("s"), Version(1), permanent=False)

        assert resolved == Version(1)
        sent = registry.producer.send_message.call_args.kwargs
        assert sent["key"]["keytype"] == "SCHEMA"
        # Soft delete keeps the schema body; only the ``deleted`` flag flips.
        assert sent["value"] is not None
        assert sent["value"]["deleted"] is True

    async def test_permanent_delete_of_live_version_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1, deleted=False)
        with pytest.raises(SchemaVersionNotSoftDeletedException):
            await registry.subject_version_delete_local(Subject("s"), Version(1), permanent=True)

    async def test_delete_raises_when_version_is_referenced(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1)
        registry.schema_reader.get_referenced_by.return_value = [("other-subject", 1)]

        with pytest.raises(ReferenceExistsException):
            await registry.subject_version_delete_local(Subject("s"), Version(1), permanent=False)


class TestSubjectVersionGet:
    def test_returns_expected_fields_for_avro(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", schema_id=7, version=1)

        result = registry.subject_version_get(Subject("s"), Version(1))

        assert result["subject"] == "s"
        assert result["version"] == 1
        assert result["id"] == 7
        assert isinstance(result["schema"], str)
        # Avro is the default — no schemaType key in the response.
        assert "schemaType" not in result
        # No references inserted.
        assert "references" not in result

    def test_includes_schema_type_for_non_avro(self, registry: KarapaceSchemaRegistry) -> None:
        json_schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.JSONSCHEMA,
            schema_str='{"type": "object", "properties": {"age": {"type": "integer"}}}',
        )
        _insert_subject_with_schema(registry, subject="s", schema=json_schema, version=1)

        result = registry.subject_version_get(Subject("s"), Version(1))
        assert result["schemaType"] == SchemaType.JSONSCHEMA

    def test_includes_compatibility_when_subject_override_set(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("s")
        _insert_subject_with_schema(registry, subject="s", version=1)
        registry.database.set_subject_compatibility(subject=subject, compatibility="NONE")

        result = registry.subject_version_get(subject, Version(1))
        assert result["compatibility"] == "NONE"

    def test_unknown_version_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1)
        with pytest.raises(VersionNotFoundException):
            registry.subject_version_get(Subject("s"), Version(99))


class TestCheckSchemaCompatibility:
    def test_returns_compatible_when_no_existing_versions(self, registry: KarapaceSchemaRegistry) -> None:
        # Brand-new subject with no rows: nothing to compare against -> compatible.
        result = registry.check_schema_compatibility(_avro_schema(), Subject("s"))
        assert result.compatibility.name == "compatible"

    def test_latest_only_mode_ignores_older_incompatible_versions(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("s")
        # v1 had ``age: int``; v2 changed the type to ``string`` (BACKWARD-incompatible
        # with v1). In non-transitive BACKWARD mode the new schema is only compared
        # against the latest live version (v2), so v1's incompatibility is masked.
        v1_schema = _avro_schema()  # ``age: int``
        v2_schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.AVRO,
            schema_str=json.dumps({"type": "record", "name": "Obj", "fields": [{"name": "age", "type": "string"}]}),
        )
        _insert_subject_with_schema(registry, subject="s", schema_id=1, version=1, schema=v1_schema)
        _insert_subject_with_schema(registry, subject="s", schema_id=2, version=2, schema=v2_schema)
        registry.database.set_subject_compatibility(subject=subject, compatibility="BACKWARD")

        # New schema is identical to v2 -> compatible when only v2 is consulted.
        result = registry.check_schema_compatibility(v2_schema, subject)
        assert result.compatibility.name == "compatible"

    def test_transitive_mode_checks_against_all_live_versions(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("s")
        v1_schema = _avro_schema()  # ``age: int``
        # Type change int -> string is BACKWARD-incompatible with v1.
        v2_schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.AVRO,
            schema_str=json.dumps({"type": "record", "name": "Obj", "fields": [{"name": "age", "type": "string"}]}),
        )
        _insert_subject_with_schema(registry, subject="s", schema_id=1, version=1, schema=v1_schema)
        _insert_subject_with_schema(registry, subject="s", schema_id=2, version=2, schema=v2_schema)
        registry.database.set_subject_compatibility(subject=subject, compatibility="BACKWARD_TRANSITIVE")

        # New schema equals v2: compatible with v2 but NOT with v1; transitive
        # mode walks every live version and surfaces the v1 incompatibility.
        result = registry.check_schema_compatibility(v2_schema, subject)
        assert result.compatibility.name != "compatible"


class TestWriteNewSchemaLocal:
    """Cover the four distinct branches of ``write_new_schema_local``."""

    async def test_returns_existing_schema_id_on_cache_hit(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("s")
        schema = _avro_schema()
        # Seed: the exact same schema is already registered for this subject.
        _insert_subject_with_schema(registry, subject="s", schema_id=42, version=1, schema=schema)

        result_id = await registry.write_new_schema_local(subject, schema, new_schema_references=None)

        assert result_id == 42
        # Fast path: no producer write must occur.
        registry.producer.send_message.assert_not_called()

    async def test_creates_first_version_for_brand_new_subject(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("brand-new")
        schema = _avro_schema()

        schema_id = await registry.write_new_schema_local(subject, schema, new_schema_references=None)

        assert isinstance(schema_id, int)
        # Exactly one SCHEMA message pinned to version 1.
        registry.producer.send_message.assert_called_once()
        sent = registry.producer.send_message.call_args.kwargs
        assert sent["key"]["keytype"] == "SCHEMA"
        assert sent["key"]["version"] == 1
        assert sent["value"]["deleted"] is False

    async def test_resurrects_subject_when_all_versions_were_deleted(self, registry: KarapaceSchemaRegistry) -> None:
        subject = Subject("s")
        old_schema = _avro_schema()
        new_schema = _avro_schema(extra_field="email")
        # Existing soft-deleted version + a different incoming schema -> the
        # "all-deleted-versions resurrection" branch.
        _insert_subject_with_schema(registry, subject="s", schema_id=1, version=1, deleted=True, schema=old_schema)

        schema_id = await registry.write_new_schema_local(subject, new_schema, new_schema_references=None)

        assert isinstance(schema_id, int)
        registry.producer.send_message.assert_called_once()
        sent = registry.producer.send_message.call_args.kwargs
        assert sent["key"]["version"] == 2
        assert sent["value"]["deleted"] is False

    async def test_raises_incompatible_when_new_schema_breaks_backward_compatibility(
        self, registry: KarapaceSchemaRegistry
    ) -> None:
        subject = Subject("s")
        v1_schema = _avro_schema()  # ``age: int``
        # Adding a required field with NO default is BACKWARD-incompatible.
        v2_schema = ValidatedTypedSchema.parse(
            schema_type=SchemaType.AVRO,
            schema_str=json.dumps(
                {
                    "type": "record",
                    "name": "Obj",
                    "fields": [
                        {"name": "age", "type": "int"},
                        {"name": "must_have", "type": "string"},
                    ],
                }
            ),
        )
        _insert_subject_with_schema(registry, subject="s", schema_id=1, version=1, schema=v1_schema)
        registry.database.set_subject_compatibility(subject=subject, compatibility="BACKWARD")

        with pytest.raises(IncompatibleSchema):
            await registry.write_new_schema_local(subject, v2_schema, new_schema_references=None)

        # On the incompatible branch we must NOT have produced any message.
        registry.producer.send_message.assert_not_called()
