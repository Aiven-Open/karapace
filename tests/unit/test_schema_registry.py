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

from unittest.mock import MagicMock, patch

import pytest

from karapace.core.compatibility import CompatibilityModes
from karapace.core.container import KarapaceContainer
from karapace.core.errors import (
    SchemasNotFoundException,
    SchemaVersionSoftDeletedException,
    SubjectNotFoundException,
    SubjectSoftDeletedException,
    VersionNotFoundException,
)
from karapace.core.schema_models import SchemaVersion, TypedSchema
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.schema_type import SchemaType
from karapace.core.typing import PrimaryInfo, SchemaId, Subject, Version


@pytest.fixture(name="registry")
def fixture_registry(karapace_container: KarapaceContainer) -> KarapaceSchemaRegistry:
    """Build a KarapaceSchemaRegistry with all Kafka-bound collaborators mocked."""
    stats = MagicMock()
    with (
        patch("karapace.core.schema_registry.KarapaceProducer") as mock_producer,
        patch("karapace.core.schema_registry.MasterCoordinator") as mock_mc,
        patch("karapace.core.schema_registry.KafkaSchemaReader") as mock_reader,
    ):
        # ``get_referenced_by`` is called by delete paths; default to "nobody references this".
        mock_reader.return_value.get_referenced_by.return_value = []
        mock_reader.return_value.ready.return_value = True

        registry = KarapaceSchemaRegistry(config=karapace_container.config(), stats=stats)

    # Expose the inner mocks for assertions in tests.
    registry.producer = mock_producer.return_value
    registry.mc = mock_mc.return_value
    registry.schema_reader = mock_reader.return_value
    return registry


def _avro_schema(name: str = "Obj") -> TypedSchema:
    """Build a tiny, valid Avro TypedSchema for tests that need to insert one."""
    from karapace.core.schema_models import ValidatedTypedSchema

    return ValidatedTypedSchema.parse(
        schema_type=SchemaType.AVRO,
        schema_str=('{"type": "record", "name": "' + name + '", "fields": [{"name": "age", "type": "int"}]}'),
    )


def _insert_subject_with_schema(
    registry: KarapaceSchemaRegistry,
    *,
    subject: str,
    schema_id: int = 1,
    version: int = 1,
    deleted: bool = False,
) -> None:
    """Populate the in-memory DB with a single (subject, version, schema)."""
    schema = _avro_schema()
    subject_obj = Subject(subject)
    registry.database.insert_subject(subject=subject_obj)
    registry.database.insert_schema_version(
        subject=subject_obj,
        schema_id=SchemaId(schema_id),
        version=Version(version),
        deleted=deleted,
        schema=schema,
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
        # `include_deleted=False` lookup should return nothing -> SchemasNotFound.
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
        # The default container config sets a valid global compatibility value.
        _insert_subject_with_schema(registry, subject="s")
        mode = registry.get_compatibility_mode(Subject("s"))
        # Whatever the global default is, it must be a member of CompatibilityModes.
        assert isinstance(mode, CompatibilityModes)

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
        registry.mc.get_master_info.return_value = PrimaryInfo(True, primary_url="http://other:8081")

        result = await registry.get_master()
        # When reader isn't ready we must report not-primary regardless of mc state.
        assert result.primary is False
        assert result.primary_url == "http://other:8081"

    async def test_returns_master_info_when_reader_is_ready(self, registry: KarapaceSchemaRegistry) -> None:
        registry.schema_reader.ready.return_value = True
        expected = PrimaryInfo(True, primary_url="http://self:8081")
        registry.mc.get_master_info.return_value = expected

        result = await registry.get_master()
        assert result == expected


class TestSubjectDelete:
    async def test_soft_delete_on_already_soft_deleted_subject_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", deleted=True)
        with pytest.raises(SubjectSoftDeletedException):
            await registry.subject_delete_local(Subject("s"), permanent=False)

    async def test_permanent_delete_on_live_subject_raises(self, registry: KarapaceSchemaRegistry) -> None:
        from karapace.core.errors import SubjectNotSoftDeletedException

        _insert_subject_with_schema(registry, subject="s", deleted=False)
        with pytest.raises(SubjectNotSoftDeletedException):
            await registry.subject_delete_local(Subject("s"), permanent=True)


class TestSubjectVersionDelete:
    async def test_unknown_version_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1)
        with pytest.raises(VersionNotFoundException):
            await registry.subject_version_delete_local(Subject("s"), Version(99), permanent=False)

    async def test_soft_delete_of_already_soft_deleted_version_raises(self, registry: KarapaceSchemaRegistry) -> None:
        _insert_subject_with_schema(registry, subject="s", version=1, deleted=True)
        with pytest.raises(SchemaVersionSoftDeletedException):
            await registry.subject_version_delete_local(Subject("s"), Version(1), permanent=False)
