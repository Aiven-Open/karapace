"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Final
from unittest.mock import Mock

import pytest
from confluent_kafka.cimpl import KafkaError

from karapace.core.constants import DEFAULT_SCHEMA_TOPIC
from karapace.core.container import KarapaceContainer
from karapace.core.in_memory_database import InMemoryDatabase, KarapaceDatabase, Subject, SubjectData
from karapace.core.kafka.types import Timestamp
from karapace.core.key_format import KeyFormatter
from karapace.core.offset_watcher import OffsetWatcher
from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.schema_models import SchemaVersion, TypedSchema
from karapace.core.schema_reader import KafkaSchemaReader
from karapace.core.schema_references import Reference, Referents
from karapace.core.schema_type import SchemaType
from karapace.core.stats import StatsClient
from karapace.core.typing import SchemaId, Version

TEST_DATA_FOLDER: Final = Path("tests/unit/test_data/")


class TestFindSchemas:
    def test_returns_empty_list_when_no_schemas(self) -> None:
        database = InMemoryDatabase()
        subject = Subject("hello_world")
        database.insert_subject(subject=subject)
        expected = {subject: []}
        assert database.find_schemas(include_deleted=True, latest_only=True) == expected


class AlwaysFineKafkaMessage:
    def __init__(
        self,
        offset: int,
        timestamp: tuple[int, int],
        topic: str,
        key: str | bytes | None = None,
        value: str | bytes | None = None,
        partition: int = 0,
        headers: list[tuple[str, bytes]] | None = None,
        error: KafkaError | None = None,
    ) -> None:
        self._offset = offset
        self._timestamp = timestamp
        self._key = key
        self._value = value
        self._topic = topic
        self._partition = partition
        self._headers = headers
        self._error = error

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp

    def key(self) -> str | bytes | None:
        return self._key

    def value(self) -> str | bytes | None:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def error(self) -> KafkaError | None:
        return self._error


class WrappedInMemoryDatabase(KarapaceDatabase):
    def __init__(self) -> None:
        self._duplicates: dict[SchemaId, list[TypedSchema]] = {}
        self._schema_id_to_subject: dict[SchemaId, list[Subject]] = defaultdict(list)
        self._duplicates_timestamp: dict[SchemaId, list[int]] = {}
        self.db = InMemoryDatabase()
        self.timestamp = -1

    def get_schema_id(self, new_schema: TypedSchema) -> SchemaId:
        return self.db.get_schema_id(new_schema)

    def get_schema_id_if_exists(
        self,
        *,
        subject: Subject,
        schema: TypedSchema,
        include_deleted: bool,
    ) -> SchemaId | None:
        return self.db.get_schema_id_if_exists(subject=subject, schema=schema, include_deleted=include_deleted)

    def get_next_version(self, *, subject: Subject) -> Version:
        return self.db.get_next_version(subject=subject)

    def insert_schema_version(
        self,
        *,
        subject: Subject,
        schema_id: SchemaId,
        version: Version,
        deleted: bool,
        schema: TypedSchema,
        references: Sequence[Reference] | None,
    ) -> None:
        self._schema_id_to_subject[schema_id].append(subject)
        if schema_id in self.db.schemas:
            if schema_id not in self._duplicates:
                self._duplicates[schema_id] = [self.db.schemas[schema_id]]
            self._duplicates[schema_id].append(schema)

            if schema_id not in self._duplicates_timestamp:
                self._duplicates_timestamp[schema_id] = [self.timestamp]
            self._duplicates_timestamp[schema_id].append(self.timestamp)

        return self.db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=version, deleted=deleted, schema=schema, references=references
        )

    def insert_subject(self, *, subject: Subject) -> None:
        return self.db.insert_subject(subject=subject)

    def get_subject_compatibility(self, *, subject: Subject) -> str | None:
        return self.db.get_subject_compatibility(subject=subject)

    def delete_subject_compatibility(self, *, subject: Subject) -> None:
        return self.db.delete_subject_compatibility(subject=subject)

    def set_subject_compatibility(self, *, subject: Subject, compatibility: str) -> None:
        return self.db.set_subject_compatibility(subject=subject, compatibility=compatibility)

    def find_schema(self, *, schema_id: SchemaId) -> TypedSchema | None:
        return self.db.find_schema(schema_id=schema_id)

    def find_schemas(self, *, include_deleted: bool, latest_only: bool) -> dict[Subject, list[SchemaVersion]]:
        return self.db.find_schemas(include_deleted=include_deleted, latest_only=latest_only)

    def subjects_for_schema(self, schema_id: SchemaId) -> list[Subject]:
        return self.db.subjects_for_schema(schema_id=schema_id)

    def find_schema_versions_by_schema_id(self, *, schema_id: SchemaId, include_deleted: bool) -> list[SchemaVersion]:
        return self.db.find_schema_versions_by_schema_id(schema_id=schema_id, include_deleted=include_deleted)

    def find_subject(self, *, subject: Subject) -> Subject | None:
        return self.db.find_subject(subject=subject)

    def find_subjects(self, *, include_deleted: bool) -> list[Subject]:
        return self.db.find_subjects(include_deleted=include_deleted)

    def find_subject_schemas(self, *, subject: Subject, include_deleted: bool) -> dict[Version, SchemaVersion]:
        return self.db.find_subject_schemas(subject=subject, include_deleted=include_deleted)

    def delete_subject(self, *, subject: Subject, version: Version) -> None:
        return self.db.delete_subject(subject=subject, version=version)

    def delete_subject_hard(self, *, subject: Subject) -> None:
        return self.db.delete_subject_hard(subject=subject)

    def delete_subject_schema(self, *, subject: Subject, version: Version) -> None:
        return self.db.delete_subject_schema(subject=subject, version=version)

    def num_schemas(self) -> int:
        return self.db.num_schemas()

    def num_subjects(self) -> int:
        return self.db.num_subjects()

    def num_schema_versions(self) -> tuple[int, int]:
        return self.db.num_schema_versions()

    def get_referenced_by(self, subject: Subject, version: Version) -> Referents | None:
        return self.db.get_referenced_by(subject=subject, version=version)

    def duplicates(self) -> dict[SchemaId, list[tuple[Subject, TypedSchema]]]:
        duplicate_data = defaultdict(list)
        for schema_id, schemas in self._duplicates.items():
            for subject, schema in zip(self._schema_id_to_subject[schema_id], schemas):
                duplicate_data[schema_id].append((subject, schema))
        return duplicate_data

    def subject_to_subject_data(self) -> dict[Subject, SubjectData]:
        return self.db.subjects


def compute_schema_id_to_subjects(
    duplicates: dict[SchemaId, list[tuple[Subject, TypedSchema]]], subject_to_subject_data: dict[Subject, SubjectData]
) -> dict[SchemaId, list[tuple[Subject, Version]]]:
    tuples = [(schema_id, subject) for schema_id, dup in duplicates.items() for subject, _ in dup]
    schema_id_to_duplicated_subjects = defaultdict(list)
    for schema_id, subject_referring_to_duplicate_schema in tuples:
        corrupted_data = subject_to_subject_data[subject_referring_to_duplicate_schema]
        corrupted_version = -1
        for schema_version, schema_data in corrupted_data.schemas.items():
            assert schema_version == schema_data.version

            if schema_data.schema_id == schema_id:
                corrupted_version = schema_version

        schema_id_to_duplicated_subjects[schema_id].append((subject_referring_to_duplicate_schema, corrupted_version))
    return schema_id_to_duplicated_subjects


def test_can_ingest_schemas_from_log(karapace_container: KarapaceContainer) -> None:
    """
    Test for the consistency of a backup, this checks that each SchemaID its unique in the backup.
    The format of the log its the one obtained by running:

            `kafkacat -C -t _schemas -o beginning -e -f "%k\t%s\t%T\n"`

    on a node running kafka that hosts the `_schemas` topic.
    """
    stats_mock = Mock(spec=StatsClient)
    restore_location = TEST_DATA_FOLDER / "schemas.log"
    schema_log = restore_location.read_text(encoding="utf-8").strip()

    database = WrappedInMemoryDatabase()
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=KeyFormatter(),
        master_coordinator=None,
        database=database,
        stats=stats_mock,
    )

    kafka_messages: list[AlwaysFineKafkaMessage] = []
    for i, message in enumerate(schema_log.split("\n")[:-1]):
        res = message.split("\t")
        timestamp = res[-1]
        maybe_key_val = res[:-1]
        # the tuple follows the kafka message specific
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message.timestamp
        timestamp_tuple = (Timestamp.CREATE_TIME, int(timestamp))
        database.timestamp = timestamp
        if len(maybe_key_val) > 1:
            key, value = maybe_key_val
            kafka_message = AlwaysFineKafkaMessage(i, timestamp_tuple, DEFAULT_SCHEMA_TOPIC, key=key, value=value)
        else:
            key = maybe_key_val[0]
            kafka_message = AlwaysFineKafkaMessage(i, timestamp_tuple, DEFAULT_SCHEMA_TOPIC, key=key)

        kafka_messages.append(kafka_message)

    schema_reader.consume_messages(kafka_messages, False)
    duplicates = database.duplicates()

    schema_id_to_duplicated_subjects = compute_schema_id_to_subjects(duplicates, database.subject_to_subject_data())
    assert schema_id_to_duplicated_subjects == {}, "there shouldn't be any duplicated schemas"
    assert duplicates == {}, "the schema database is broken. The id should be unique"


@pytest.fixture(name="db_with_schemas")
def fixture_in_memory_database_with_schemas() -> InMemoryDatabase:
    db = InMemoryDatabase()
    schema_str = "syntax = 'proto3'; message Test { string test = 1; }"

    subject_a = Subject("subject_a")
    schema_a = TypedSchema(
        schema_type=SchemaType.PROTOBUF,
        schema_str=schema_str,
        schema=ProtobufSchema(schema=schema_str),
    )
    db.insert_subject(subject=subject_a)
    schema_id_a = db.get_schema_id(schema_a)
    db.insert_schema_version(
        subject=subject_a, schema_id=schema_id_a, version=Version(1), schema=schema_a, deleted=False, references=None
    )
    db.insert_schema_version(
        subject=subject_a, schema_id=schema_id_a, version=Version(2), schema=schema_a, deleted=False, references=None
    )

    subject_b = Subject("subject_b")
    references_b = [Reference(name="test", subject=subject_a, version=Version(1))]
    schema_b = TypedSchema(
        schema_type=SchemaType.PROTOBUF,
        schema_str=schema_str,
        schema=ProtobufSchema(schema=schema_str),
        references=references_b,
    )
    db.insert_subject(subject=subject_b)
    schema_id_b = db.get_schema_id(schema_b)
    db.insert_schema_version(
        subject=subject_b,
        schema_id=schema_id_b,
        version=Version(1),
        schema=schema_b,
        deleted=False,
        references=references_b,
    )

    return db


def test_delete_schema_references(db_with_schemas: InMemoryDatabase) -> None:
    # Check that the schema is referenced by subject_b
    referents = db_with_schemas.get_referenced_by(subject=Subject("subject_a"), version=Version(1))
    assert referents is not None
    version = db_with_schemas.find_schema_versions_by_schema_id(schema_id=referents.pop(), include_deleted=False)[0]
    assert version.subject == Subject("subject_b")
    assert version.version == Version(1)

    # Delete the schema from subject_b
    db_with_schemas.delete_subject_schema(subject=Subject("subject_b"), version=Version(1))

    # Check that the schema is no longer referenced by subject_b
    referents = db_with_schemas.get_referenced_by(subject=Subject("subject_a"), version=Version(1))
    assert len(referents) == 0, "referents should be gone after deleting the schema"


def test_delete_subject(db_with_schemas: InMemoryDatabase) -> None:
    # Check that the schema is referenced by subject_b
    referents = db_with_schemas.get_referenced_by(subject=Subject("subject_a"), version=Version(1))
    assert referents is not None
    version = db_with_schemas.find_schema_versions_by_schema_id(schema_id=referents.pop(), include_deleted=False)[0]
    assert version.subject == Subject("subject_b")
    assert version.version == Version(1)

    # Hard delete subject_b
    db_with_schemas.delete_subject_hard(subject=Subject("subject_b"))

    # Check that the schema is no longer referenced by subject_b
    referents = db_with_schemas.get_referenced_by(subject=Subject("subject_a"), version=Version(1))
    assert len(referents) == 0, "referents should be gone after hard deleting the subject"


def _avro_schema(name: str = "Obj") -> TypedSchema:
    return TypedSchema(
        schema_type=SchemaType.AVRO,
        schema_str=f'{{"type": "record", "name": "{name}", "fields": []}}',
    )


class TestLogState:
    def test_logs_debug_summary_when_debug_enabled(self, caplog: pytest.LogCaptureFixture) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema()
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        with caplog.at_level(logging.DEBUG, logger="karapace.core.in_memory_database"):
            db.log_state()

        assert any("Schemas:" in record.message for record in caplog.records)

    def test_noop_when_debug_disabled(self, caplog: pytest.LogCaptureFixture) -> None:
        db = InMemoryDatabase()
        with caplog.at_level(logging.INFO, logger="karapace.core.in_memory_database"):
            db.log_state()
        assert caplog.records == []


class TestGetSchemaIdDeduplication:
    def test_returns_same_id_for_identical_schema_content(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema()
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        duplicate = _avro_schema()
        assert db.get_schema_id(duplicate) == schema_id

    def test_returns_new_id_for_distinct_schema_content(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema("First")
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        other_id = db.get_schema_id(_avro_schema("Second"))
        assert other_id != schema_id


class TestInsertSchemaVersionUpdatesExistingVersion:
    def test_reinserting_same_version_logs_update_not_add(self, caplog: pytest.LogCaptureFixture) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema()
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)

        with caplog.at_level(logging.INFO, logger="karapace.core.in_memory_database"):
            db.insert_schema_version(
                subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
            )
            caplog.clear()
            db.insert_schema_version(
                subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=True, references=None
            )

        assert any("Updating entry" in record.message for record in caplog.records)
        assert db.find_subject_schemas(subject=subject, include_deleted=True)[Version(1)].deleted is True


class TestSchemaIdOnSubjectCleanup:
    def test_marking_schema_deleted_removes_empty_subject_entry(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema()
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )
        assert subject in db._hash_to_schema_id_on_subject

        # Re-inserting the same (subject, version) as deleted must clean up the
        # now-empty per-subject fingerprint index, not merely leave it with no fingerprints.
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=True, references=None
        )

        assert subject not in db._hash_to_schema_id_on_subject


class TestCompatibilitySettings:
    def test_delete_subject_compatibility_clears_existing_value(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        db.set_subject_compatibility(subject=subject, compatibility="FULL")
        assert db.get_subject_compatibility(subject=subject) == "FULL"

        db.delete_subject_compatibility(subject=subject)

        assert db.get_subject_compatibility(subject=subject) is None

    def test_set_subject_compatibility_is_noop_for_unknown_subject(self) -> None:
        db = InMemoryDatabase()
        # Must not raise even though "unknown" was never inserted.
        db.set_subject_compatibility(subject=Subject("unknown"), compatibility="FULL")
        assert db.get_subject_compatibility(subject=Subject("unknown")) is None

    def test_delete_subject_compatibility_is_noop_for_unknown_subject(self) -> None:
        db = InMemoryDatabase()
        # Must not raise even though "unknown" was never inserted.
        db.delete_subject_compatibility(subject=Subject("unknown"))


class TestFindSchemasVariants:
    def _db_with_two_versions(self) -> InMemoryDatabase:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        schema_v1 = _avro_schema("First")
        schema_id_v1 = db.get_schema_id(schema_v1)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id_v1, version=Version(1), schema=schema_v1, deleted=True, references=None
        )
        schema_v2 = _avro_schema("Second")
        schema_id_v2 = db.get_schema_id(schema_v2)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id_v2, version=Version(2), schema=schema_v2, deleted=False, references=None
        )
        return db

    def test_latest_only_returns_single_most_recent_version(self) -> None:
        db = self._db_with_two_versions()
        result = db.find_schemas(include_deleted=False, latest_only=True)
        versions = result[Subject("s")]
        assert len(versions) == 1
        assert versions[0].version == Version(2)

    def test_include_deleted_true_excludes_soft_deleted_versions(self) -> None:
        # NOTE: The `include_deleted` parameter has inverted semantics in the implementation:
        # `include_deleted=True` actually *excludes* deleted schemas from the result.
        db = self._db_with_two_versions()
        result = db.find_schemas(include_deleted=True, latest_only=False)
        versions = result[Subject("s")]
        assert all(not v.deleted for v in versions)
        assert len(versions) == 1


class TestSubjectsForSchema:
    def test_finds_all_subjects_referencing_a_schema_id(self) -> None:
        db = InMemoryDatabase()
        schema = _avro_schema()
        schema_id = db.get_schema_id(schema)
        for name in ("subject_a", "subject_b"):
            subject = Subject(name)
            db.insert_subject(subject=subject)
            db.insert_schema_version(
                subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
            )

        assert set(db.subjects_for_schema(schema_id)) == {Subject("subject_a"), Subject("subject_b")}

    def test_excludes_deleted_versions(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema()
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=True, references=None
        )

        assert db.subjects_for_schema(schema_id) == []


class TestDeleteSubjectSoft:
    def test_marks_versions_up_to_given_version_as_deleted(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        for v, name in ((1, "First"), (2, "Second"), (3, "Third")):
            schema = _avro_schema(name)
            schema_id = db.get_schema_id(schema)
            db.insert_schema_version(
                subject=subject, schema_id=schema_id, version=Version(v), schema=schema, deleted=False, references=None
            )

        db.delete_subject(subject=subject, version=Version(2))

        all_versions = db.find_subject_schemas(subject=subject, include_deleted=True)
        assert all_versions[Version(1)].deleted is True
        assert all_versions[Version(2)].deleted is True
        assert all_versions[Version(3)].deleted is False


class TestDeleteSubjectHardWithoutReferences:
    def test_hard_delete_subject_with_no_references_does_not_touch_referenced_by(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema()
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        # Must not raise even though this schema has no `references` to clean up.
        db.delete_subject_hard(subject=subject)

        assert db.find_subject(subject=subject) is None


class TestDeleteSubjectSchemaEdgeCases:
    def test_deleting_nonexistent_version_is_a_noop(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)

        # Must not raise even though version 99 was never inserted.
        db.delete_subject_schema(subject=subject, version=Version(99))

    def test_deleting_version_without_references_does_not_touch_referenced_by(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        schema = _avro_schema()
        db.insert_subject(subject=subject)
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        db.delete_subject_schema(subject=subject, version=Version(1))

        assert db.find_subject_schemas(subject=subject, include_deleted=True) == {}


class TestNumSchemaVersions:
    def test_counts_live_and_soft_deleted_versions_separately(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        live_schema = _avro_schema("Live")
        live_id = db.get_schema_id(live_schema)
        db.insert_schema_version(
            subject=subject, schema_id=live_id, version=Version(1), schema=live_schema, deleted=False, references=None
        )
        deleted_schema = _avro_schema("Deleted")
        deleted_id = db.get_schema_id(deleted_schema)
        db.insert_schema_version(
            subject=subject, schema_id=deleted_id, version=Version(2), schema=deleted_schema, deleted=True, references=None
        )

        live, soft_deleted = db.num_schema_versions()

        assert live == 1
        assert soft_deleted == 1


class TestReferencedByAggregation:
    def test_multiple_referents_accumulate_on_same_referenced_version(self) -> None:
        db = InMemoryDatabase()
        referenced_subject = Subject("referenced")
        db.insert_subject(subject=referenced_subject)
        referenced_schema = _avro_schema("Referenced")
        referenced_id = db.get_schema_id(referenced_schema)
        db.insert_schema_version(
            subject=referenced_subject,
            schema_id=referenced_id,
            version=Version(1),
            schema=referenced_schema,
            deleted=False,
            references=None,
        )

        reference = Reference(name="ref", subject=referenced_subject, version=Version(1))

        for name in ("consumer_a", "consumer_b"):
            consumer_subject = Subject(name)
            db.insert_subject(subject=consumer_subject)
            consumer_schema = _avro_schema(name)
            consumer_id = db.get_schema_id(consumer_schema)
            db.insert_schema_version(
                subject=consumer_subject,
                schema_id=consumer_id,
                version=Version(1),
                schema=consumer_schema,
                deleted=False,
                references=[reference],
            )

        referents = db.get_referenced_by(subject=referenced_subject, version=Version(1))
        assert referents is not None
        assert len(referents) == 2

    def test_removing_one_referent_leaves_the_other_intact(self) -> None:
        db = InMemoryDatabase()
        referenced_subject = Subject("referenced")
        db.insert_subject(subject=referenced_subject)
        referenced_schema = _avro_schema("Referenced")
        referenced_id = db.get_schema_id(referenced_schema)
        db.insert_schema_version(
            subject=referenced_subject,
            schema_id=referenced_id,
            version=Version(1),
            schema=referenced_schema,
            deleted=False,
            references=None,
        )
        reference = Reference(name="ref", subject=referenced_subject, version=Version(1))

        surviving_id = None
        for name in ("consumer_a", "consumer_b"):
            consumer_subject = Subject(name)
            db.insert_subject(subject=consumer_subject)
            consumer_schema = _avro_schema(name)
            consumer_id = db.get_schema_id(consumer_schema)
            if name == "consumer_b":
                surviving_id = consumer_id
            db.insert_schema_version(
                subject=consumer_subject,
                schema_id=consumer_id,
                version=Version(1),
                schema=consumer_schema,
                deleted=False,
                references=[reference],
            )

        db.delete_subject_schema(subject=Subject("consumer_a"), version=Version(1))

        referents = db.get_referenced_by(subject=referenced_subject, version=Version(1))
        assert referents == {surviving_id}


class TestGetSchemaIdIfExists:
    def test_returns_none_when_subject_unknown(self) -> None:
        db = InMemoryDatabase()
        schema = _avro_schema()

        result = db.get_schema_id_if_exists(subject=Subject("unknown"), schema=schema, include_deleted=False)

        assert result is None

    def test_returns_none_when_subject_known_but_schema_not_registered(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        registered_schema = _avro_schema("Registered")
        schema_id = db.get_schema_id(registered_schema)
        db.insert_schema_version(
            subject=subject,
            schema_id=schema_id,
            version=Version(1),
            schema=registered_schema,
            deleted=False,
            references=None,
        )

        result = db.get_schema_id_if_exists(subject=subject, schema=_avro_schema("NotRegistered"), include_deleted=False)

        assert result is None

    def test_returns_schema_id_when_registered_on_subject(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        schema = _avro_schema()
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        result = db.get_schema_id_if_exists(subject=subject, schema=schema, include_deleted=False)

        assert result == schema_id


class TestGetNextVersion:
    def test_returns_one_past_highest_existing_version(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        schema_v1 = _avro_schema("First")
        schema_id_v1 = db.get_schema_id(schema_v1)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id_v1, version=Version(1), schema=schema_v1, deleted=False, references=None
        )
        schema_v2 = _avro_schema("Second")
        schema_id_v2 = db.get_schema_id(schema_v2)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id_v2, version=Version(2), schema=schema_v2, deleted=False, references=None
        )

        assert db.get_next_version(subject=subject) == Version(3)


class TestFindSubjects:
    def test_include_deleted_false_excludes_fully_deleted_subjects(self) -> None:
        db = InMemoryDatabase()
        live_subject = Subject("live")
        db.insert_subject(subject=live_subject)
        live_schema = _avro_schema("Live")
        live_id = db.get_schema_id(live_schema)
        db.insert_schema_version(
            subject=live_subject, schema_id=live_id, version=Version(1), schema=live_schema, deleted=False, references=None
        )

        deleted_subject = Subject("deleted")
        db.insert_subject(subject=deleted_subject)
        deleted_schema = _avro_schema("Deleted")
        deleted_id = db.get_schema_id(deleted_schema)
        db.insert_schema_version(
            subject=deleted_subject,
            schema_id=deleted_id,
            version=Version(1),
            schema=deleted_schema,
            deleted=True,
            references=None,
        )

        assert db.find_subjects(include_deleted=False) == [live_subject]
        assert set(db.find_subjects(include_deleted=True)) == {live_subject, deleted_subject}


class TestInsertSchemaVersionImplicitSubjectCreation:
    def test_creates_subject_when_it_does_not_exist_yet(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("brand-new")
        schema = _avro_schema()
        schema_id = db.get_schema_id(schema)

        assert db.find_subject(subject=subject) is None

        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        assert db.find_subject(subject=subject) == subject
        assert db.subjects[subject].schemas[Version(1)].schema_id == schema_id


class TestFindSchema:
    def test_returns_the_registered_schema(self) -> None:
        db = InMemoryDatabase()
        subject = Subject("s")
        db.insert_subject(subject=subject)
        schema = _avro_schema()
        schema_id = db.get_schema_id(schema)
        db.insert_schema_version(
            subject=subject, schema_id=schema_id, version=Version(1), schema=schema, deleted=False, references=None
        )

        assert db.find_schema(schema_id=schema_id) == schema


class TestFindSubjectSchemas:
    def test_returns_empty_dict_for_unknown_subject(self) -> None:
        db = InMemoryDatabase()

        assert db.find_subject_schemas(subject=Subject("unknown"), include_deleted=False) == {}
