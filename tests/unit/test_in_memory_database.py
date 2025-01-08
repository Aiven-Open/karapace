"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from confluent_kafka.cimpl import KafkaError
from karapace.constants import DEFAULT_SCHEMA_TOPIC
from karapace.container import KarapaceContainer
from karapace.in_memory_database import InMemoryDatabase, KarapaceDatabase, Subject, SubjectData
from karapace.kafka.types import Timestamp
from karapace.key_format import KeyFormatter
from karapace.offset_watcher import OffsetWatcher
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_models import SchemaVersion, TypedSchema
from karapace.schema_references import Reference, Referents
from karapace.schema_type import SchemaType
from karapace.typing import SchemaId, Version
from pathlib import Path
from schema_registry.reader import KafkaSchemaReader
from typing import Final

import pytest

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
    restore_location = TEST_DATA_FOLDER / "schemas.log"
    schema_log = restore_location.read_text(encoding="utf-8").strip()

    database = WrappedInMemoryDatabase()
    schema_reader = KafkaSchemaReader(
        config=karapace_container.config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=KeyFormatter(),
        master_coordinator=None,
        database=database,
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
