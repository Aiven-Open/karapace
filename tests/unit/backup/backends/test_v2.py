"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from functools import partial
from kafka.consumer.fetcher import ConsumerRecord
from karapace.backup.backends.reader import ProducerSend, RestoreTopic
from karapace.backup.backends.v2 import AnonymizeAvroWriter, SchemaBackupV2Reader, SchemaBackupV2Writer
from karapace.backup.encoders import encode_key, encode_value
from karapace.key_format import KeyFormatter
from pathlib import Path

import datetime
import json
import time


def get_reader() -> SchemaBackupV2Reader:
    return SchemaBackupV2Reader(
        key_encoder=partial(encode_key, key_formatter=KeyFormatter()),
        value_encoder=encode_value,
    )


def test_schema_backup_v2_roundtrip(tmp_path: Path) -> None:
    backup_path = tmp_path / "a-topic.data"
    topic_name = "a-topic"
    partition_index = 123
    records = (
        ConsumerRecord(
            key=json.dumps(
                {
                    "keytype": "SCHEMA",
                    "magic": 1,
                    "subject": "subject-1",
                    "version": 1,
                }
            ).encode(),
            value=json.dumps(
                {
                    "deleted": False,
                    "id": 1,
                    "schema": '"string"',
                    "subject": "subject-1",
                    "version": 1,
                }
            ).encode(),
            topic=topic_name,
            partition=partition_index,
            offset=0,
            timestamp=round(time.time()),
            timestamp_type=None,
            headers=(),
            checksum=None,
            serialized_key_size=None,
            serialized_value_size=None,
            serialized_header_size=None,
        ),
        ConsumerRecord(
            key=json.dumps(
                {
                    "keytype": "SCHEMA",
                    "magic": 1,
                    "subject": "subject-1",
                    "version": 2,
                }
            ).encode(),
            value=json.dumps(
                {
                    "deleted": False,
                    "id": 2,
                    "schema": '"string"',
                    "subject": "subject-1",
                    "version": 2,
                }
            ).encode(),
            topic=topic_name,
            partition=partition_index,
            offset=0,
            timestamp=round(time.time()),
            timestamp_type=None,
            headers=(),
            checksum=None,
            serialized_key_size=None,
            serialized_value_size=None,
            serialized_header_size=None,
        ),
    )

    # Write backup to file.
    backup_writer = SchemaBackupV2Writer()
    file_path = backup_writer.start_partition(
        path=backup_path,
        topic_name=topic_name,
        index=partition_index,
    )
    with backup_writer.safe_writer(file_path, False) as buffer:
        for record in records:
            backup_writer.store_record(buffer, record)
    data_file = backup_writer.finalize_partition(  # pylint: disable=assignment-from-no-return
        index=partition_index,
        filename=file_path.name,
    )
    backup_writer.store_metadata(
        path=backup_path,
        topic_name=topic_name,
        topic_id=None,
        started_at=datetime.datetime.now(datetime.timezone.utc),
        finished_at=datetime.datetime.now(datetime.timezone.utc),
        data_files=(data_file,),
    )

    reader = get_reader()
    (
        restore_topic,
        first_send,
        second_send,
    ) = reader.read(backup_path, topic_name)

    assert restore_topic == RestoreTopic(name=topic_name, partition_count=1)

    # First message.
    assert isinstance(first_send, ProducerSend)
    assert first_send.topic_name == topic_name
    assert first_send.partition_index == 0
    assert json.loads(first_send.key) == {
        "keytype": "SCHEMA",
        "magic": 1,
        "subject": "subject-1",
        "version": 1,
    }
    assert json.loads(first_send.value) == {
        "deleted": False,
        "id": 1,
        "schema": '"string"',
        "subject": "subject-1",
        "version": 1,
    }

    # Second message.
    assert isinstance(second_send, ProducerSend)
    assert second_send.topic_name == topic_name
    assert second_send.partition_index == 0
    assert json.loads(second_send.key) == {
        "keytype": "SCHEMA",
        "magic": 1,
        "subject": "subject-1",
        "version": 2,
    }
    assert json.loads(second_send.value) == {
        "deleted": False,
        "id": 2,
        "schema": '"string"',
        "subject": "subject-1",
        "version": 2,
    }


def test_anonymize_avro_roundtrip(tmp_path: Path) -> None:
    backup_path = tmp_path / "a-topic.data"
    topic_name = "a-topic"
    partition_index = 123
    records = (
        ConsumerRecord(
            key=json.dumps(
                {
                    "keytype": "SCHEMA",
                    "subject": "avro-schemas",
                    "version": 1,
                    "magic": 1,
                }
            ).encode(),
            value=json.dumps(
                {
                    "subject": "avro-schemas",
                    "version": 1,
                    "id": 1,
                    "schema": json.dumps(
                        {
                            "fields": [{"name": "f1", "type": "string"}],
                            "name": "myrecord",
                            "namespace": "io.aiven",
                            "type": "record",
                        }
                    ),
                    "deleted": False,
                }
            ).encode(),
            topic=topic_name,
            partition=partition_index,
            offset=0,
            timestamp=round(time.time()),
            timestamp_type=None,
            headers=(),
            checksum=None,
            serialized_key_size=None,
            serialized_value_size=None,
            serialized_header_size=None,
        ),
        ConsumerRecord(
            key=json.dumps(
                {
                    "keytype": "SCHEMA",
                    "subject": "avro-schemas",
                    "version": 2,
                    "magic": 1,
                }
            ).encode(),
            value=json.dumps(
                {
                    "subject": "avro-schemas",
                    "version": 1,
                    "id": 2,
                    "schema": json.dumps(
                        {
                            "fields": [{"name": "f1", "type": "string"}],
                            "name": "myrecord",
                            "namespace": "io.aiven",
                            "type": "record",
                        }
                    ),
                    "deleted": False,
                }
            ).encode(),
            topic=topic_name,
            partition=partition_index,
            offset=0,
            timestamp=round(time.time()),
            timestamp_type=None,
            headers=(),
            checksum=None,
            serialized_key_size=None,
            serialized_value_size=None,
            serialized_header_size=None,
        ),
    )

    # Write backup to file.
    backup_writer = AnonymizeAvroWriter()
    file_path = backup_writer.start_partition(
        path=backup_path,
        topic_name=topic_name,
        index=partition_index,
    )
    with backup_writer.safe_writer(file_path, False) as buffer:
        for record in records:
            backup_writer.store_record(buffer, record)
    data_file = backup_writer.finalize_partition(  # pylint: disable=assignment-from-no-return
        index=partition_index,
        filename=file_path.name,
    )
    backup_writer.store_metadata(
        path=backup_path,
        topic_name=topic_name,
        topic_id=None,
        started_at=datetime.datetime.now(datetime.timezone.utc),
        finished_at=datetime.datetime.now(datetime.timezone.utc),
        data_files=(data_file,),
    )

    # Only the backup file, and no temporary files exist in backup directory.
    assert tuple(backup_path.parent.iterdir()) == (backup_path,)

    reader = get_reader()

    (
        restore_topic,
        first_send,
        second_send,
    ) = reader.read(backup_path, topic_name)

    assert restore_topic == RestoreTopic(name=topic_name, partition_count=1)

    # First message.
    assert isinstance(first_send, ProducerSend)
    assert first_send.topic_name == topic_name
    assert first_send.partition_index == 0
    assert json.loads(first_send.key) == {
        "keytype": "SCHEMA",
        "subject": "a801beafef1fb8c03907b44ec7baca341a58420d-",
        "version": 1,
        "magic": 1,
    }
    assert json.loads(first_send.value) == {
        "subject": "a801beafef1fb8c03907b44ec7baca341a58420d-",
        "version": 1,
        "id": 1,
        "schema": (
            '{"fields":[{"name":"a09bb890b096f7306f688cc6d1dad34e7e52a223","type":"string"}],'
            '"name":"afe8733e983101f1f4ff50d24152890d0da71418",'
            '"namespace":"aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",'
            '"type":"record"}'
        ),
        "deleted": False,
    }

    # Second message.
    assert isinstance(second_send, ProducerSend)
    assert second_send.topic_name == topic_name
    assert second_send.partition_index == 0
    assert json.loads(second_send.key) == {
        "keytype": "SCHEMA",
        "subject": "a801beafef1fb8c03907b44ec7baca341a58420d-",
        "version": 2,
        "magic": 1,
    }
    assert json.loads(second_send.value) == {
        "subject": "a801beafef1fb8c03907b44ec7baca341a58420d-",
        "version": 1,
        "id": 2,
        "schema": (
            '{"fields":[{"name":"a09bb890b096f7306f688cc6d1dad34e7e52a223","type":"string"}],'
            '"name":"afe8733e983101f1f4ff50d24152890d0da71418",'
            '"namespace":"aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",'
            '"type":"record"}'
        ),
        "deleted": False,
    }


def test_yields_restore_topic_for_empty_file(tmp_file: Path) -> None:
    reader = get_reader()
    tmp_file.write_text("/V2\n")
    instructions = tuple(reader.read(tmp_file, "a-topic"))
    assert instructions == (RestoreTopic(name="a-topic", partition_count=1),)
