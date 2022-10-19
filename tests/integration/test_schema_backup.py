"""
karapace - test schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaConsumer
from karapace.client import Client
from karapace.config import set_config_defaults
from karapace.key_format import is_key_in_canonical_format
from karapace.schema_backup import SchemaBackup, serialize_record
from karapace.utils import Expiration
from pathlib import Path
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import new_random_name

import json
import os
import pytest
import time

baseurl = "http://localhost:8081"


async def insert_data(client: Client) -> str:
    subject = new_random_name("subject")
    res = await client.post(
        f"subjects/{subject}/versions",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    return subject


async def test_backup_get(
    registry_async_client,
    kafka_servers: KafkaServers,
    tmp_path: Path,
    registry_cluster: RegistryDescription,
) -> None:
    await insert_data(registry_async_client)

    # Get the backup
    backup_location = tmp_path / "schemas.log"
    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": registry_cluster.schemas_topic,
        }
    )
    sb = SchemaBackup(config, str(backup_location))
    sb.export(serialize_record)

    # The backup file has been created
    assert os.path.exists(backup_location)

    lines = 0
    with open(backup_location, "r", encoding="utf8") as fp:
        version_line = fp.readline()
        assert version_line.rstrip() == "/V2"
        for line in fp:
            lines += 1
            data = line.split("\t")
            assert len(data) == 2

    assert lines == 1


async def test_backup_restore_and_get_non_schema_topic(
    kafka_servers: KafkaServers,
    tmp_path: Path,
) -> None:
    test_topic_name = new_random_name("non-schemas")

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": test_topic_name,
        }
    )

    # Restore from backup
    test_data_path = Path("tests/integration/test_data/")
    restore_location = test_data_path / "test_restore_non_schema_topic_v2.log"
    sb = SchemaBackup(config, str(restore_location))
    sb.restore_backup()

    # Get the backup
    backup_location = tmp_path / "non_schemas_topic.log"
    sb = SchemaBackup(config, str(backup_location))
    sb.export(serialize_record)
    # The backup file has been created
    assert os.path.exists(backup_location)

    restore_file_content = None
    with open(restore_location, "r", encoding="utf8") as fp:
        restore_file_content = fp.read()
    backup_file_content = None
    with open(backup_location, "r", encoding="utf8") as fp:
        backup_file_content = fp.read()
    assert restore_file_content is not None
    assert backup_file_content is not None
    assert restore_file_content == backup_file_content


def _assert_canonical_key_format(
    bootstrap_servers: str,
    schemas_topic: str,
) -> None:
    # Consume all records and assert key order is canonical
    consumer = KafkaConsumer(
        schemas_topic,
        group_id="assert-canonical-key-format-consumer",
        enable_auto_commit=False,
        api_version=(1, 0, 0),
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
    )

    raw_msgs = consumer.poll(timeout_ms=2000)
    while raw_msgs:
        for _, messages in raw_msgs.items():
            for message in messages:
                key = json.loads(message.key)
                assert is_key_in_canonical_format(key), f"Not in canonical format: {key}"
        raw_msgs = consumer.poll()
    consumer.close()


@pytest.mark.parametrize("backup_file_version", ["v1", "v2"])
async def test_backup_restore(
    registry_async_client: Client,
    kafka_servers: KafkaServers,
    registry_cluster: RegistryDescription,
    backup_file_version: str,
) -> None:
    subject = "subject-1"
    test_data_path = Path("tests/integration/test_data/")
    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": registry_cluster.schemas_topic,
            "force_key_correction": True,
        }
    )

    # Test basic restore functionality
    restore_location = test_data_path / f"test_restore_{backup_file_version}.log"

    sb = SchemaBackup(config, str(restore_location))
    sb.restore_backup()

    # The restored karapace should have the previously created subject
    all_subjects = []
    expiration = Expiration.from_timeout(timeout=10)
    while subject not in all_subjects:
        expiration.raise_timeout_if_expired(
            msg_format="{subject} not in {all_subjects}",
            subject=subject,
            all_subjects=all_subjects,
        )
        res = await registry_async_client.get("subjects")
        assert res.status_code == 200
        all_subjects = res.json()
        time.sleep(0.1)

    # Test a few exotic scenarios

    # Restore a compatibility config remove message
    subject = "compatibility-config-remove"
    restore_location = test_data_path / f"test_restore_compatibility_config_remove_{backup_file_version}.log"
    sb = SchemaBackup(config, str(restore_location))

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "FORWARD"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FORWARD"
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FORWARD"
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status_code == 404

    # Restore a complete schema delete message
    subject = "complete-schema-delete-version"
    restore_location = test_data_path / f"test_restore_complete_schema_delete_{backup_file_version}.log"
    sb = SchemaBackup(config, str(restore_location))

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "NONE"})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "int"}'})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "float"}'})
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1, 2]
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1]

    # Schema delete for a nonexistent subject version is ignored
    subject = "delete-nonexistent-schema"
    restore_location = test_data_path / f"test_restore_delete_nonexistent_schema_{backup_file_version}.log"
    sb = SchemaBackup(config, str(restore_location))
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "string"}'})
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1]

    _assert_canonical_key_format(
        bootstrap_servers=kafka_servers.bootstrap_servers, schemas_topic=registry_cluster.schemas_topic
    )
