"""
karapace - test schema backup

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import asyncio
import json
import os
import time
from pathlib import Path
from unittest import mock

import pytest
from aiokafka.errors import InvalidTopicError

from karapace.backup import api
from karapace.backup.api import BackupVersion
from karapace.backup.errors import StaleConsumerError
from karapace.backup.poll_timeout import PollTimeout
from karapace.core.client import Client
from karapace.core.config import Config
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka.common import KafkaError
from karapace.core.kafka.consumer import KafkaConsumer
from karapace.core.key_format import is_key_in_canonical_format
from karapace.core.utils import Expiration
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import StubMessage, new_random_name

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
    registry_async_client: Client,
    kafka_servers: KafkaServers,
    tmp_path: Path,
    registry_cluster: RegistryDescription,
) -> None:
    await insert_data(registry_async_client)

    # Get the backup
    backup_location = tmp_path / "schemas.log"
    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.topic_name = registry_cluster.schemas_topic

    api.create_backup(
        config=config,
        backup_location=backup_location,
        topic_name=api.normalize_topic_name(None, config),
        version=BackupVersion.V2,
    )

    # The backup file has been created
    assert os.path.exists(backup_location)

    lines = 0
    with open(backup_location, encoding="utf8") as fp:
        version_line = fp.readline()
        assert version_line.rstrip() == "/V2"
        for line in fp:
            lines += 1
            data = line.split("\t")
            assert len(data) == 2

    assert lines == 1


async def test_backup_restore_and_get_non_schema_topic(
    kafka_servers: KafkaServers, tmp_path: Path, admin_client: KafkaAdminClient
) -> None:
    test_topic_name = new_random_name("non-schemas")

    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]

    admin_client.new_topic(name=test_topic_name)

    # Restore from backup
    test_data_path = Path("tests/integration/test_data/")
    restore_location = test_data_path / "test_restore_non_schema_topic_v2.log"
    api.restore_backup(
        config=config,
        backup_location=restore_location,
        topic_name=api.normalize_topic_name(test_topic_name, config),
    )

    # Get the backup
    backup_location = tmp_path / "non_schemas_topic.log"
    api.create_backup(
        config=config,
        backup_location=backup_location,
        topic_name=api.normalize_topic_name(test_topic_name, config),
        version=BackupVersion.V2,
    )
    # The backup file has been created
    assert os.path.exists(backup_location)

    restore_file_content = None
    with open(restore_location, encoding="utf8") as fp:
        restore_file_content = fp.read()
    backup_file_content = None
    with open(backup_location, encoding="utf8") as fp:
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
        topic=schemas_topic,
        group_id="assert-canonical-key-format-consumer",
        enable_auto_commit=False,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
    )

    messages = consumer.consume(timeout=30)
    while messages:
        for message in messages:
            key = json.loads(message.key())
            assert is_key_in_canonical_format(key), f"Not in canonical format: {key}"
        messages = consumer.consume(timeout=30)
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
    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.topic_name = registry_cluster.schemas_topic
    config.force_key_correction = True

    # Test basic restore functionality
    restore_location = test_data_path / f"test_restore_{backup_file_version}.log"

    api.restore_backup(
        config=config,
        backup_location=restore_location,
        topic_name=api.normalize_topic_name(None, config),
    )

    # The restored karapace should have the previously created subject
    all_subjects = []
    expiration = Expiration.from_timeout(timeout=30)
    while subject not in all_subjects:
        expiration.raise_timeout_if_expired(
            msg_format="{subject} not in {all_subjects}",
            subject=subject,
            all_subjects=all_subjects,
        )
        res = await registry_async_client.get("subjects")
        assert res.status_code == 200
        all_subjects = res.json()
        time.sleep(1)

    # Test a few exotic scenarios

    # Restore a compatibility config remove message
    subject = "compatibility-config-remove"
    restore_location = test_data_path / f"test_restore_compatibility_config_remove_{backup_file_version}.log"

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "FORWARD"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FORWARD"
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FORWARD"
    api.restore_backup(
        config=config,
        backup_location=restore_location,
        topic_name=api.normalize_topic_name(None, config),
    )
    await asyncio.sleep(1)
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status_code == 404

    # Restore a complete schema delete message
    subject = "complete-schema-delete-version"
    restore_location = test_data_path / f"test_restore_complete_schema_delete_{backup_file_version}.log"

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "NONE"})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "int"}'})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "float"}'})
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1, 2]
    api.restore_backup(
        config=config,
        backup_location=restore_location,
        topic_name=api.normalize_topic_name(None, config),
    )
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1]

    # Schema delete for a nonexistent subject version is ignored
    subject = "delete-nonexistent-schema"
    restore_location = test_data_path / f"test_restore_delete_nonexistent_schema_{backup_file_version}.log"
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "string"}'})
    api.restore_backup(
        config=config,
        backup_location=restore_location,
        topic_name=api.normalize_topic_name(None, config),
    )
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1]

    _assert_canonical_key_format(
        bootstrap_servers=kafka_servers.bootstrap_servers, schemas_topic=registry_cluster.schemas_topic
    )


async def test_stale_consumer(
    kafka_servers: KafkaServers,
    registry_async_client: Client,
    registry_cluster: RegistryDescription,
    tmp_path: Path,
) -> None:
    await insert_data(registry_async_client)
    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.topic_name = registry_cluster.schemas_topic

    with pytest.raises(StaleConsumerError) as e:
        # The proper way to test this would be with quotas by throttling our client to death while using a very short
        # poll timeout. However, we have no way to set up quotas because all Kafka clients available to us do not
        # implement the necessary APIs.
        with mock.patch(f"{KafkaConsumer.__module__}.{KafkaConsumer.__qualname__}.poll") as poll_mock:
            poll_mock.return_value = None
            api.create_backup(
                config=config,
                backup_location=tmp_path / "backup",
                topic_name=api.normalize_topic_name(None, config),
                version=BackupVersion.V2,
                poll_timeout=PollTimeout.of(seconds=1),
            )
    assert str(e.value) == f"{registry_cluster.schemas_topic}:0#0 (0,0) after PT1S"


async def test_message_error(
    kafka_servers: KafkaServers,
    registry_async_client: Client,
    registry_cluster: RegistryDescription,
    tmp_path: Path,
) -> None:
    await insert_data(registry_async_client)
    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.topic_name = registry_cluster.schemas_topic

    with pytest.raises(InvalidTopicError):
        with mock.patch(f"{KafkaConsumer.__module__}.{KafkaConsumer.__qualname__}.poll") as poll_mock:
            poll_mock.return_value = StubMessage(error=KafkaError(KafkaError.TOPIC_EXCEPTION))
            api.create_backup(
                config=config,
                backup_location=tmp_path / "backup",
                topic_name=api.normalize_topic_name(None, config),
                version=BackupVersion.V2,
                poll_timeout=PollTimeout.of(seconds=1),
            )
