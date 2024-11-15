"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from contextlib import closing
from dataclasses import dataclass
from karapace.config import Config
from karapace.constants import DEFAULT_SCHEMA_TOPIC
from karapace.coordinator.master_coordinator import MasterCoordinator
from karapace.in_memory_database import InMemoryDatabase
from karapace.kafka.admin import KafkaAdminClient
from karapace.kafka.producer import KafkaProducer
from karapace.key_format import KeyFormatter, KeyMode
from karapace.offset_watcher import OffsetWatcher
from karapace.schema_reader import KafkaSchemaReader
from karapace.typing import PrimaryInfo
from karapace.utils import json_encode
from tests.base_testcase import BaseTestCase
from tests.integration.test_master_coordinator import AlwaysAvailableSchemaReaderStoppper
from tests.integration.utils.kafka_server import KafkaServers
from tests.schemas.json_schemas import FALSE_SCHEMA, TRUE_SCHEMA
from tests.utils import create_group_name_factory, create_subject_name_factory, new_random_name, new_topic

import asyncio
import pytest


async def _wait_until_reader_is_ready_and_master(
    master_coordinator: MasterCoordinator,
    reader: KafkaSchemaReader,
) -> None:
    """Wait until the reader starts setting Events for seen offsets.

    - A `reader` is ready once it caught up with the pre-existing data in a topic.
    - A `reader` learns it is the master after the `master_coordinator` election.
    """
    # Caught up with the topic
    while not reader.ready:
        await asyncio.sleep(0.1)

    # Won master election
    primary_info = PrimaryInfo(primary=False, primary_url=None)
    while not primary_info.primary:
        primary_info = master_coordinator.get_master_info()
        await asyncio.sleep(0.1)


async def test_regression_soft_delete_schemas_should_be_registered(
    kafka_servers: KafkaServers,
    producer: KafkaProducer,
) -> None:
    """Soft deleted schemas should be registered in the subject.

    The following would happen when:
    - Schema 1 is registered
    - Schema 2 is registered
    - Schema 2 is soft deleted
    - Topic is compacted
    - Server is started
    """
    test_name = "test_regression_soft_delete_schemas_should_be_registered"
    topic_name = new_random_name("topic")
    subject = create_subject_name_factory(test_name)()
    group_id = create_group_name_factory(test_name)()

    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.admin_metadata_max_age = 2
    config.group_id = group_id
    config.topic_name = topic_name

    master_coordinator = MasterCoordinator(config=config)
    master_coordinator.set_stoppper(AlwaysAvailableSchemaReaderStoppper())
    try:
        master_coordinator.start()
        database = InMemoryDatabase()
        offset_watcher = OffsetWatcher()
        schema_reader = KafkaSchemaReader(
            config=config,
            offset_watcher=offset_watcher,
            key_formatter=KeyFormatter(),
            master_coordinator=master_coordinator,
            database=database,
        )
        schema_reader.start()

        with closing(schema_reader):
            await _wait_until_reader_is_ready_and_master(master_coordinator, schema_reader)

            # Send an initial schema to initialize the subject in the reader, this is preparing the state
            key = {
                "subject": subject,
                "version": 1,
                "magic": 1,
                "keytype": "SCHEMA",
            }
            value = {
                "deleted": False,
                "id": 1,
                "subject": subject,
                "version": 1,
                "schema": json_encode(TRUE_SCHEMA.schema),
            }
            future = producer.send(
                topic_name,
                key=json_encode(key, binary=True),
                value=json_encode(value, binary=True),
            )
            producer.flush()
            msg = future.result()

            schema_reader._offset_watcher.wait_for_offset(msg.offset(), timeout=5)  # pylint: disable=protected-access

            schemas = database.find_subject_schemas(subject=subject, include_deleted=True)
            assert len(schemas) == 1, "Deleted schemas must have been registered"

            # Produce a soft deleted schema, this is the regression test
            key = {
                "subject": subject,
                "version": 2,
                "magic": 1,
                "keytype": "SCHEMA",
            }
            test_global_schema_id = 2
            value = {
                "deleted": True,
                "id": test_global_schema_id,
                "subject": subject,
                "version": 2,
                "schema": json_encode(FALSE_SCHEMA.schema),
            }
            future = producer.send(
                topic_name,
                key=json_encode(key, binary=True),
                value=json_encode(value, binary=True),
            )
            producer.flush()
            msg = future.result()

            seen = schema_reader._offset_watcher.wait_for_offset(msg.offset(), timeout=5)  # pylint: disable=protected-access
            assert seen is True
            assert database.global_schema_id == test_global_schema_id

            schemas = database.find_subject_schemas(subject=subject, include_deleted=True)
            assert len(schemas) == 2, "Deleted schemas must have been registered"
    finally:
        await master_coordinator.close()


async def test_regression_config_for_inexisting_object_should_not_throw(
    kafka_servers: KafkaServers,
    producer: KafkaProducer,
) -> None:
    test_name = "test_regression_config_for_inexisting_object_should_not_throw"
    subject = create_subject_name_factory(test_name)()
    group_id = create_group_name_factory(test_name)()

    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.admin_metadata_max_age = 2
    config.group_id = group_id

    master_coordinator = MasterCoordinator(config=config)
    master_coordinator.set_stoppper(AlwaysAvailableSchemaReaderStoppper())
    try:
        master_coordinator.start()
        database = InMemoryDatabase()
        offset_watcher = OffsetWatcher()
        schema_reader = KafkaSchemaReader(
            config=config,
            offset_watcher=offset_watcher,
            key_formatter=KeyFormatter(),
            master_coordinator=master_coordinator,
            database=database,
        )
        schema_reader.start()

        with closing(schema_reader):
            await _wait_until_reader_is_ready_and_master(master_coordinator, schema_reader)

            # Send an initial schema to initialize the subject in the reader, this is preparing the state
            key = {
                "subject": subject,
                "magic": 0,
                "keytype": "CONFIG",
            }
            value = ""  # Delete the config

            future = producer.send(
                DEFAULT_SCHEMA_TOPIC,
                key=json_encode(key, binary=True),
                value=json_encode(value, binary=True),
            )
            producer.flush()
            msg = future.result()

            seen = schema_reader._offset_watcher.wait_for_offset(msg.offset(), timeout=5)  # pylint: disable=protected-access
            assert seen is True
            assert database.find_subject(subject=subject) is not None, "The above message should be handled gracefully"
    finally:
        await master_coordinator.close()


@dataclass
class DetectKeyFormatCase(BaseTestCase):
    raw_msgs: list[tuple[bytes, bytes]]
    expected: KeyMode


@pytest.mark.parametrize(
    "testcase",
    [
        # Canonical format
        DetectKeyFormatCase(
            test_name="Canonical format messages",
            raw_msgs=[
                (b'{"keytype":"CONFIG","subject":null,"magic":0}', b'{"compatibilityLevel":"BACKWARD"}'),
                (b'{"keytype":"CONFIG","subject":null,"magic":0}', b'{"compatibilityLevel":"BACKWARD"}'),
                (b'{"keytype":"CONFIG","subject":null,"magic":0}', b'{"compatibilityLevel":"BACKWARD"}'),
            ],
            expected=KeyMode.CANONICAL,
        ),
        DetectKeyFormatCase(
            test_name="Mixed format messages",
            raw_msgs=[
                (b'{"keytype":"CONFIG","subject":null,"magic":0}', b'{"compatibilityLevel":"BACKWARD"}'),
                (b'{"subject":null,"magic":0,"keytype":"CONFIG"}', b'{"compatibilityLevel":"BACKWARD"}'),
                (b'{"keytype":"CONFIG","subject":null,"magic":0}', b'{"compatibilityLevel":"BACKWARD"}'),
            ],
            expected=KeyMode.DEPRECATED_KARAPACE,
        ),
        DetectKeyFormatCase(
            test_name="Karapace format messages",
            raw_msgs=[
                (b'{"subject":null,"magic":0,"keytype":"CONFIG"}', b'{"compatibilityLevel":"BACKWARD"}'),
                (b'{"subject":null,"magic":0,"keytype":"CONFIG"}', b'{"compatibilityLevel":"BACKWARD"}'),
                (b'{"subject":null,"magic":0,"keytype":"CONFIG"}', b'{"compatibilityLevel":"BACKWARD"}'),
            ],
            expected=KeyMode.DEPRECATED_KARAPACE,
        ),
    ],
)
async def test_key_format_detection(
    testcase: DetectKeyFormatCase,
    kafka_servers: KafkaServers,
    producer: KafkaProducer,
    admin_client: KafkaAdminClient,
) -> None:
    group_id = create_group_name_factory(testcase.test_name)()
    test_topic = new_topic(admin_client)

    for message in testcase.raw_msgs:
        producer.send(
            test_topic,
            key=message[0],
            value=message[1],
        )
    producer.flush()

    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.admin_metadata_max_age = 2
    config.group_id = group_id
    config.topic_name = test_topic

    master_coordinator = MasterCoordinator(config=config)
    master_coordinator.set_stoppper(AlwaysAvailableSchemaReaderStoppper())
    try:
        master_coordinator.start()
        key_formatter = KeyFormatter()
        database = InMemoryDatabase()
        offset_watcher = OffsetWatcher()
        schema_reader = KafkaSchemaReader(
            config=config,
            offset_watcher=offset_watcher,
            key_formatter=key_formatter,
            master_coordinator=master_coordinator,
            database=database,
        )
        schema_reader.start()

        with closing(schema_reader):
            await _wait_until_reader_is_ready_and_master(master_coordinator, schema_reader)

        assert key_formatter.get_keymode() == testcase.expected
    finally:
        await master_coordinator.close()
