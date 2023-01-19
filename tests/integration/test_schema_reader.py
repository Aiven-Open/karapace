"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from contextlib import closing
from dataclasses import dataclass
from kafka import KafkaAdminClient, KafkaProducer
from karapace.config import set_config_defaults
from karapace.constants import DEFAULT_SCHEMA_TOPIC
from karapace.key_format import KeyFormatter, KeyMode
from karapace.master_coordinator import MasterCoordinator
from karapace.schema_reader import KafkaSchemaReader
from karapace.utils import json_encode
from tests.base_testcase import BaseTestCase
from tests.integration.utils.kafka_server import KafkaServers
from tests.schemas.json_schemas import TRUE_SCHEMA
from tests.utils import create_group_name_factory, create_subject_name_factory, new_random_name, new_topic
from typing import List, Tuple

import pytest
import time


def _wait_until_reader_is_ready_and_master(
    master_coordinator: MasterCoordinator,
    reader: KafkaSchemaReader,
) -> None:
    """Wait until the reader starts setting Events for seen offsets.

    - A `reader` is ready once it caught up with the pre-existing data in a topic.
    - A `reader` learns it is the master after the `master_coordinator` election.
    """
    # Caught up with the topic
    while not reader.ready:
        time.sleep(0.1)

    # Won master election
    are_we_master = False
    while not are_we_master:
        are_we_master, _ = master_coordinator.get_master_info()
        time.sleep(0.1)


def test_regression_soft_delete_schemas_should_be_registered(
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

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "group_id": group_id,
            "topic_name": topic_name,
        }
    )
    master_coordinator = MasterCoordinator(config=config)
    master_coordinator.start()
    schema_reader = KafkaSchemaReader(config=config, key_formatter=KeyFormatter(), master_coordinator=master_coordinator)
    schema_reader.start()

    with closing(master_coordinator), closing(schema_reader):
        _wait_until_reader_is_ready_and_master(master_coordinator, schema_reader)

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
        msg = future.get()

        schema_reader.offset_watcher.wait_for_offset(msg.offset, timeout=5)

        schemas = schema_reader.get_schemas(subject=subject, include_deleted=True)
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
            "schema": json_encode(TRUE_SCHEMA.schema),
        }
        future = producer.send(
            topic_name,
            key=json_encode(key, binary=True),
            value=json_encode(value, binary=True),
        )
        msg = future.get()

        assert schema_reader.offset_watcher.wait_for_offset(msg.offset, timeout=5) is True
        assert schema_reader.global_schema_id == test_global_schema_id

        schemas = schema_reader.get_schemas(subject=subject, include_deleted=True)
        assert len(schemas) == 2, "Deleted schemas must have been registered"


def test_regression_config_for_inexisting_object_should_not_throw(
    kafka_servers: KafkaServers,
    producer: KafkaProducer,
) -> None:
    test_name = "test_regression_config_for_inexisting_object_should_not_throw"
    subject = create_subject_name_factory(test_name)()
    group_id = create_group_name_factory(test_name)()

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "group_id": group_id,
        }
    )
    master_coordinator = MasterCoordinator(config=config)
    master_coordinator.start()
    schema_reader = KafkaSchemaReader(config=config, key_formatter=KeyFormatter(), master_coordinator=master_coordinator)
    schema_reader.start()

    with closing(master_coordinator), closing(schema_reader):
        _wait_until_reader_is_ready_and_master(master_coordinator, schema_reader)

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
        msg = future.get()

        assert schema_reader.offset_watcher.wait_for_offset(msg.offset, timeout=5) is True
        assert subject in schema_reader.subjects, "The above message should be handled gracefully"


@dataclass
class DetectKeyFormatCase(BaseTestCase):
    raw_msgs: List[Tuple[bytes, bytes]]
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
def test_key_format_detection(
    testcase: DetectKeyFormatCase,
    kafka_servers: KafkaServers,
    producer: KafkaProducer,
    admin_client: KafkaAdminClient,
) -> None:
    group_id = create_group_name_factory(testcase.test_name)()
    test_topic = new_topic(admin_client)

    for message in testcase.raw_msgs:
        future = producer.send(
            test_topic,
            key=message[0],
            value=message[1],
        )
        future.get()
    producer.flush()

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "group_id": group_id,
            "topic_name": test_topic,
        }
    )
    master_coordinator = MasterCoordinator(config=config)
    master_coordinator.start()
    key_formatter = KeyFormatter()
    schema_reader = KafkaSchemaReader(config=config, key_formatter=key_formatter, master_coordinator=master_coordinator)
    schema_reader.start()

    with closing(master_coordinator), closing(schema_reader):
        _wait_until_reader_is_ready_and_master(master_coordinator, schema_reader)

    assert key_formatter.get_keymode() == testcase.expected
