"""
karapace - Test key correction

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.admin.config_resource import ConfigResource, ConfigResourceType
from kafka.consumer.fetcher import ConsumerRecord
from karapace.config import set_config_defaults
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.utils import json_encode
from tests.integration.utils.kafka_server import KafkaServers
from tests.schemas.json_schemas import FALSE_SCHEMA, TRUE_SCHEMA
from tests.utils import create_group_name_factory, new_random_name
from typing import Dict, List, Optional, Union

import asyncio
import json
import time


class SchemaMessage:
    def __init__(
        self,
        key: Dict[str, Union[int, str]],
        value: Optional[Dict[str, Union[int, str]]],
    ):
        # No key sorting for the key, allows to test with incorrect order of properties in the key.
        self.key = json_encode(key, binary=True, sort_keys=False, compact=True)
        if value:
            self.value = json_encode(value, binary=True, sort_keys=False, compact=True)
        else:
            self.value = None


def _wait_for_data_available_in_kafka(
    topic: str,
    kafka_servers: KafkaServers,
    expected_msgs: int = 0,
) -> List[ConsumerRecord]:
    group_id = create_group_name_factory("test-data-waiter")()
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        enable_auto_commit=False,
        api_version=(1, 0, 0),
        bootstrap_servers=kafka_servers.bootstrap_servers,
        auto_offset_reset="earliest",
    )
    max_time_ms = 30_000
    start_time = time.monotonic()

    try:
        while time.monotonic() - start_time < max_time_ms:
            raw_msgs = consumer.poll()
            if raw_msgs:
                all_msgs = []
                for _, msgs in raw_msgs.items():
                    all_msgs += msgs
                    if expected_msgs == 0 or len(all_msgs) == expected_msgs:
                        return all_msgs
            consumer.seek_to_beginning()
    finally:
        consumer.close()
    return []


def _produce_test_data(
    topic: str,
    kafka_servers: KafkaServers,
    admin_client: KafkaAdminClient,
    producer: KafkaProducer,
    test_records: List[SchemaMessage],
) -> None:
    admin_client.create_topics(
        new_topics=[
            NewTopic(name=topic, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": "compact"})
        ]
    )

    record_count = len(test_records)

    for record in test_records:
        future = producer.send(
            topic,
            key=record.key,
            value=record.value,
        )
        future.get()

    producer.flush()
    msgs = _wait_for_data_available_in_kafka(topic=topic, kafka_servers=kafka_servers)
    assert len(msgs) == record_count, "Data consumed is less than produced."


def _create_basic_test_data(
    topic: str,
    kafka_servers: KafkaServers,
    admin_client: KafkaAdminClient,
    producer: KafkaProducer,
) -> None:
    test_records = [
        SchemaMessage(
            key={
                "keytype": "SCHEMA",
                "subject": "test-schema-subject",
                "version": 1,
                "magic": 0,
            },
            value={
                "deleted": False,
                "id": 1,
                "subject": "test-schema-subject",
                "version": 1,
                "schema": json_encode(TRUE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={  # Schema with invalid key order
                "magic": 0,
                "subject": "test-schema-subject",
                "version": 2,
                "keytype": "SCHEMA",
            },
            value={
                "deleted": False,
                "id": 2,
                "subject": "test-schema-subject",
                "version": 2,
                "schema": json_encode(FALSE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={  # Send config change to test-schema-subject
                "magic": 0,
                "subject": "test-schema-subject",
                "keytype": "CONFIG",
            },
            value={"compatibilityLevel": "NONE"},
        ),
        SchemaMessage(
            key={  # Send NOOP, spec completeness
                "magic": 0,
                "subject": "test-noop-subject",
                "keytype": "NOOP",
            },
            value={"noop": "for spec completeness"},
        ),
        SchemaMessage(
            key={  # Send schema to create a subject for deletion
                "magic": 0,
                "subject": "test-delete-subject",
                "version": 1,
                "keytype": "SCHEMA",
            },
            value={
                "deleted": False,
                "id": 1,
                "subject": "test-delete-subject",
                "version": 1,
                "schema": json_encode(TRUE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={  # Send subject delete
                "magic": 0,
                "subject": "test-delete-subject",
                "keytype": "DELETE_SUBJECT",
            },
            value={"subject": "test-delete-subject", "version": 1},
        ),
    ]
    _produce_test_data(
        topic=topic, kafka_servers=kafka_servers, admin_client=admin_client, producer=producer, test_records=test_records
    )


async def test_empty_initial_schema_registry(kafka_servers: KafkaServers) -> None:
    """Test key correction with empty initial schemas topic"""
    topic_name = new_random_name("topic")
    test_name = "test_key_correction_with_schema_registry"
    group_id = create_group_name_factory(test_name)()

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "group_id": group_id,
            "topic_name": topic_name,
        }
    )
    schema_registry = KarapaceSchemaRegistry(config=config)

    def run_registry():
        nonlocal schema_registry
        schema_registry.start()

    try:
        run_registry()
        while schema_registry.schema_reader._kfc.key_correction_done is False:  # pylint: disable=protected-access
            await asyncio.sleep(0.5)

    finally:
        # Schema registry can be closed at this point.
        await schema_registry.close()

    msgs = _wait_for_data_available_in_kafka(topic=topic_name, kafka_servers=kafka_servers, expected_msgs=0)
    assert len(msgs) == 1
    key_correction_done_msg = msgs[0]
    assert key_correction_done_msg.key == b'{"keytype":"KEY_CORRECTION_DONE","magic":0}'
    assert key_correction_done_msg.value == b""


async def test_schema_registry(
    kafka_servers: KafkaServers,
    admin_client: KafkaAdminClient,
    producer: KafkaProducer,
) -> None:
    """Test the whole schema reader and schema registry integration when correcting keys."""
    topic_name = new_random_name("topic")
    _create_basic_test_data(topic=topic_name, kafka_servers=kafka_servers, admin_client=admin_client, producer=producer)
    test_name = "test_key_correction_with_schema_registry"
    group_id = create_group_name_factory(test_name)()

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "group_id": group_id,
            "topic_name": topic_name,
        }
    )
    schema_registry = KarapaceSchemaRegistry(config=config)

    def run_registry():
        nonlocal schema_registry
        schema_registry.start()

    try:
        run_registry()
        while schema_registry.schema_reader._kfc.key_correction_done is False:  # pylint: disable=protected-access
            await asyncio.sleep(0.5)

    finally:
        # Schema registry can be closed at this point.
        await schema_registry.close()

    # To run the compaction, some trickery is needed
    # See KIP-354 and documentation of related configuration entries below.
    config_resources = []
    config_resources.append(
        ConfigResource(
            resource_type=ConfigResourceType.BROKER,
            name=1,
            configs={
                "log.cleaner.min.cleanable.dirty.ratio": 0,
                "log.cleaner.max.compaction.lag.ms": 1000,
                "log.cleaner.delete.retention.ms": 1000,
                "log.roll.ms": 100,
            },
        )
    )
    admin_client.alter_configs(config_resources=config_resources)

    # Give some time for config change to stick
    await asyncio.sleep(5)

    # Send NOOP, to create a new segment
    key = {
        "keytype": "NOOP",
        "subject": "test-roll-segment-noop",
        "magic": 0,
    }
    value = {"noop": "payload-1"}
    future = producer.send(
        topic_name,
        key=json_encode(key, binary=True, sort_keys=False, compact=True),
        value=json_encode(value, binary=True, sort_keys=False, compact=True),
    )
    future.get()

    # In total there is sixteen messages sent to Kafka, five entries with invalid key that shall be compacted.
    # After compaction there should be twelve messages.
    # One unfixed correct entry (republished), five corrected entries, five tombstones
    # and key correction done message.
    msgs = _wait_for_data_available_in_kafka(topic=topic_name, kafka_servers=kafka_servers, expected_msgs=12)
    assert len(msgs) == 12
    tombstones = [msg for msg in msgs if msg.value is None]

    list(filter(lambda msg: msg.value is None, msgs))
    assert len(tombstones) == 5

    def correct_key_order(msg) -> bool:
        key_data = json.loads(msg.key)
        if key_data.get("keytype") == "SCHEMA":
            return list(key_data.keys()) == ["keytype", "subject", "version", "magic"]
        return list(key_data.keys()) == ["keytype", "subject", "magic"]

    entries = [msg for msg in msgs if msg.value is not None and correct_key_order(msg)]
    assert len(entries) == 6

    # Assert key correction message is last in the expected data.
    key_correction_done_msg = msgs[-1]
    assert key_correction_done_msg.key == b'{"keytype":"KEY_CORRECTION_DONE","magic":0}'
    assert key_correction_done_msg.value == b""


def _create_test_data_for_ordering_check(
    topic: str,
    kafka_servers: KafkaServers,
    admin_client: KafkaAdminClient,
    producer: KafkaProducer,
) -> None:
    test_records = [
        SchemaMessage(
            key={
                "subject": "test-ordering",
                "version": 1,
                "magic": 1,
                "keytype": "SCHEMA",
            },
            value={
                "deleted": False,
                "id": 1,
                "version": 1,
                "subject": "test-ordering",
                "schema": json_encode(TRUE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={
                "subject": "test-ordering",
                "version": 2,
                "magic": 1,
                "keytype": "SCHEMA",
            },
            value={
                "deleted": False,
                "id": 6,
                "version": 2,
                "subject": "test-ordering",
                "schema": json_encode(FALSE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={
                "keytype": "SCHEMA",
                "subject": "test-ordering",
                "version": 4,
                "magic": 1,
            },
            value={
                "deleted": False,
                "id": 6,
                "version": 4,
                "subject": "test-ordering",
                "schema": json_encode(FALSE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={
                "keytype": "SCHEMA",
                "subject": "test-ordering",
                "version": 2,
                "magic": 1,
            },
            value=None,
        ),
        SchemaMessage(
            key={
                "subject": "test-ordering",
                "version": 5,
                "magic": 1,
                "keytype": "SCHEMA",
            },
            value={
                "deleted": False,
                "id": 106,
                "version": 5,
                "subject": "test-ordering",
                "schema": json_encode(TRUE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={
                "keytype": "SCHEMA",
                "subject": "test-ordering",
                "version": 3,
                "magic": 1,
            },
            value=None,
        ),
    ]

    _produce_test_data(
        topic=topic, kafka_servers=kafka_servers, admin_client=admin_client, producer=producer, test_records=test_records
    )


async def test_schema_and_subject_data_ordering(
    kafka_servers: KafkaServers,
    admin_client: KafkaAdminClient,
    producer: KafkaProducer,
) -> None:
    topic_name = new_random_name("topic")
    _create_test_data_for_ordering_check(
        topic=topic_name, kafka_servers=kafka_servers, admin_client=admin_client, producer=producer
    )
    test_name = "test_key_correction_with_schema_registry"
    group_id = create_group_name_factory(test_name)()

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "group_id": group_id,
            "topic_name": topic_name,
        }
    )
    schema_registry = KarapaceSchemaRegistry(config=config)

    def run_registry():
        nonlocal schema_registry
        schema_registry.start()

    try:
        run_registry()
        while schema_registry.schema_reader._kfc.key_correction_done is False:  # pylint: disable=protected-access
            await asyncio.sleep(0.5)

    finally:
        # Schema registry can be closed at this point.
        await schema_registry.close()

    assert len(schema_registry.schema_reader.schemas) == 3
    # Schema order
    assert list(schema_registry.schema_reader.schemas.keys()) == [1, 6, 106]

    assert len(schema_registry.schema_reader.subjects["test-ordering"]["schemas"]) == 3
    # Version order
    assert list(schema_registry.schema_reader.subjects["test-ordering"]["schemas"].keys()) == [1, 4, 5]

    version = await schema_registry.subject_version_get("test-ordering", "latest")
    assert version["version"] == 5


def _create_test_data_for_ordering_edge_case_that_changes_insertion_order(
    topic: str,
    kafka_servers: KafkaServers,
    admin_client: KafkaAdminClient,
    producer: KafkaProducer,
) -> None:
    test_records = [
        SchemaMessage(
            key={
                "keytype": "SCHEMA",
                "subject": "test-ordering",
                "version": 1,
                "magic": 1,
            },
            value={
                "deleted": False,
                "id": 1,
                "version": 1,
                "subject": "test-ordering",
                "schema": json_encode(TRUE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={
                "keytype": "SCHEMA",
                "subject": "test-ordering",
                "version": 2,
                "magic": 1,
            },
            value={
                "deleted": False,
                "id": 2,
                "version": 2,
                "subject": "test-ordering",
                "schema": json_encode(FALSE_SCHEMA.schema),
            },
        ),
        SchemaMessage(
            key={
                "keytype": "SCHEMA",
                "subject": "test-ordering",
                "version": 1,
                "magic": 1,
            },
            value={
                "deleted": False,
                "id": 1,
                "version": 1,
                "subject": "test-ordering",
                "schema": json_encode(TRUE_SCHEMA.schema),
            },
        ),
    ]

    _produce_test_data(
        topic=topic, kafka_servers=kafka_servers, admin_client=admin_client, producer=producer, test_records=test_records
    )


async def test_schema_version_ordering_edge_case_that_changes_insertion_order(
    kafka_servers: KafkaServers,
    admin_client: KafkaAdminClient,
    producer: KafkaProducer,
) -> None:
    """Test edge case when schema version is inserted, new version inserted and reverted back to first version.

    This case causes the insertion order in the schema reader subject data dictionary to change from the original
    as the version reverting back to first version is first deleted and reinserted.
    """
    topic_name = new_random_name("topic")
    _create_test_data_for_ordering_edge_case_that_changes_insertion_order(
        topic=topic_name,
        kafka_servers=kafka_servers,
        admin_client=admin_client,
        producer=producer,
    )
    test_name = "test_key_correction_with_schema_registry"
    group_id = create_group_name_factory(test_name)()

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "admin_metadata_max_age": 2,
            "group_id": group_id,
            "topic_name": topic_name,
        }
    )
    schema_registry = KarapaceSchemaRegistry(config=config)

    def run_registry():
        nonlocal schema_registry
        schema_registry.start()

    try:
        run_registry()
        while schema_registry.schema_reader._kfc.key_correction_done is False:  # pylint: disable=protected-access
            await asyncio.sleep(0.5)

        assert len(schema_registry.schema_reader.schemas) == 2
        # Schema order
        assert list(schema_registry.schema_reader.schemas.keys()) == [1, 2]

        assert len(schema_registry.schema_reader.subjects["test-ordering"]["schemas"]) == 2
        # Version order
        assert list(schema_registry.schema_reader.subjects["test-ordering"]["schemas"].keys()) == [2, 1]

    finally:
        # Schema registry can be closed at this point.
        await schema_registry.close()

    version = await schema_registry.subject_version_get("test-ordering", "latest")
    assert version["version"] == 2
