from contextlib import closing
from kafka import KafkaProducer
from karapace.config import set_config_defaults
from karapace.master_coordinator import MasterCoordinator
from karapace.schema_reader import KafkaSchemaReader
from karapace.utils import json_encode
from tests.schemas.json_schemas import TRUE_SCHEMA
from tests.utils import create_group_name_factory, create_subject_name_factory, KafkaServers, new_random_name

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
    schema_reader = KafkaSchemaReader(config=config, master_coordinator=master_coordinator)
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
