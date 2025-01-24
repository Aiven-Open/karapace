"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from aiokafka.errors import NoBrokersAvailable
from confluent_kafka.admin import NewTopic
from karapace.core.backup.api import BackupVersion, create_backup
from karapace.core.config import Config
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka_utils import kafka_producer_from_config
from pathlib import Path
from tests.integration.conftest import create_kafka_server
from tests.integration.utils.config import KafkaDescription
from tests.integration.utils.kafka_server import KafkaServers

import pytest

SESSION_TIMEOUT_MS = 65000
GROUP_MIN_SESSION_TIMEOUT_MS = 60000
GROUP_MAX_SESSION_TIMEOUT_MS = 70000


# use a dedicated kafka server with specific values for
# group.min.session.timeout.ms and group.max.session.timeout.ms
@pytest.fixture(scope="function", name="kafka_server_session_timeout")
def fixture_kafka_server(
    kafka_description: KafkaDescription,
    tmp_path_factory: pytest.TempPathFactory,
):
    # use custom data and log dir to avoid conflict with other kafka servers
    session_datadir = tmp_path_factory.mktemp("kafka_server_min_data")
    session_logdir = tmp_path_factory.mktemp("kafka_server_min_log")
    kafka_config_extra = {
        "group.min.session.timeout.ms": GROUP_MIN_SESSION_TIMEOUT_MS,
        "group.max.session.timeout.ms": GROUP_MAX_SESSION_TIMEOUT_MS,
    }
    yield from create_kafka_server(
        session_datadir,
        session_logdir,
        kafka_description,
        kafka_config_extra,
    )


def test_producer_with_custom_kafka_properties_does_not_fail(
    kafka_server_session_timeout: KafkaServers,
    new_topic: NewTopic,
    tmp_path: Path,
) -> None:
    """
    This test checks wether the custom properties are accepted by kafka.
    We know by the implementation of the consumer startup code that if
    `group.session.min.timeout.ms` > `session.timeout.ms` the consumer
    will raise an exception during the startup.
    This test ensures that the `session.timeout.ms` can be injected in
    the kafka config so that the exception isn't raised
    """
    config = Config()
    config.bootstrap_uri = kafka_server_session_timeout.bootstrap_servers[0]
    config.session_timeout_ms = SESSION_TIMEOUT_MS

    admin_client = KafkaAdminClient(bootstrap_servers=kafka_server_session_timeout.bootstrap_servers)
    admin_client.new_topic(new_topic.topic, num_partitions=1, replication_factor=1)

    with kafka_producer_from_config(config) as producer:
        producer.send(
            new_topic.topic,
            key=b"foo",
            value=b"bar",
            partition=0,
            headers=[
                ("some-header", b"some header value"),
                ("other-header", b"some other header value"),
            ],
            timestamp=1683474657,
        )
        producer.flush()

    # without performing the backup the exception isn't raised.
    create_backup(
        config=config,
        backup_location=tmp_path / "backup",
        topic_name=new_topic.topic,
        version=BackupVersion.V3,
        replication_factor=1,
    )


def test_producer_with_custom_kafka_properties_fail(
    kafka_server_session_timeout: KafkaServers,
    new_topic: NewTopic,
) -> None:
    """
    This test checks wether the custom properties are accepted by kafka.
    We know by the implementation of the consumer startup code that if
    `group.session.min.timeout.ms` > `session.timeout.ms` the consumer
    will raise an exception during the startup.
    This test ensures that the `session.timeout.ms` can be injected in
    the kafka config so that the exception isn't raised
    """
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_server_session_timeout.bootstrap_servers)
    admin_client.new_topic(new_topic.topic, num_partitions=1, replication_factor=1)

    config = Config()
    # TODO: This test is broken. Test has used localhost:9092 when this should use
    # the configured broker from kafka_server_session.
    # config.bootstrap_uri = kafka_server_session_timeout.bootstrap_servers[0]
    config.bootstrap_uri = "localhost:9092"

    with pytest.raises(NoBrokersAvailable):
        with kafka_producer_from_config(config) as producer:
            _ = producer
