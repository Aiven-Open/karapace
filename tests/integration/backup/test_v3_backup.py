"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from aiokafka.errors import UnknownTopicOrPartitionError
from collections.abc import Iterator
from confluent_kafka import Message, TopicPartition
from confluent_kafka.admin import NewTopic
from dataclasses import fields
from karapace.backup import api
from karapace.backup.api import _consume_records, BackupVersion, TopicName
from karapace.backup.backends.v3.errors import InconsistentOffset
from karapace.backup.backends.v3.readers import read_metadata
from karapace.backup.backends.v3.schema import Metadata
from karapace.backup.errors import BackupDataRestorationError, EmptyPartition
from karapace.backup.poll_timeout import PollTimeout
from karapace.backup.topic_configurations import ConfigSource, get_topic_configurations
from karapace.config import Config, set_config_defaults
from karapace.kafka.admin import KafkaAdminClient
from karapace.kafka.consumer import KafkaConsumer
from karapace.kafka.producer import KafkaProducer
from karapace.kafka.types import Timestamp
from karapace.kafka_utils import kafka_consumer_from_config, kafka_producer_from_config
from karapace.version import __version__
from pathlib import Path
from tempfile import mkdtemp
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from typing import NoReturn
from unittest.mock import patch

import datetime
import json
import logging
import os
import pytest
import shutil
import subprocess
import textwrap
import time

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function", name="karapace_config")
def config_fixture(
    kafka_servers: KafkaServers,
    registry_cluster: RegistryDescription,
) -> Config:
    return set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": registry_cluster.schemas_topic,
        }
    )


@pytest.fixture(scope="function", name="config_file")
def config_file_fixture(
    kafka_servers: KafkaServers,
    registry_cluster: RegistryDescription,
) -> Iterator[Path]:
    str_path = mkdtemp()
    directory_path = Path(str_path)
    file_path = directory_path / "config.json"
    try:
        file_path.write_text(
            json.dumps(
                {
                    "bootstrap_uri": kafka_servers.bootstrap_servers,
                    "topic_name": registry_cluster.schemas_topic,
                },
                indent=2,
            )
        )
        yield file_path
    finally:
        shutil.rmtree(directory_path)


@pytest.fixture(scope="function", name="producer")
def producer_fixture(karapace_config: Config) -> Iterator[KafkaProducer]:
    with kafka_producer_from_config(karapace_config) as producer:
        yield producer


def _raise(exception: Exception) -> NoReturn:
    raise exception


def _delete_topic(admin_client: KafkaAdminClient, topic_name: str) -> None:
    try:
        admin_client.delete_topic(topic_name)
    except UnknownTopicOrPartitionError:
        logger.info("No previously existing topic.")
    else:
        logger.info("Deleted topic from previous run.")

    start_time = time.monotonic()
    while True:
        topics = admin_client.cluster_metadata().get("topics", {})
        if topic_name not in topics:
            break
        if time.monotonic() - start_time > 60:
            raise TimeoutError(f"Topic {topic_name} still in cluster metadata topics {topics}")


def test_roundtrip_from_kafka_state(
    tmp_path: Path,
    new_topic: NewTopic,
    producer: KafkaProducer,
    config_file: Path,
    admin_client: KafkaAdminClient,
    karapace_config: Config,
) -> None:
    # Configure topic.
    admin_client.update_topic_config(new_topic.topic, {"max.message.bytes": "999"})

    # Populate topic.
    first_record_fut = producer.send(
        new_topic.topic,
        key=b"bar",
        value=b"foo",
        partition=0,
    )
    second_record_fut = producer.send(
        new_topic.topic,
        key=b"foo",
        value=b"bar",
        partition=0,
        headers=[
            ("some-header", b"some header value"),
            ("other-header", b"some other header value"),
        ],
    )
    producer.flush()

    first_message_timestamp = first_record_fut.result(timeout=5).timestamp()[1]
    second_message_timestamp = second_record_fut.result(timeout=5).timestamp()[1]

    topic_config = get_topic_configurations(admin_client, new_topic.topic, {ConfigSource.DYNAMIC_TOPIC_CONFIG})

    # Execute backup creation.
    backup_location = tmp_path / "backup"
    subprocess.run(
        [
            "karapace_schema_backup",
            "get",
            "--use-format-v3",
            "--verbose",
            "--config",
            str(config_file),
            "--topic",
            new_topic.topic,
            "--replication-factor=1",
            "--location",
            str(backup_location),
        ],
        capture_output=True,
        check=True,
    )

    # Verify exactly the expected file structure in the target path, and no residues
    # from temporary files.
    (backup_directory,) = tmp_path.iterdir()
    assert backup_directory.name == "backup"
    assert sorted(path.name for path in backup_directory.iterdir()) == [
        f"{new_topic.topic}.metadata",
        f"{new_topic.topic}:0.data",
    ]
    (metadata_path,) = backup_directory.glob("*.metadata")
    assert metadata_path.exists()

    # Delete the source topic.
    _delete_topic(admin_client, new_topic.topic)

    # todo: assert new topic uuid != old topic uuid?
    # Execute backup restoration.
    subprocess.run(
        [
            "karapace_schema_backup",
            "restore",
            "--verbose",
            "--config",
            str(config_file),
            "--topic",
            new_topic.topic,
            "--location",
            str(backup_directory),
        ],
        capture_output=True,
        check=True,
    )

    # Verify configuration is identical.
    assert topic_config == get_topic_configurations(admin_client, new_topic.topic, {ConfigSource.DYNAMIC_TOPIC_CONFIG})

    # Verify records of restored topic.
    with kafka_consumer_from_config(karapace_config, new_topic.topic) as consumer:
        (partition,) = consumer.partitions_for_topic(new_topic.topic)
        first_record, second_record = _consume_records(
            consumer=consumer,
            topic_partition=TopicPartition(new_topic.topic, partition),
            poll_timeout=PollTimeout.default(),
        )

    # First record.
    assert isinstance(first_record, Message)
    assert first_record.topic() == new_topic.topic
    assert first_record.partition() == partition
    # Note: This might be unreliable due to not using idempotent producer, i.e. we have
    # no guarantee against duplicates currently.
    assert first_record.offset() == 0
    assert first_record.timestamp()[1] == first_message_timestamp
    assert first_record.timestamp()[0] == Timestamp.CREATE_TIME
    assert first_record.key() == b"bar"
    assert first_record.value() == b"foo"
    assert first_record.headers() is None

    # Second record.
    assert isinstance(second_record, Message)
    assert second_record.topic() == new_topic.topic
    assert second_record.partition() == partition
    assert second_record.offset() == 1
    assert second_record.timestamp()[1] == second_message_timestamp
    assert second_record.timestamp()[0] == Timestamp.CREATE_TIME
    assert second_record.key() == b"foo"
    assert second_record.value() == b"bar"
    assert second_record.headers() == [
        ("some-header", b"some header value"),
        ("other-header", b"some other header value"),
    ]


def test_roundtrip_empty_topic(
    tmp_path: Path,
    new_topic: NewTopic,
    config_file: Path,
    admin_client: KafkaAdminClient,
    karapace_config: Config,
) -> None:
    # Configure topic.
    admin_client.update_topic_config(new_topic.topic, {"max.message.bytes": "987"})

    # Execute backup creation.
    backup_location = tmp_path / "backup"
    subprocess.run(
        [
            "karapace_schema_backup",
            "get",
            "--use-format-v3",
            "--verbose",
            "--config",
            str(config_file),
            "--topic",
            new_topic.topic,
            "--replication-factor=1",
            "--location",
            str(backup_location),
        ],
        capture_output=True,
        check=True,
    )

    # Verify exactly the expected file structure in the target path, and no residues
    # from temporary files.
    (backup_directory,) = tmp_path.iterdir()
    assert backup_directory.name == "backup"
    (metadata_path,) = backup_directory.iterdir()

    # Delete the source topic.
    _delete_topic(admin_client, new_topic.topic)

    # Execute backup restoration.
    subprocess.run(
        [
            "karapace_schema_backup",
            "restore",
            "--verbose",
            "--config",
            str(config_file),
            "--topic",
            new_topic.topic,
            "--location",
            str(metadata_path),
        ],
        capture_output=True,
        check=True,
    )

    # Verify configuration.
    assert get_topic_configurations(admin_client, new_topic.topic, {ConfigSource.DYNAMIC_TOPIC_CONFIG}) == {
        "min.insync.replicas": "1",
        "cleanup.policy": "delete",
        "retention.ms": "604800000",
        "max.message.bytes": "987",
        "retention.bytes": "-1",
    }

    # Verify the restored partition is empty.
    consumer_ctx = kafka_consumer_from_config(karapace_config, new_topic.topic)
    with consumer_ctx as consumer, pytest.raises(EmptyPartition):
        (partition,) = consumer.partitions_for_topic(new_topic.topic)
        () = _consume_records(
            consumer=consumer,
            topic_partition=TopicPartition(new_topic.topic, partition),
            poll_timeout=PollTimeout.default(),
        )


def test_errors_when_omitting_replication_factor(config_file: Path) -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        subprocess.run(
            [
                "karapace_schema_backup",
                "get",
                "--verbose",
                "--use-format-v3",
                f"--config={config_file!s}",
                "--topic=foo-bar",
                "--location=foo-bar",
            ],
            capture_output=True,
            check=True,
        )
    assert "the following arguments are required: --replication-factor" in exc_info.value.stderr.decode()


def test_exits_with_return_code_3_for_data_restoration_error(
    config_file: Path,
    admin_client: KafkaAdminClient,
) -> None:
    topic_name = "a-topic"
    location = (
        Path(__file__).parent.parent.resolve()
        / "test_data"
        / "backup_v3_corrupt_last_record_bit_flipped_no_checkpoints"
        / f"{topic_name}.metadata"
    )

    # Make sure topic doesn't exist beforehand.
    _delete_topic(admin_client, topic_name)

    admin_client.new_topic(topic_name)
    with pytest.raises(subprocess.CalledProcessError) as er:
        subprocess.run(
            [
                "karapace_schema_backup",
                "restore",
                "--verbose",
                "--config",
                str(config_file),
                "--topic",
                topic_name,
                "--location",
                str(location),
                "--skip-topic-creation",
            ],
            capture_output=True,
            check=True,
        )
    assert er.value.returncode == 3


def test_roundtrip_from_file(
    tmp_path: Path,
    config_file: Path,
    admin_client: KafkaAdminClient,
) -> None:
    topic_name = "0cdc85dc"
    backup_directory = Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic_name
    metadata_path = backup_directory / f"{topic_name}.metadata"
    with metadata_path.open("rb") as buffer:
        metadata = read_metadata(buffer)
    (data_file,) = metadata_path.parent.glob("*.data")

    # Make sure topic doesn't exist beforehand.
    _delete_topic(admin_client, topic_name)

    # Execute backup restoration.
    subprocess.run(
        [
            "karapace_schema_backup",
            "restore",
            "--verbose",
            "--config",
            str(config_file),
            "--topic",
            topic_name,
            "--location",
            str(metadata_path),
        ],
        capture_output=True,
        check=True,
    )

    # Execute backup creation.
    backup_start_time = datetime.datetime.now(datetime.timezone.utc)
    backup_location = tmp_path / "backup"
    subprocess.run(
        [
            "karapace_schema_backup",
            "get",
            "--verbose",
            "--use-format-v3",
            "--config",
            str(config_file),
            "--topic",
            topic_name,
            "--replication-factor=1",
            "--location",
            str(backup_location),
        ],
        capture_output=True,
        check=True,
    )
    backup_end_time = datetime.datetime.now(datetime.timezone.utc)

    # Verify exactly the expected file directory structure, no other files in target
    # path. This is important so that assert temporary files are properly cleaned up.
    (backup_directory,) = tmp_path.iterdir()
    assert backup_directory.name == "backup"
    assert sorted(path.name for path in backup_directory.iterdir()) == [
        f"{topic_name}.metadata",
        f"{topic_name}:0.data",
    ]

    # Parse metadata from file.
    (new_metadata_path,) = backup_directory.glob("*.metadata")
    with new_metadata_path.open("rb") as buffer:
        new_metadata = read_metadata(buffer)

    # Verify start and end timestamps are within expected range.
    assert backup_start_time < new_metadata.started_at
    assert new_metadata.started_at < new_metadata.finished_at
    assert new_metadata.finished_at < backup_end_time

    # Verify new version matches current version of Karapace.
    assert new_metadata.tool_version == __version__

    # Verify replication factor is correctly propagated.
    assert new_metadata.replication_factor == 1

    # Verify all fields other than timings and version match exactly.
    for field in fields(Metadata):
        if field.name in {"started_at", "finished_at", "tool_version"}:
            continue
        assert getattr(metadata, field.name) == getattr(new_metadata, field.name)

    # Verify data files are identical.
    (new_data_file,) = new_metadata_path.parent.glob("*.data")
    assert data_file.read_bytes() == new_data_file.read_bytes()


def test_roundtrip_from_file_skipping_topic_creation(
    tmp_path: Path,
    config_file: Path,
    admin_client: KafkaAdminClient,
) -> None:
    topic_name = "942700b6"
    backup_directory = Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic_name
    metadata_path = backup_directory / f"{topic_name}.metadata"
    with metadata_path.open("rb") as buffer:
        metadata = read_metadata(buffer)
    (data_file,) = metadata_path.parent.glob("*.data")

    # Create topic exactly as it was stored on backup file
    _delete_topic(admin_client, topic_name)
    admin_client.new_topic(topic_name)

    # Execute backup restoration.
    subprocess.run(
        [
            "karapace_schema_backup",
            "restore",
            "--verbose",
            "--config",
            str(config_file),
            "--topic",
            topic_name,
            "--location",
            str(metadata_path),
            "--skip-topic-creation",
        ],
        capture_output=True,
        check=True,
    )

    # Execute backup creation.
    backup_start_time = datetime.datetime.now(datetime.timezone.utc)
    backup_location = tmp_path / "backup"
    subprocess.run(
        [
            "karapace_schema_backup",
            "get",
            "--use-format-v3",
            "--verbose",
            "--config",
            str(config_file),
            "--topic",
            topic_name,
            "--replication-factor=1",
            "--location",
            str(backup_location),
        ],
        capture_output=True,
        check=True,
    )
    backup_end_time = datetime.datetime.now(datetime.timezone.utc)

    # Verify exactly the expected file directory structure, no other files in target
    # path. This is important so that assert temporary files are properly cleaned up.
    (backup_directory,) = tmp_path.iterdir()
    assert backup_directory.name == "backup"
    assert sorted(path.name for path in backup_directory.iterdir()) == [
        f"{topic_name}.metadata",
        f"{topic_name}:0.data",
    ]

    # Parse metadata from file.
    (new_metadata_path,) = backup_directory.glob("*.metadata")
    with new_metadata_path.open("rb") as buffer:
        new_metadata = read_metadata(buffer)

    # Verify start and end timestamps are within expected range.
    assert backup_start_time < new_metadata.started_at
    assert new_metadata.started_at < new_metadata.finished_at
    assert new_metadata.finished_at < backup_end_time

    # Verify new version matches current version of Karapace.
    assert new_metadata.tool_version == __version__

    # Verify replication factor is correctly propagated.
    assert new_metadata.replication_factor == 1

    # Verify all fields other than timings and version match exactly.
    for field in fields(Metadata):
        if field.name in {"started_at", "finished_at", "tool_version"}:
            continue
        assert getattr(metadata, field.name) == getattr(new_metadata, field.name)

    # Verify data files are identical.
    (new_data_file,) = new_metadata_path.parent.glob("*.data")
    assert data_file.read_bytes() == new_data_file.read_bytes()


def test_backup_restoration_fails_when_topic_does_not_exist_and_skip_creation_is_true(
    admin_client: KafkaAdminClient,
    kafka_servers: KafkaServers,
) -> None:
    topic_name = "83bdbd2a"
    backup_directory = Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic_name
    metadata_path = backup_directory / f"{topic_name}.metadata"

    # Make sure topic doesn't exist beforehand.
    _delete_topic(admin_client, topic_name)

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
        }
    )

    class LowTimeoutProducer:
        def __init__(self):
            self._producer = KafkaProducer(
                bootstrap_servers=config["bootstrap_uri"],
                security_protocol=config["security_protocol"],
                ssl_cafile=config["ssl_cafile"],
                ssl_certfile=config["ssl_certfile"],
                ssl_keyfile=config["ssl_keyfile"],
                sasl_mechanism=config["sasl_mechanism"],
                sasl_plain_username=config["sasl_plain_username"],
                sasl_plain_password=config["sasl_plain_password"],
                socket_timeout_ms=5000,
            )

        def __enter__(self):
            return self._producer

        def __exit__(self, exc_type, exc_value, exc_traceback):
            self._producer.flush()

    with patch("karapace.backup.api._producer") as p:
        p.return_value = LowTimeoutProducer()
        with pytest.raises(BackupDataRestorationError) as excinfo:
            api.restore_backup(
                config=config,
                backup_location=metadata_path,
                topic_name=TopicName(topic_name),
                skip_topic_creation=True,
            )
        excinfo.match("Error while producing restored messages")


def test_backup_restoration_fails_when_producer_send_fails_on_unknown_topic_or_partition(
    admin_client: KafkaAdminClient,
    kafka_servers: KafkaServers,
) -> None:
    topic_name = "596ddf6b"
    backup_directory = Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic_name
    metadata_path = backup_directory / f"{topic_name}.metadata"

    # Make sure topic doesn't exist beforehand.
    _delete_topic(admin_client, topic_name)

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
        }
    )

    class FailToSendProducer(KafkaProducer):
        def send(self, *args, **kwargs):
            raise UnknownTopicOrPartitionError()

    class FailToSendProducerContext:
        def __init__(self):
            self._producer = FailToSendProducer(
                bootstrap_servers=config["bootstrap_uri"],
                security_protocol=config["security_protocol"],
                ssl_cafile=config["ssl_cafile"],
                ssl_certfile=config["ssl_certfile"],
                ssl_keyfile=config["ssl_keyfile"],
                sasl_mechanism=config["sasl_mechanism"],
                sasl_plain_username=config["sasl_plain_username"],
                sasl_plain_password=config["sasl_plain_password"],
            )

        def __enter__(self):
            return self._producer

        def __exit__(self, exc_type, exc_value, exc_traceback):
            self._producer.flush()

    with patch("karapace.backup.api._producer") as p:
        p.return_value = FailToSendProducerContext()
        with pytest.raises(BackupDataRestorationError):
            api.restore_backup(
                config=config,
                backup_location=metadata_path,
                topic_name=TopicName(topic_name),
            )


def test_backup_restoration_fails_when_producer_send_fails_on_buffer_error(
    admin_client: KafkaAdminClient,
    kafka_servers: KafkaServers,
) -> None:
    topic_name = "296ddf62"
    backup_directory = Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic_name
    metadata_path = backup_directory / f"{topic_name}.metadata"

    # Make sure topic doesn't exist beforehand.
    _delete_topic(admin_client, topic_name)

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
        }
    )

    class FailToSendProducer(KafkaProducer):
        def send(self, *args, **kwargs):
            raise BufferError()

        def poll(self, timeout: float) -> None:  # pylint: disable=unused-argument
            return

    class FailToSendProducerContext:
        def __init__(self):
            self._producer = FailToSendProducer(
                bootstrap_servers=config["bootstrap_uri"],
                security_protocol=config["security_protocol"],
                ssl_cafile=config["ssl_cafile"],
                ssl_certfile=config["ssl_certfile"],
                ssl_keyfile=config["ssl_keyfile"],
                sasl_mechanism=config["sasl_mechanism"],
                sasl_plain_username=config["sasl_plain_username"],
                sasl_plain_password=config["sasl_plain_password"],
            )

        def __enter__(self):
            return self._producer

        def __exit__(self, exc_type, exc_value, exc_traceback):
            self._producer.flush()

    with patch("karapace.backup.api._producer") as p:
        p.return_value = FailToSendProducerContext()
        with pytest.raises(BackupDataRestorationError, match="Kafka producer buffer is full"):
            api.restore_backup(
                config=config,
                backup_location=metadata_path,
                topic_name=TopicName(topic_name),
            )


def no_color_env() -> dict[str, str]:
    env = os.environ.copy()
    try:
        del env["FORCE_COLOR"]
    except KeyError:
        pass
    return {**env, "COLUMNS": "100"}


class TestInspect:
    def test_can_inspect_v3(self) -> None:
        topic = "0cdc85dc"
        metadata_path = (
            Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic / f"{topic}.metadata"
        )

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "inspect",
                "--verbose",
                "--location",
                str(metadata_path),
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr == b""
        assert json.loads(cp.stdout) == {
            "version": 3,
            "tool_name": "karapace",
            "tool_version": "3.5.0-31-g15440ce",
            "started_at": "2023-05-31T11:42:21.116000+00:00",
            "finished_at": "2023-05-31T11:42:21.762000+00:00",
            "topic_name": "0cdc85dc",
            "topic_id": None,
            "partition_count": 1,
            "record_count": 2,
            "replication_factor": 1,
            "topic_configurations": {
                "min.insync.replicas": "1",
                "cleanup.policy": "delete",
                "retention.ms": "604800000",
                "max.message.bytes": "999",
                "retention.bytes": "-1",
            },
            "checksum_algorithm": "xxhash3_64_be",
            "data_files": [
                {
                    "filename": "0cdc85dc:0.data",
                    "partition": 0,
                    "checksum_hex": "f414f504a8e49313",
                    "record_count": 2,
                    "start_offset": 0,
                    "end_offset": 1,
                },
            ],
        }

    def test_can_inspect_v3_with_future_checksum_algorithm(self) -> None:
        metadata_path = (
            Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_future_algorithm" / "a5f7a413.metadata"
        )

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "inspect",
                "--verbose",
                "--location",
                str(metadata_path),
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr.decode() == (
            "Warning! This file has an unknown checksum algorithm and cannot be restored with this version of \nKarapace.\n"
        )
        assert json.loads(cp.stdout) == {
            "version": 3,
            "tool_name": "karapace",
            "tool_version": "3.5.0-27-g9535c1d",
            "started_at": "2023-05-31T12:01:01.165000+00:00",
            "finished_at": "2023-05-31T12:01:01.165000+00:00",
            "topic_name": "a5f7a413",
            "topic_id": None,
            "partition_count": 1,
            "record_count": 2,
            "replication_factor": 2,
            "topic_configurations": {"cleanup.policy": "compact", "min.insync.replicas": "2"},
            "checksum_algorithm": "unknown",
            "data_files": [
                {
                    "filename": "a5f7a413:0.data",
                    "partition": 0,
                    "checksum_hex": "66343134663530346138653439333133",
                    "record_count": 2,
                    "start_offset": 0,
                    "end_offset": 1,
                },
            ],
        }

    def test_can_inspect_v2(self) -> None:
        backup_path = Path(__file__).parent.parent.resolve() / "test_data" / "test_restore_v2.log"

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "inspect",
                "--location",
                str(backup_path),
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr == b""
        assert json.loads(cp.stdout) == {"version": 2}

    def test_can_inspect_v1(self) -> None:
        backup_path = Path(__file__).parent.parent.resolve() / "test_data" / "test_restore_v1.log"

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "inspect",
                "--verbose",
                "--location",
                str(backup_path),
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr == b""
        assert json.loads(cp.stdout) == {"version": 1}


class TestVerify:
    def test_can_verify_file_integrity(self) -> None:
        topic = "0cdc85dc"
        metadata_path = (
            Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic / f"{topic}.metadata"
        )

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "verify",
                "--verbose",
                f"--location={metadata_path!s}",
                "--level=file",
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr == b""
        assert cp.stdout.decode() == textwrap.dedent(
            """\
            Integrity of 0cdc85dc:0.data is intact.
            ✅ Verified 1 data files in backup OK.
            """
        )

    def test_can_verify_record_integrity(self) -> None:
        topic = "0cdc85dc"
        metadata_path = (
            Path(__file__).parent.parent.resolve() / "test_data" / "backup_v3_single_partition" / topic / f"{topic}.metadata"
        )

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "verify",
                "--verbose",
                f"--location={metadata_path!s}",
                "--level=record",
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr == b""
        assert cp.stdout.decode() == textwrap.dedent(
            """\
            Integrity of 0cdc85dc:0.data is intact.
            ✅ Verified 1 data files in backup OK.
            """
        )

    def test_can_verify_file_integrity_from_large_topic(
        self,
        tmp_path: Path,
        new_topic: NewTopic,
        producer: KafkaProducer,
        config_file: Path,
    ) -> None:
        # Populate the test topic.
        for _ in range(100):
            producer.send(
                new_topic.topic,
                key=1000 * b"a",
                value=1000 * b"b",
                partition=0,
            )
        producer.flush()

        # Execute backup creation.
        backup_location = tmp_path / "backup"
        subprocess.run(
            [
                "karapace_schema_backup",
                "get",
                "--use-format-v3",
                "--verbose",
                f"--config={config_file!s}",
                f"--topic={new_topic.topic!s}",
                "--replication-factor=1",
                f"--location={backup_location!s}",
            ],
            capture_output=True,
            check=True,
        )
        metadata_path = backup_location / f"{new_topic.topic}.metadata"

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "verify",
                "--verbose",
                f"--location={metadata_path!s}",
                "--level=file",
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr == b""
        assert cp.stdout.decode() == textwrap.dedent(
            f"""\
            Integrity of {new_topic.topic}:0.data is intact.
            ✅ Verified 1 data files in backup OK.
            """
        )

    def test_can_verify_record_integrity_from_large_topic(
        self,
        tmp_path: Path,
        new_topic: NewTopic,
        producer: KafkaProducer,
        config_file: Path,
    ) -> None:
        # Populate the test topic.
        for _ in range(100):
            producer.send(
                new_topic.topic,
                key=1000 * b"a",
                value=1000 * b"b",
                partition=0,
            )
        producer.flush()

        # Execute backup creation.
        backup_location = tmp_path / "backup"
        subprocess.run(
            [
                "karapace_schema_backup",
                "get",
                "--use-format-v3",
                "--verbose",
                f"--config={config_file!s}",
                f"--topic={new_topic.topic}",
                "--replication-factor=1",
                f"--location={backup_location!s}",
            ],
            capture_output=True,
            check=True,
        )
        metadata_path = backup_location / f"{new_topic.topic}.metadata"

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "verify",
                "--verbose",
                f"--location={metadata_path}",
                "--level=record",
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 0
        assert cp.stderr == b""
        assert cp.stdout.decode() == textwrap.dedent(
            f"""\
            Integrity of {new_topic.topic}:0.data is intact.
            ✅ Verified 1 data files in backup OK.
            """
        )

    def test_can_refute_file_integrity(self) -> None:
        metadata_path = (
            Path(__file__).parent.parent.resolve()
            / "test_data"
            / "backup_v3_corrupt_last_record_bit_flipped_no_checkpoints"
            / "a-topic.metadata"
        )

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "verify",
                "--verbose",
                f"--location={metadata_path!s}",
                "--level=file",
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 1
        assert cp.stderr == b""
        assert cp.stdout.decode() == textwrap.dedent(
            """\
            Integrity of a-topic:123.data is not intact!
            💥 Failed to verify integrity of some data files.
            """
        )

    def test_can_refute_record_integrity(self) -> None:
        metadata_path = (
            Path(__file__).parent.parent.resolve()
            / "test_data"
            / "backup_v3_corrupt_last_record_bit_flipped_no_checkpoints"
            / "a-topic.metadata"
        )

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "verify",
                "--verbose",
                f"--location={metadata_path!s}",
                "--level=record",
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 1
        assert cp.stderr == b""
        assert cp.stdout.decode() == textwrap.dedent(
            """\
            Integrity of a-topic:123.data is not intact!
                InvalidChecksum: Found checksum mismatch after reading full data file.
            💥 Failed to verify integrity of some data files.
            """
        )

    @pytest.mark.parametrize(
        ("test_file", "error_message"),
        (
            (
                "test_restore_v1.log",
                "Only backups using format V3 can be verified, found V1.\n",
            ),
            (
                "test_restore_v2.log",
                "Only backups using format V3 can be verified, found V2.\n",
            ),
        ),
    )
    def test_gives_non_successful_exit_code_for_legacy_backup_format(
        self,
        test_file: str,
        error_message: str,
    ) -> None:
        backup_path = Path(__file__).parent.parent.resolve() / "test_data" / test_file

        cp = subprocess.run(
            [
                "karapace_schema_backup",
                "verify",
                "--verbose",
                f"--location={backup_path}",
                "--level=file",
            ],
            capture_output=True,
            check=False,
            env=no_color_env(),
        )

        assert cp.returncode == 1
        assert cp.stderr.decode() == error_message
        assert cp.stdout == b""


def test_backup_creation_succeeds_no_duplicate_offsets(
    kafka_servers: KafkaServers,
    producer: KafkaProducer,
    new_topic: NewTopic,
    tmp_path: Path,
) -> None:
    """This test was added to prevent a regression scenario of duplicate offsets.

    After introducing the confluent-kafka based consumer in the backups code,
    backing up large topics would result in duplicate offsets, which would fail
    the backup creation. The original code called `assign(TopicPartition(...))`
    after `subscribe`, resulting in an eventual reset of the consumer.

    The issue occurred only for large topics, as these had sufficient amount of
    data for the consumer to reset before the backup was completed.

    In this test a "large" topic is simulated by slowing down the consumer.
    """
    for i in range(1000):
        producer.send(
            new_topic.topic,
            key=f"message-key-{i}",
            value=f"message-value-{i}-" + 1000 * "X",
        )
    producer.flush()

    backup_location = tmp_path / "fails.log"
    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": new_topic.topic,
        }
    )

    class SlowConsumer(KafkaConsumer):
        def poll(self, *args, **kwargs):
            # Slow down the consumer so more time is given for `assign` to kick in
            # This simulates a backup of a _large_ topic
            time.sleep(0.01)
            return super().poll(*args, **kwargs)

    class ConsumerContext:
        def __init__(self):
            self._consumer = SlowConsumer(
                bootstrap_servers=kafka_servers.bootstrap_servers,
                topic=new_topic.topic,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )

        def __enter__(self):
            return self._consumer

        def __exit__(self, exc_type, exc_value, exc_traceback):
            self._consumer.close()

    with patch("karapace.backup.api._consumer") as consumer_patch:
        consumer_patch.return_value = ConsumerContext()
        try:
            api.create_backup(
                config=config,
                backup_location=backup_location,
                topic_name=api.normalize_topic_name(None, config),
                version=BackupVersion.V3,
                replication_factor=1,
            )
        except InconsistentOffset as exc:
            pytest.fail(f"Unexpected InconsistentOffset error {exc}")
