"""
karapace - schema backup

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from .backend import BaseBackupReader, BaseBackupWriter, BaseItemsBackupReader
from .consumer import PollTimeout
from .encoders import encode_key, encode_value
from .errors import BackupError, PartitionCountError, StaleConsumerError
from .v1 import SchemaBackupV1Reader
from .v2 import AnonymizeAvroWriter, SchemaBackupV2Reader, SchemaBackupV2Writer
from enum import Enum
from functools import partial
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import TopicAlreadyExistsError
from kafka.structs import PartitionMetadata, TopicPartition
from karapace import constants
from karapace.config import Config
from karapace.key_format import KeyFormatter
from karapace.schema_reader import new_schema_topic_from_config
from karapace.utils import assert_never, KarapaceKafkaClient
from pathlib import Path
from tempfile import mkstemp
from tenacity import retry, RetryCallState, stop_after_delay, wait_fixed
from typing import AbstractSet, Callable, Collection, Final, Generator, IO, Literal, TextIO

import contextlib
import logging
import os
import sys

LOG = logging.getLogger(__name__)


class BackupVersion(Enum):
    ANONYMIZE_AVRO = -1
    V1 = 1
    V2 = 2

    @property
    def marker(self) -> str:
        if self is BackupVersion.V2 or self is BackupVersion.ANONYMIZE_AVRO:
            return "/V2\n"
        raise AttributeError(f"{self} has no marker")

    @classmethod
    def by_marker(cls, marker: str) -> BackupVersion:
        try:
            return {BackupVersion.V2.marker: BackupVersion.V2}[marker]
        except KeyError:
            # pylint: disable=raise-missing-from
            raise ValueError("No BackupVersion matches the given marker")

    @property
    def reader(self) -> type[BaseBackupReader]:
        if self is BackupVersion.V2 or self is BackupVersion.ANONYMIZE_AVRO:
            return SchemaBackupV2Reader
        if self is BackupVersion.V1:
            return SchemaBackupV1Reader
        assert_never(self)

    @property
    def writer(self) -> type[BaseBackupWriter]:
        if self is BackupVersion.V2:
            return SchemaBackupV2Writer
        if self is BackupVersion.ANONYMIZE_AVRO:
            return AnonymizeAvroWriter
        if self is BackupVersion.V1:
            raise AttributeError("Cannot produce backups for V1")
        assert_never(self)


def __before_sleep(description: str) -> Callable[[RetryCallState], None]:
    """Returns a function to print a user-friendly message before going to sleep in retries.

    :param description: of the action, should compose well with _failed_ and _returned_ as next words.
    :returns: a function that can be used in ``tenacity.retry``'s ``before_sleep`` argument for printing a user-friendly
        message that explains which action failed, that a retry is going to happen, and how to abort if desired.
    """

    def before_sleep(it: RetryCallState) -> None:
        outcome = it.outcome
        if outcome is None:
            result = "did not complete yet"
        elif outcome.failed:
            result = f"failed ({outcome.exception()})"
        else:
            result = f"returned {outcome.result()!r}"
        print(f"{description} {result}, retrying... (Ctrl+C to abort)", file=sys.stderr)

    return before_sleep


def __check_partition_count(topic: str, supplier: Callable[[str], AbstractSet[PartitionMetadata]]) -> None:
    """Checks that the given topic has exactly one partition.

    :param topic: to check.
    :param supplier: of topic partition metadata.
    :raises PartitionCountError: if the topic does not have exactly one partition.
    """
    partition_count = len(supplier(topic))
    if partition_count != 1:
        raise PartitionCountError(
            f"Topic {topic!r} has {partition_count} partitions, but only topics with exactly 1 partition can be backed "
            "up. The schemas topic MUST have exactly 1 partition to ensure perfect ordering of schema updates."
        )


@contextlib.contextmanager
def _admin(config: Config) -> KafkaAdminClient:
    """Creates an automatically closing Kafka admin client.

    :param config: for the client.
    :raises Exception: if client creation fails, concrete exception types are unknown, see Kafka implementation.
    """

    @retry(
        before_sleep=__before_sleep("Kafka Admin client creation"),
        reraise=True,
        stop=stop_after_delay(60),  # seconds
        wait=wait_fixed(1),  # seconds
    )
    def __admin() -> KafkaAdminClient:
        return KafkaAdminClient(
            api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
            bootstrap_servers=config["bootstrap_uri"],
            client_id=config["client_id"],
            security_protocol=config["security_protocol"],
            ssl_cafile=config["ssl_cafile"],
            ssl_certfile=config["ssl_certfile"],
            ssl_keyfile=config["ssl_keyfile"],
            kafka_client=KarapaceKafkaClient,
        )

    admin = __admin()
    try:
        yield admin
    finally:
        admin.close()


@retry(
    before_sleep=__before_sleep("Schemas topic creation"),
    reraise=True,
    stop=stop_after_delay(60),  # seconds
    wait=wait_fixed(1),  # seconds
)
def _maybe_create_topic(config: Config, name: str | None = None) -> bool | None:
    """Creates the topic if the given name and the one in the config are the same.

    :param config: for the admin client.
    :param name: of the topic to create.
    :returns: ``True`` if the topic was created, ``False`` if it already exists, and ``None`` if the given name does not
        match the name of the schema topic in the config, in which case nothing has been done.
    :raises Exception: if topic creation fails, concrete exception types are unknown, see Kafka implementation.
    """
    topic = new_schema_topic_from_config(config)

    if name is not None and topic.name != name:
        LOG.warning(
            "Not creating topic, because the name %r from the config and the name %r from the CLI differ.",
            topic.name,
            name,
        )
        return None

    with _admin(config) as admin:
        try:
            admin.create_topics([topic], timeout_ms=constants.TOPIC_CREATION_TIMEOUT_MS)
            LOG.info(
                "Created topic %r (partition count: %s, replication factor: %s, config: %s)",
                topic.name,
                topic.num_partitions,
                topic.replication_factor,
                topic.topic_configs,
            )
            return True
        except TopicAlreadyExistsError:
            LOG.debug("Topic %r already exists", topic.name)
            return False


@contextlib.contextmanager
def _consumer(config: Config, topic: str) -> KafkaConsumer:
    """Creates an automatically closing Kafka consumer client.

    :param config: for the client.
    :param topic: to consume from.
    :raises PartitionCountError: if the topic does not have exactly one partition.
    :raises Exception: if client creation fails, concrete exception types are unknown, see Kafka implementation.
    """
    consumer = KafkaConsumer(
        topic,
        enable_auto_commit=False,
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        auto_offset_reset="earliest",
        metadata_max_age_ms=config["metadata_max_age_ms"],
        kafka_client=KarapaceKafkaClient,
    )
    try:
        __check_partition_count(topic, consumer.partitions_for_topic)
        yield consumer
    finally:
        consumer.close()


@contextlib.contextmanager
def _producer(config: Config, topic: str) -> KafkaProducer:
    """Creates an automatically closing Kafka producer client.

    :param config: for the client.
    :param topic: to produce to.
    :raises PartitionCountError: if the topic does not have exactly one partition.
    :raises Exception: if client creation fails, concrete exception types are unknown, see Kafka implementation.
    """
    producer = KafkaProducer(
        bootstrap_servers=config["bootstrap_uri"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        kafka_client=KarapaceKafkaClient,
    )
    try:
        __check_partition_count(topic, producer.partitions_for)
        yield producer
    finally:
        producer.close()


@contextlib.contextmanager
def _writer(
    file: str | Path,
    *,
    overwrite: bool | None = None,
) -> Generator[TextIO, None, None]:
    """Opens the given file for writing.

    This function uses a safe temporary file to collect all written data, followed by a final rename. On most systems
    the final rename is atomic under most conditions, but there are no guarantees. The temporary file is always created
    next to the given file, to ensure that the temporary file is on the same physical volume as the target file, and
    avoid issues that might arise when moving data between physical volumes.

    :param file: to open for writing, both the empty string and the conventional single dash ``-`` will yield
        ``sys.stdout`` instead of actually creating a file for writing.
    :param overwrite: may be set to ``True`` to overwrite an existing file at the same location.
    :raises FileExistsError: if ``overwrite`` is not ``True`` and the file already exists, or if the parent directory of
        the file is not a directory.
    :raises OSError: if writing fails or if the file already exists and is not actually a file.
    """
    if file in ("", "-"):
        yield sys.stdout
    else:
        if not isinstance(file, Path):
            file = Path(file)
        dst = file.absolute()

        def check_dst() -> None:
            if dst.exists():
                if overwrite is not True:
                    raise FileExistsError(f"--location already exists at {dst}, use --overwrite to replace the file.")
                if not dst.is_file():
                    raise FileExistsError(
                        f"--location already exists at {dst}, but is not a file and thus cannot be overwritten."
                    )

        check_dst()
        dst.parent.mkdir(parents=True, exist_ok=True)
        fd, path = mkstemp(dir=dst.parent, prefix=dst.name)
        src = Path(path)
        try:
            fp = open(fd, "w", encoding="utf8")
            try:
                yield fp
                fp.flush()
                os.fsync(fd)
            finally:
                fp.close()
            check_dst()
            # This might still fail despite all checks, because there is a time window in which other processes can make
            # changes to the filesystem while our program is advancing. However, we have done the best we can.
            src.replace(dst)
        finally:
            try:
                src.unlink()
            except FileNotFoundError:
                pass


def _discover_reader_backend(fp: IO[str]) -> type[BaseBackupReader[IO[str]]]:
    try:
        version = BackupVersion.by_marker(fp.read(4))
    except ValueError:
        return SchemaBackupV1Reader
    finally:
        # Seek back to start.
        fp.seek(0)
    # Consume until linefeed.
    fp.readline()
    return version.reader


class SchemaBackup:
    def __init__(
        self,
        config: Config,
        backup_path: str,
        topic_option: str | None = None,
    ) -> None:
        self.config: Final = config
        self.backup_location: Final = backup_path
        self.topic_name: Final[str] = topic_option or self.config["topic_name"]
        self.timeout_ms: Final = 1000
        self.timeout_kafka_producer: Final = 5
        self.producer_exception: Exception | None = None

        # Schema key formatter
        self.key_formatter: Final = (
            KeyFormatter()
            if self.topic_name == constants.DEFAULT_SCHEMA_TOPIC or self.config.get("force_key_correction", False)
            else None
        )

    def restore_backup(self) -> None:
        if not os.path.exists(self.backup_location):
            raise BackupError("Backup location doesn't exist")

        _maybe_create_topic(self.config, self.topic_name)

        with _producer(self.config, self.topic_name) as producer:
            LOG.info("Starting backup restore for topic: %r", self.topic_name)

            with open(self.backup_location, encoding="utf8") as fp:
                backend_type = _discover_reader_backend(fp)
                backend = (
                    backend_type(
                        key_encoder=partial(encode_key, key_formatter=self.key_formatter),
                        value_encoder=encode_value,
                    )
                    if issubclass(backend_type, BaseItemsBackupReader)
                    else backend_type()
                )
                LOG.info("Identified backup backend: %s", backend.__class__.__name__)
                for instruction in backend.read(
                    topic_name=self.topic_name,
                    buffer=fp,
                ):
                    LOG.debug(
                        "Sending kafka msg key: %r, value: %r",
                        instruction.key,
                        instruction.value,
                    )
                    producer.send(
                        instruction.topic_name,
                        key=instruction.key,
                        value=instruction.value,
                        partition=instruction.partition,
                    ).add_errback(self.producer_error_callback)
            producer.flush(timeout=self.timeout_kafka_producer)
            if self.producer_exception is not None:
                raise BackupError("Error while producing restored messages") from self.producer_exception

    def producer_error_callback(self, exception: Exception) -> None:
        self.producer_exception = exception

    def create(
        self,
        version: Literal[BackupVersion.V2, BackupVersion.ANONYMIZE_AVRO],
        *,
        poll_timeout: PollTimeout | None = None,
        overwrite: bool | None = None,
    ) -> None:
        """Creates a backup of the configured topic.

        :param version: Specifies which format version to use for the backup.
        :param poll_timeout: specifies the maximum time to wait for receiving records, if not records are received
            within that time and the target offset has not been reached an exception is raised. Defaults to one minute.
        :param overwrite: the output file if it exists.
        :raises Exception: if consumption fails, concrete exception types are unknown, see Kafka implementation.
        :raises FileExistsError: if ``overwrite`` is not ``True`` and the file already exists, or if the parent
            directory of the file is not a directory.
        :raises OSError: if writing fails or if the file already exists and is not actually a file.
        :raises StaleConsumerError: if no records are received within the given ``poll_timeout`` and the target offset
            has not been reached yet.
        """
        backend = version.writer()

        if poll_timeout is None:
            poll_timeout = PollTimeout.default()
        poll_timeout_ms = poll_timeout.to_milliseconds()
        topic = self.topic_name
        with _writer(self.backup_location, overwrite=overwrite) as fp, _consumer(self.config, topic) as consumer:
            (partition,) = consumer.partitions_for_topic(self.topic_name)
            topic_partition = TopicPartition(self.topic_name, partition)
            start_offset: int = consumer.beginning_offsets([topic_partition])[topic_partition]
            end_offset: int = consumer.end_offsets([topic_partition])[topic_partition]
            last_offset = start_offset
            record_count = 0

            fp.write(version.marker)
            if start_offset < end_offset:  # non-empty topic
                end_offset -= 1  # high watermark to actual end offset
                print(
                    (
                        f"Started backup in format {version.name} of "
                        f"{topic}:{partition} "
                        f"(offset {start_offset:,} to {end_offset:,}) ..."
                    ),
                    file=sys.stderr,
                )
                while True:
                    records: Collection[ConsumerRecord] = consumer.poll(poll_timeout_ms).get(topic_partition, [])
                    if len(records) == 0:
                        raise StaleConsumerError(topic_partition, start_offset, end_offset, last_offset, poll_timeout)
                    for record in records:
                        backend.store_record(fp, record)
                        record_count += 1

                    last_offset = record.offset  # pylint: disable=undefined-loop-variable
                    if last_offset >= end_offset:
                        break

            destination = "stdout" if fp is sys.stdout else self.backup_location
            print(
                (
                    f"Finished backup in format {version.name} of {topic}:{partition} "
                    f"to {destination!r} (backed up {record_count:,} records)."
                ),
                file=sys.stderr,
            )
