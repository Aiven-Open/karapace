"""
karapace - schema backup

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from enum import Enum
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from karapace import constants
from karapace.anonymize_schemas import anonymize_avro
from karapace.config import Config, read_config
from karapace.key_format import KeyFormatter
from karapace.schema_reader import new_schema_topic_from_config
from karapace.typing import JsonData
from karapace.utils import json_decode, json_encode, KarapaceKafkaClient, Timeout
from pathlib import Path
from tempfile import mkstemp
from typing import IO, Optional, TextIO, Tuple, Union

import argparse
import base64
import contextlib
import logging
import os
import sys
import time

LOG = logging.getLogger(__name__)

# Schema topic has single partition.
# Use of this in `producer.send` disables the partitioner to calculate which partition the data is sent.
PARTITION_ZERO = 0
BACKUP_VERSION_2_MARKER = "/V2\n"


class BackupVersion(Enum):
    V1 = 1
    V2 = 2


class BackupError(Exception):
    """Backup Error"""


@contextlib.contextmanager
def _writer(file: Union[str, Path], *, overwrite: Optional[bool] = None) -> TextIO:
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
            nonlocal overwrite, dst
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


def _check_backup_file_version(fp: IO) -> BackupVersion:
    version_identifier = fp.read(4)
    if version_identifier == BACKUP_VERSION_2_MARKER:
        # Seek back to start, readline() to consume linefeed
        fp.seek(0)
        fp.readline()
        return BackupVersion.V2
    fp.seek(0)
    return BackupVersion.V1


class SchemaBackup:
    def __init__(self, config: Config, backup_path: str, topic_option: Optional[str] = None) -> None:
        self.config = config
        self.backup_location = backup_path
        self.topic_name = topic_option or self.config["topic_name"]
        self.consumer = None
        self.producer = None
        self.admin_client = None
        self.timeout_ms = 1000

        # Schema key formatter
        self.key_formatter = None
        if self.topic_name == constants.DEFAULT_SCHEMA_TOPIC or self.config.get("force_key_correction", False):
            self.key_formatter = KeyFormatter()

    def init_consumer(self) -> None:
        self.consumer = KafkaConsumer(
            self.topic_name,
            enable_auto_commit=False,
            bootstrap_servers=self.config["bootstrap_uri"],
            client_id=self.config["client_id"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            sasl_mechanism=self.config["sasl_mechanism"],
            sasl_plain_username=self.config["sasl_plain_username"],
            sasl_plain_password=self.config["sasl_plain_password"],
            auto_offset_reset="earliest",
            metadata_max_age_ms=self.config["metadata_max_age_ms"],
            kafka_client=KarapaceKafkaClient,
        )

    def init_producer(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=self.config["bootstrap_uri"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            sasl_mechanism=self.config["sasl_mechanism"],
            sasl_plain_username=self.config["sasl_plain_username"],
            sasl_plain_password=self.config["sasl_plain_password"],
            kafka_client=KarapaceKafkaClient,
        )

    def init_admin_client(self) -> None:
        start_time = time.monotonic()
        wait_time = constants.MINUTE
        while True:
            if time.monotonic() - start_time > wait_time:
                raise Timeout(f"Timeout ({wait_time}) on creating admin client")

            try:
                self.admin_client = KafkaAdminClient(
                    api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
                    bootstrap_servers=self.config["bootstrap_uri"],
                    client_id=self.config["client_id"],
                    security_protocol=self.config["security_protocol"],
                    ssl_cafile=self.config["ssl_cafile"],
                    ssl_certfile=self.config["ssl_certfile"],
                    ssl_keyfile=self.config["ssl_keyfile"],
                    kafka_client=KarapaceKafkaClient,
                )
                break
            except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                LOG.warning("No Brokers available yet, retrying init_admin_client()")
            except:  # pylint: disable=bare-except
                LOG.exception("Failed to initialize admin client, retrying init_admin_client()")

            time.sleep(2.0)

    def _create_schema_topic_if_needed(self) -> None:
        if self.topic_name != self.config["topic_name"]:
            LOG.info("Topic name overridden, not creating a topic with schema configuration")
            return

        self.init_admin_client()
        start_time = time.monotonic()
        wait_time = constants.MINUTE
        while True:
            if time.monotonic() - start_time > wait_time:
                raise Timeout(f"Timeout ({wait_time}) on creating admin client")

            schema_topic = new_schema_topic_from_config(self.config)
            try:
                LOG.info("Creating schema topic: %r", schema_topic)
                self.admin_client.create_topics([schema_topic], timeout_ms=constants.TOPIC_CREATION_TIMEOUT_MS)
                LOG.info("Topic: %r created successfully", self.config["topic_name"])
                break
            except TopicAlreadyExistsError:
                LOG.info("Topic: %r already exists", self.config["topic_name"])
                break
            except:  # pylint: disable=bare-except
                LOG.exception(
                    "Failed to create topic: %r, retrying _create_schema_topic_if_needed()", self.config["topic_name"]
                )
                time.sleep(5)

    def close(self) -> None:
        LOG.info("Closing schema backup reader")
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.producer:
            self.producer.close()
            self.producer = None
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None

    def restore_backup(self) -> None:
        if not os.path.exists(self.backup_location):
            raise BackupError("Backup location doesn't exist")

        self._create_schema_topic_if_needed()

        if not self.producer:
            self.init_producer()
        LOG.info("Starting backup restore for topic: %r", self.topic_name)

        with open(self.backup_location, encoding="utf8") as fp:
            if _check_backup_file_version(fp) == BackupVersion.V2:
                self._restore_backup_version_2(fp)
            else:
                self._restore_backup_version_1_single_array(fp)
        self.close()

    def _handle_restore_message(self, item: Tuple[str, str]) -> None:
        key = self.encode_key(item[0])
        value = encode_value(item[1])
        self.producer.send(self.topic_name, key=key, value=value, partition=PARTITION_ZERO)
        LOG.debug("Sent kafka msg key: %r, value: %r", key, value)

    def _restore_backup_version_1_single_array(self, fp: IO) -> None:
        values = None
        raw_msg = fp.read()
        values = json_decode(raw_msg)

        if not values:
            return

        for item in values:
            self._handle_restore_message(item)

    def _restore_backup_version_2(self, fp: IO) -> None:
        for line in fp:
            hex_key, hex_value = (val.strip() for val in line.split("\t"))  # strip to remove the linefeed

            key = base64.b16decode(hex_key).decode("utf8") if hex_key != "null" else hex_key
            value = base64.b16decode(hex_value.strip()).decode("utf8") if hex_value != "null" else hex_value
            self._handle_restore_message((key, value))

    def export(self, export_func, *, overwrite: Optional[bool] = None) -> None:
        with _writer(self.backup_location, overwrite=overwrite) as fp:
            if not self.consumer:
                self.init_consumer()
            LOG.info("Starting schema backup read for topic: %r", self.topic_name)

            topic_fully_consumed = False

            fp.write(BACKUP_VERSION_2_MARKER)
            while not topic_fully_consumed:
                raw_msg = self.consumer.poll(timeout_ms=self.timeout_ms, max_records=1000)
                topic_fully_consumed = len(raw_msg) == 0

                for _, messages in raw_msg.items():
                    for message in messages:
                        ser = export_func(key_bytes=message.key, value_bytes=message.value)
                        if ser:
                            fp.write(ser)

            LOG.info("Schema export written to %r", "stdout" if fp is sys.stdout else self.backup_location)
        self.close()

    def encode_key(self, key: Optional[Union[JsonData, str]]) -> Optional[bytes]:
        if key == "null":
            return None
        if not self.key_formatter:
            if isinstance(key, str):
                return key.encode("utf8")
            return json_encode(key, sort_keys=False, binary=True, compact=False)
        if isinstance(key, str):
            key = json_decode(key)
        return self.key_formatter.format_key(key)


def encode_value(value: Union[JsonData, str]) -> Optional[bytes]:
    if value == "null":
        return None
    if isinstance(value, str):
        return value.encode("utf8")
    return json_encode(value, compact=True, sort_keys=False, binary=True)


def serialize_record(key_bytes: Optional[bytes], value_bytes: Optional[bytes]) -> str:
    key = base64.b16encode(key_bytes).decode("utf8") if key_bytes is not None else "null"
    value = base64.b16encode(value_bytes).decode("utf8") if value_bytes is not None else "null"
    return f"{key}\t{value}\n"


def anonymize_avro_schema_message(key_bytes: bytes, value_bytes: bytes) -> str:
    # Check that the message has key `schema` and type is Avro schema.
    # The Avro schemas may have `schemaType` key, if not present the schema is Avro.

    key = json_decode(key_bytes)
    value = json_decode(value_bytes)

    if value and "schema" in value and value.get("schemaType", "AVRO") == "AVRO":
        original_schema = json_decode(value.get("schema"))
        anonymized_schema = anonymize_avro.anonymize(original_schema)
        if anonymized_schema:
            value["schema"] = json_encode(anonymized_schema, compact=True, sort_keys=False)
    if value and "subject" in value:
        value["subject"] = anonymize_avro.anonymize_name(value["subject"])
    # The schemas topic contain all changes to schema metadata.
    if key.get("subject", None):
        key["subject"] = anonymize_avro.anonymize_name(key["subject"])
    return serialize_record(json_encode(key, compact=True).encode("utf8"), json_encode(value, compact=True).encode("utf8"))


def parse_args():
    parser = argparse.ArgumentParser(description="Karapace schema backup tool")
    subparsers = parser.add_subparsers(help="Schema backup command", dest="command", required=True)

    parser_get = subparsers.add_parser("get", help="Store the schema backup into a file")
    parser_restore = subparsers.add_parser("restore", help="Restore the schema backup from a file")
    parser_export_anonymized_avro_schemas = subparsers.add_parser(
        "export-anonymized-avro-schemas", help="Export anonymized Avro schemas into a file"
    )
    for p in [parser_get, parser_restore, parser_export_anonymized_avro_schemas]:
        p.add_argument("--config", help="Configuration file path", required=True)
        p.add_argument("--location", default="", help="File path for the backup file")
        p.add_argument("--topic", help="Kafka topic name to be used", required=False)
    for p in [parser_get, parser_export_anonymized_avro_schemas]:
        p.add_argument("--overwrite", action="store_true", help="Overwrite --location even if it exists.")

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    with open(args.config, encoding="utf8") as handler:
        config = read_config(handler)

    sb = SchemaBackup(config, args.location, args.topic)

    if args.command == "get":
        sb.export(serialize_record, overwrite=args.overwrite)
        return 0
    if args.command == "restore":
        sb.restore_backup()
        return 0
    if args.command == "export-anonymized-avro-schemas":
        sb.export(anonymize_avro_schema_message, overwrite=args.overwrite)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
