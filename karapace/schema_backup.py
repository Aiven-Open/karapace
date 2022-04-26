"""
karapace - schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from karapace import constants
from karapace.anonymize_schemas import anonymize_avro
from karapace.config import Config, read_config
from karapace.schema_reader import new_schema_topic_from_config
from karapace.typing import JsonData
from karapace.utils import json_encode, KarapaceKafkaClient, Timeout
from typing import Optional

import argparse
import json
import contextlib
import logging
import os
import sys
import time

LOG = logging.getLogger(__name__)


class BackupError(Exception):
    """Backup Error"""


@contextlib.contextmanager
def Writer(filename: Optional[str] = None):
    writer = open(filename, "w", encoding="utf8") if filename else sys.stdout
    yield writer
    if filename:
        writer.close()


class SchemaBackup:
    def __init__(self, config: Config, backup_path: str, topic_option: Optional[str] = None) -> None:
        self.config = config
        self.backup_location = backup_path
        self.topic_name = topic_option or self.config["topic_name"]
        self.consumer = None
        self.producer = None
        self.admin_client = None
        self.timeout_ms = 1000

    def init_consumer(self):
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

    def init_producer(self):
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

    def init_admin_client(self):
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

    def _create_schema_topic_if_needed(self):
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

    def close(self):
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

        values = None
        with open(self.backup_location, mode="r", encoding="utf8") as fp:
            raw_msg = fp.read()
            values = json.loads(raw_msg)

        if not values:
            return

        for item in values:
            key = encode_value(item[0])
            value = encode_value(item[1])
            future = self.producer.send(self.topic_name, key=key, value=value)
            self.producer.flush(timeout=self.timeout_ms)
            msg = future.get(self.timeout_ms)
            LOG.debug("Sent kafka msg key: %r, value: %r, offset: %r", key, value, msg.offset)
        self.close()

    def export(self, export_func) -> None:
        if not self.consumer:
            self.init_consumer()
        LOG.info("Starting schema backup read for topic: %r", self.topic_name)

        topic_fully_consumed = False
        first_schema = True

        with Writer(self.backup_location) as fp:
            fp.write("[")
            while not topic_fully_consumed:

                raw_msg = self.consumer.poll(timeout_ms=self.timeout_ms, max_records=1000)
                topic_fully_consumed = len(raw_msg) == 0

                for _, messages in raw_msg.items():
                    for message in messages:
                        key = message.key.decode("utf8")
                        try:
                            key = json.loads(key)
                        except json.JSONDecodeError:
                            LOG.debug("Invalid JSON in message.key: %r, value: %r", message.key, message.value)
                        value = None
                        if message.value:
                            value = message.value.decode("utf8")
                            try:
                                value = json.loads(value)
                            except json.JSONDecodeError:
                                LOG.debug("Invalid JSON in message.value: %r, key: %r", message.value, message.key)
                        ser = export_func(key=key, value=value)
                        if ser:
                            if not first_schema:
                                fp.write(",\n")
                            else:
                                first_schema = False
                            fp.write(ser)
            fp.write("]")

        if self.backup_location:
            LOG.info("Anonymized Avro schema export written to %r", self.backup_location)
        else:
            LOG.info("Anonymized Avro schema export written to stdout")
        self.close()


def encode_value(value):
    if value == "null":
        return None
    if isinstance(value, str):
        return value.encode("utf8")
    return json_encode(value, sort_keys=False, binary=True)


def serialize_schema_message(key: JsonData, value: JsonData) -> str:
    ser = json.dumps((key, value))
    return ser


def anonymize_avro_schema_message(key: JsonData, value: JsonData) -> str:
    # Check that the message has key `schema` and type is Avro schema.
    # The Avro schemas may have `schemaType` key, if not present the schema is Avro.
    if value and "schema" in value and value.get("schemaType", "AVRO") == "AVRO":
        original_schema = json.loads(value.get("schema"))
        anonymized_schema = anonymize_avro.anonymize(original_schema)
        if anonymized_schema:
            if "subject" in value:
                value["subject"] = anonymize_avro.anonymize_name(value["subject"])
            value["schema"] = anonymized_schema
    # The schemas topic contain all changes to schema metadata.
    if key.get("subject", None):
        key["subject"] = anonymize_avro.anonymize_name(key["subject"])
    ser = json.dumps((key, value))
    return ser


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

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    with open(args.config, encoding="utf8") as handler:
        config = read_config(handler)

    sb = SchemaBackup(config, args.location, args.topic)

    if args.command == "get":
        sb.export(serialize_schema_message)
        return 0
    if args.command == "restore":
        sb.restore_backup()
        return 0
    if args.command == "export-anonymized-avro-schemas":
        sb.export(anonymize_avro_schema_message)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
