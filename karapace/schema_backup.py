"""
karapace - schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaConsumer, KafkaProducer
from karapace.karapace import Karapace
from karapace.utils import json_encode

import argparse
import json
import logging
import os
import sys


class BackupError(Exception):
    """Backup Error"""


class SchemaBackup:
    def __init__(self, config_path, backup_path):
        self.config = Karapace.read_config(config_path)
        self.backup_location = backup_path
        self.topic_name = self.config["topic_name"]
        self.log = logging.getLogger("SchemaBackup")
        self.consumer = None
        self.producer = None
        self.timeout_ms = 1000

    def init_consumer(self):
        self.consumer = KafkaConsumer(
            self.topic_name,
            enable_auto_commit=False,
            api_version=(1, 0, 0),
            bootstrap_servers=self.config["bootstrap_uri"],
            client_id=self.config["client_id"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            auto_offset_reset="earliest",
            metadata_max_age_ms=self.config["metadata_max_age_ms"],
        )

    def init_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.config["bootstrap_uri"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            api_version=(1, 0, 0),
        )

    def close(self):
        self.log.info("Closing schema backup reader")
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.producer:
            self.producer.close()
            self.producer = None

    def request_backup(self):
        if not self.consumer:
            self.init_consumer()
        self.log.info("Starting schema backup read for topic: %r", self.topic_name)
        values = []
        raw_msg = self.consumer.poll(timeout_ms=self.timeout_ms)
        for _, messages in raw_msg.items():
            for message in messages:
                try:
                    key = json.loads(message.key.decode("utf8"))
                except json.JSONDecodeError:
                    self.log.exception("Invalid JSON in message.key: %r, value: %r", message.key, message.value)
                    continue
                value = None
                if message.value:
                    try:
                        value = json.loads(message.value.decode("utf8"))
                    except json.JSONDecodeError:
                        self.log.exception("Invalid JSON in message.value: %r, key: %r", message.value, message.key)
                        continue
                values.append((key, value))

        with open(self.backup_location, "w") as fp:
            fp.write(json.dumps(values))
            self.log.info("Schema backup written to %r", self.backup_location)
        self.close()

    def restore_backup(self):
        if not os.path.exists(self.backup_location):
            raise BackupError("Backup location doesn't exist")

        if not self.producer:
            self.init_producer()
        self.log.info("Starting backup restore for topic: %r", self.topic_name)

        values = None
        with open(self.backup_location, "r") as fp:
            raw_msg = fp.read()
            values = json.loads(raw_msg)
        if not values:
            raise BackupError("Nothing to restore in %s" % self.backup_location)

        for item in values:
            key = encode_value(item[0])
            value = encode_value(item[1])
            future = self.producer.send(self.topic_name, key=key, value=value)
            self.producer.flush(timeout=self.timeout_ms)
            msg = future.get(self.timeout_ms)
            self.log.debug("Sent kafka msg key: %r, value: %r, offset: %r", key, value, msg.offset)
        self.close()


def encode_value(value):
    if value == "null":
        return None
    if isinstance(value, str):
        return value.encode("utf8")
    return json_encode(value, sort_keys=False, binary=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Karapace schema backup tool")
    subparsers = parser.add_subparsers(help="Schema backup command", dest="command", required=True)

    parser_get = subparsers.add_parser("get", help="Store the schema backup into a file")
    parser_restore = subparsers.add_parser("restore", help="Restore the schema backup from a file")
    for p in {parser_get, parser_restore}:
        p.add_argument("--config", help="Configuration file path", required=True)
        p.add_argument("--location", help="File path for the backup file", required=True)

    return parser.parse_args()


def main():
    args = parse_args()
    sb = SchemaBackup(args.config, args.location)

    if args.command == "get":
        return sb.request_backup()
    if args.command == "restore":
        return sb.restore_backup()
    return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
