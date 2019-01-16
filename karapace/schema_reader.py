"""
karapace - Kafka schema reader

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import json
import logging
import time
from queue import Queue
from threading import Thread

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError)


class KafkaSchemaReader(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.log = logging.getLogger("KafkaSchemaReader")
        self.api_version_auto_timeout_ms = 30000
        self.topic_creation_timeout_ms = 20000
        self.timeout_ms = 200
        self.config = config
        self.subjects = {}
        self.schemas = {}
        self.global_schema_id = 0
        self.offset = 0
        self.admin_client = None
        self.topic_num_partitions = 1
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer = None
        self.queue = Queue()
        self.ready = False
        self.running = True

    def init_consumer(self):
        # Group not set on purpose, all consumers read the same data
        self.consumer = KafkaConsumer(
            self.config["topic_name"],
            enable_auto_commit=False,
            api_version=(1, 0, 0),
            bootstrap_servers=self.config["bootstrap_uri"],
            client_id=self.config["client_id"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            auto_offset_reset="earliest",
        )

    def init_admin_client(self):
        try:
            self.admin_client = KafkaAdminClient(
                api_version_auto_timeout_ms=self.api_version_auto_timeout_ms,
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
            )
            return True
        except (NodeNotReadyError, NoBrokersAvailable):
            self.log.warning("No Brokers available yet, retrying init_admin_client()")
            time.sleep(2.0)
        return False

    def create_schema_topic(self):
        schema_topic = NewTopic(
            name=self.config["topic_name"],
            num_partitions=self.topic_num_partitions,
            replication_factor=self.config["replication_factor"],
            topic_configs={"cleanup.policy": "compact"}
        )
        try:
            self.log.info("Creating topic: %r", schema_topic)
            self.admin_client.create_topics([schema_topic], timeout_ms=self.topic_creation_timeout_ms)
            self.log.info("Topic: %r created successfully", self.config["topic_name"])
            return True
        except TopicAlreadyExistsError:
            self.log.warning("Topic: %r already exists", self.config["topic_name"])
            return True
        return False

    def get_new_schema_id(self):
        self.global_schema_id += 1
        return self.global_schema_id

    def close(self):
        self.log.info("Closing schema_reader")
        self.running = False
        if self.admin_client:
            self.admin_client.close()
        if self.consumer:
            self.consumer.close()

    def run(self):
        while self.running:
            if not self.admin_client:
                if self.init_admin_client() is False:
                    continue
                self.create_schema_topic()
            if not self.consumer:
                self.init_consumer()
            self.handle_messages()

    def handle_messages(self):
        raw_msgs = self.consumer.poll(timeout_ms=self.timeout_ms)
        if self.ready is False and raw_msgs == {}:
            self.ready = True

        for _, msgs in raw_msgs.items():
            for msg in msgs:
                try:
                    key = json.loads(msg.key.decode("utf8"))
                except json.JSONDecodeError:
                    self.log.exception("Invalid JSON in msg.key: %r, value: %r", msg.key, msg.value)
                    continue

                value = None
                if msg.value:
                    try:
                        value = json.loads(msg.value.decode("utf8"))
                    except json.JSONDecodeError:
                        self.log.exception("Invalid JSON in msg.value: %r, key: %r", msg.value, msg.key)
                        continue

                self.log.info("Read new record: key: %r, value: %r, offset: %r", key, value, msg.offset)
                self.handle_msg(key, value)
                self.offset = msg.offset
                self.log.info("Handled message, current offset: %r", self.offset)
                if self.ready:
                    self.queue.put(self.offset)

    def handle_msg(self, key, value):
        if key["keytype"] == "CONFIG":
            if "subject" in key and key["subject"] is not None:
                self.log.info(
                    "Setting subject: %r config to: %r, value: %r", key["subject"], value["compatibilityLevel"], value
                )
                subject_data = self.subjects.get(key["subject"])
                subject_data["compatibility"] = value["compatibilityLevel"]
            else:
                self.log.info("Setting global config to: %r, value: %r", value["compatibilityLevel"], value)
                self.config["compatibility"] = value["compatibilityLevel"]
        elif key["keytype"] == "SCHEMA":
            subject = value["subject"]
            if subject not in self.subjects:
                self.log.info("Adding first version of subject: %r, value: %r", subject, value)
                self.subjects[subject] = {
                    "schemas": {
                        value["version"]: {
                            "schema": value["schema"],
                            "version": value["version"],
                            "id": value["id"],
                        }
                    }
                }
                self.log.info("Setting schema_id: %r with schema: %r", value["id"], value["schema"])
                self.schemas[value["id"]] = value["schema"]
                self.global_schema_id = value["id"]
            elif value["deleted"] is True:
                self.log.info("Deleting subject: %r, version: %r", subject, value["version"])
                entry = self.subjects[subject]["schemas"].pop(value["version"], None)
                if not entry:
                    self.log.error(
                        "Subject: %r, version: %r, value: %r did not exist, should have.", subject, value["version"], value
                    )
                else:
                    self.log.info("Deleting schema_id: %r, schema: %r", value["id"], self.schemas.get(value["id"]))
                    entry = self.schemas.pop(value["id"], None)
                    if not entry:
                        self.log.error("Schema: %r did not exist, should have", value["id"])
            elif value["deleted"] is False:
                self.log.info("Adding new version of subject: %r, value: %r", subject, value)
                self.subjects[subject]["schemas"][value["version"]] = {
                    "schema": value["schema"],
                    "version": value["version"],
                    "id": value["id"]
                }
                self.log.info("Setting schema_id: %r with schema: %r", value["id"], value["schema"])
                self.schemas[value["id"]] = value["schema"]
                self.global_schema_id = value["id"]
        elif key["keytype"] == "DELETE_SUBJECT":
            self.log.info("Deleting subject: %r, value: %r", value["subject"], value)
            subject = self.subjects.pop(value["subject"], None)
            if not subject:
                self.log.error("Subject: %r did not exist, should have", value["subject"])
            else:
                for schema in subject["schemas"].values():
                    self.log.info(
                        "Deleting subject: %r, schema_id: %r, schema: %r", subject, schema["id"],
                        self.schemas.get(schema["id"])
                    )
                    entry = self.schemas.pop(schema["id"], None)
                    if not entry:
                        self.log.error("Schema: %r did not exist, should have", schema["id"])
        elif key["keytype"] == "NOOP":  # for spec completeness
            pass
