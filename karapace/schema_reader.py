"""
karapace - Kafka schema reader

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from aiokafka import AIOKafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from karapace.config import create_ssl_context
from karapace.utils import json_encode

import asyncio
import collections
import json
import logging
import time


class KafkaSchemaReader:
    def __init__(self, config, *, loop):
        self.log = logging.getLogger("KafkaSchemaReader")
        self.api_version_auto_timeout_ms = 30000
        self.topic_creation_timeout_ms = 20000
        self.timeout_ms = 2000
        self.loop = loop
        self.config = config
        self.subjects = {}
        self.schemas = {}
        self.global_schema_id = 0
        self.offset = 0
        self.admin_client = None
        self.schema_topic = None
        self.topic_num_partitions = 1
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer = None
        self.inbound_requests = {}
        self.inbound_locker = asyncio.Lock()
        self.inbound_requests = collections.defaultdict(asyncio.Event)
        self.ready = False
        self.is_ready = asyncio.Event()
        self.topic_created = False
        self.init_done = asyncio.Event()
        self.stopped = asyncio.Event()
        self.running = True

        self.running_task = self.loop.create_task(self.run())
        self.create_schema_topic()

    async def init_consumer(self):
        # Group not set on purpose, all consumers read the same data
        while True:
            try:
                self.consumer = AIOKafkaConsumer(
                    self.config["topic_name"],
                    loop=self.loop,
                    enable_auto_commit=False,
                    bootstrap_servers=self.config["bootstrap_uri"],
                    client_id=self.config["client_id"],
                    security_protocol=self.config["security_protocol"],
                    ssl_context=None if self.config["security_protocol"] != "SSL" else create_ssl_context(self.config),
                    auto_offset_reset="earliest",
                )
                await asyncio.wait_for(self.consumer.start(), timeout=1.0, loop=self.loop)
                break
            except TimeoutError:
                self.log.info("timed out waiting for AIOKafkaConsumer to start, retrying")

    def init_admin_client(self):
        if self.admin_client is not None:
            return
        self.log.info("Initializing admin client for schema reader")
        while True:
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
                self.log.info("Admin client initialized")
                break
            except (NodeNotReadyError, NoBrokersAvailable, AssertionError) as e:
                self.log.error("No Brokers available yet, retrying init_admin_client(): %r", e)
                time.sleep(3.0)
            except Exception as e:  # pylint: disable=W0703
                self.log.exception("Failed to initialize admin client, retrying init_admin_client(): %r", e)
                time.sleep(2.0)

    def create_schema_topic(self):
        self.init_admin_client()
        if self.topic_created:
            return
        schema_topic = NewTopic(
            name=self.config["topic_name"],
            num_partitions=self.topic_num_partitions,
            replication_factor=self.config["replication_factor"],
            topic_configs={"cleanup.policy": "compact"}
        )
        while True:
            try:
                self.log.info("Creating topic: %r", schema_topic)
                self.admin_client.create_topics([schema_topic], timeout_ms=self.topic_creation_timeout_ms)
                self.log.info("Topic: %r created successfully", self.config["topic_name"])
                self.schema_topic = schema_topic
                self.topic_created = True
                break
            except TopicAlreadyExistsError:
                self.log.warning("Topic: %r already exists", self.config["topic_name"])
                self.schema_topic = schema_topic
                self.topic_created = True
                break
            except:  # pylint: disable=bare-except
                self.log.exception("Failed to create topic: %r, retrying create_schema_topic()", self.config["topic_name"])
                time.sleep(3)

    def get_schema_id(self, new_schema):
        new_schema_encoded = json_encode(new_schema.to_json(), compact=True)
        for schema_id, schema in self.schemas.items():
            if schema == new_schema_encoded:
                return schema_id
        self.global_schema_id += 1
        return self.global_schema_id

    async def close(self):
        self.log.info("Closing schema_reader")
        self.running = False
        await self.stopped.wait()

    async def run(self):
        self.log.info("schema reader is waiting for kafka consumer to start")
        await self.init_consumer()
        self.log.info("schema reader is waiting for topic to be created")
        while not self.topic_created:
            await asyncio.sleep(1)
        self.init_done.set()
        self.log.info("Starting message handling loop")
        while self.running:
            await self.handle_messages()
        self.log.info("Closing down schema reader components")
        if self.admin_client:
            self.admin_client.close()
        if self.consumer:
            await self.consumer.stop()
        self.stopped.set()

    async def handle_messages(self):
        raw_msgs = await self.consumer.getmany(timeout_ms=self.timeout_ms)
        if not self.is_ready.is_set() and sum(len(raw_msgs[x]) for x in raw_msgs) == 0:
            self.log.info("signaling karapace app we're ready")
            self.is_ready.set()

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
                if self.is_ready.is_set():
                    async with self.inbound_locker:
                        self.inbound_requests[self.offset].set()
                    self.log.info("Signaling processing done for offset %d", self.offset)
                else:
                    self.log.info("Skipping message with offset %d, not ready to process data yet", self.offset)

    def handle_msg(self, key, value):
        if key["keytype"] == "CONFIG":
            if "subject" in key and key["subject"] is not None:
                if not value:
                    self.log.info("Deleting compatibility config completely for subject: %r", key["subject"])
                    self.subjects[key["subject"]].pop("compatibility", None)
                    return
                self.log.info(
                    "Setting subject: %r config to: %r, value: %r", key["subject"], value["compatibilityLevel"], value
                )
                if not key["subject"] in self.subjects:
                    self.log.info("Adding first version of subject: %r with no schemas", key["subject"])
                    self.subjects[key["subject"]] = {"schemas": {}}
                subject_data = self.subjects.get(key["subject"])
                subject_data["compatibility"] = value["compatibilityLevel"]
            else:
                self.log.info("Setting global config to: %r, value: %r", value["compatibilityLevel"], value)
                self.config["compatibility"] = value["compatibilityLevel"]
        elif key["keytype"] == "SCHEMA":
            if not value:
                self.log.info("Deleting subject: %r version: %r completely", key["subject"], key["version"])
                self.subjects[key["subject"]]["schemas"].pop(key["version"], None)
                return
            subject = value["subject"]
            if subject not in self.subjects:
                self.log.info("Adding first version of subject: %r, value: %r", subject, value)
                self.subjects[subject] = {
                    "schemas": {
                        value["version"]: {
                            "schema": value["schema"],
                            "version": value["version"],
                            "id": value["id"],
                            "deleted": value.get("deleted", False),
                        }
                    }
                }
                self.log.info("Setting schema_id: %r with schema: %r", value["id"], value["schema"])
                self.schemas[value["id"]] = value["schema"]
                if value["id"] > self.global_schema_id:  # Not an existing schema
                    self.global_schema_id = value["id"]
            elif value.get("deleted", False) is True:
                self.log.info("Deleting subject: %r, version: %r", subject, value["version"])
                if not value["version"] in self.subjects[subject]["schemas"]:
                    self.log.error(
                        "Subject: %r, version: %r, value: %r did not exist, should have.", subject, value["version"], value
                    )
                else:
                    self.subjects[subject]["schemas"][value["version"]]["deleted"] = True
            elif value.get("deleted", False) is False:
                self.log.info("Adding new version of subject: %r, value: %r", subject, value)
                self.subjects[subject]["schemas"][value["version"]] = {
                    "schema": value["schema"],
                    "version": value["version"],
                    "id": value["id"],
                    "deleted": value.get("deleted", False),
                }
                self.log.info("Setting schema_id: %r with schema: %r", value["id"], value["schema"])
                self.schemas[value["id"]] = value["schema"]
                if value["id"] > self.global_schema_id:  # Not an existing schema
                    self.global_schema_id = value["id"]
        elif key["keytype"] == "DELETE_SUBJECT":
            self.log.info("Deleting subject: %r, value: %r", value["subject"], value)
            if not value["subject"] in self.subjects:
                self.log.error("Subject: %r did not exist, should have", value["subject"])
            else:
                updated_schemas = {
                    key: self._delete_schema_below_version(schema, value["version"])
                    for key, schema in self.subjects[value["subject"]]["schemas"].items()
                }
                self.subjects[value["subject"]]["schemas"] = updated_schemas
        elif key["keytype"] == "NOOP":  # for spec completeness
            pass

    @staticmethod
    def _delete_schema_below_version(schema, version):
        if schema["version"] <= version:
            schema["deleted"] = True
        return schema

    def get_schemas(self, subject):
        non_deleted_schemas = {
            key: val
            for key, val in self.subjects[subject]["schemas"].items()
            if val.get("deleted", False) is False
        }
        return non_deleted_schemas
