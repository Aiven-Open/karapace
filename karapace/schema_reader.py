"""
karapace - Kafka schema reader

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from enum import Enum, unique
from json import JSONDecodeError, loads
from jsonschema.exceptions import SchemaError
from jsonschema.validators import Draft7Validator
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from karapace import constants
from karapace.utils import json_encode
from queue import Queue
from threading import Thread
from typing import Union

import avro
import json
import logging
import time


class InvalidSchema(Exception):
    pass


@unique
class SchemaType(Enum):
    AVRO = "AVRO"
    JSONSCHEMA = "JSONSCHEMA"
    PROTOBUF = "PROTOBUF"


class TypedSchema:
    def __init__(self, schema: Union[Draft7Validator, avro.schema.Schema], schema_type: SchemaType):
        self.schema_type = schema_type
        self.schema = schema

    @staticmethod
    def parse_json(schema_str: str):
        try:
            js = loads(schema_str)
            Draft7Validator.check_schema(js)
            return TypedSchema(Draft7Validator(js), SchemaType.JSONSCHEMA)
        except (JSONDecodeError, SchemaError) as e:
            raise InvalidSchema from e

    @staticmethod
    def parse_avro(schema_str: str):
        try:
            return TypedSchema(avro.schema.Parse(schema_str), SchemaType.AVRO)
        except avro.schema.SchemaParseException as e:
            raise InvalidSchema from e

    def to_json(self):
        return self.schema.schema if self.schema_type is SchemaType.JSONSCHEMA else self.schema.to_json(names=None)


class KafkaSchemaReader(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.log = logging.getLogger("KafkaSchemaReader")
        self.timeout_ms = 200
        self.config = config
        self.subjects = {}
        self.schemas = {}
        self.global_schema_id = 0
        self.offset = 0
        self.admin_client = None
        self.schema_topic = None
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
                api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
            )
            return True
        except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
            self.log.warning("No Brokers available yet, retrying init_admin_client()")
            time.sleep(2.0)
        except:  # pylint: disable=bare-except
            self.log.exception("Failed to initialize admin client, retrying init_admin_client()")
            time.sleep(2.0)
        return False

    @staticmethod
    def get_new_schema_topic(config):
        return NewTopic(
            name=config["topic_name"],
            num_partitions=constants.SCHEMA_TOPIC_NUM_PARTITIONS,
            replication_factor=config["replication_factor"],
            topic_configs={"cleanup.policy": "compact"}
        )

    def create_schema_topic(self):
        schema_topic = self.get_new_schema_topic(self.config)
        try:
            self.log.info("Creating topic: %r", schema_topic)
            self.admin_client.create_topics([schema_topic], timeout_ms=constants.TOPIC_CREATION_TIMEOUT_MS)
            self.log.info("Topic: %r created successfully", self.config["topic_name"])
            self.schema_topic = schema_topic
            return True
        except TopicAlreadyExistsError:
            self.log.warning("Topic: %r already exists", self.config["topic_name"])
            self.schema_topic = schema_topic
            return True
        except:  # pylint: disable=bare-except
            self.log.exception("Failed to create topic: %r, retrying create_schema_topic()", self.config["topic_name"])
            time.sleep(5)
        return False

    def get_schema_id(self, new_schema):
        new_schema_encoded = json_encode(new_schema.to_json(), compact=True)
        for schema_id, schema in self.schemas.items():
            if schema == new_schema_encoded:
                return schema_id
        self.global_schema_id += 1
        return self.global_schema_id

    def close(self):
        self.log.info("Closing schema_reader")
        self.running = False

    def run(self):
        while self.running:
            if not self.admin_client:
                if self.init_admin_client() is False:
                    continue
            if not self.schema_topic:
                if self.create_schema_topic() is False:
                    continue
            if not self.consumer:
                self.init_consumer()
            self.handle_messages()

        if self.admin_client:
            self.admin_client.close()
        if self.consumer:
            self.consumer.close()

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
                            "schema_type": value.get("schema_type", SchemaType.AVRO).value,
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
                    "schema_type": value.get("schema_type", SchemaType.AVRO).value,
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
