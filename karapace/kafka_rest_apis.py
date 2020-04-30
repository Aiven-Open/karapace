from binascii import Error as B64DecodeError
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from kafka import KafkaAdminClient, KafkaConsumer, OffsetAndMetadata, TopicPartition
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import (
    BrokerResponseError, for_code, IllegalStateError, KafkaError, KafkaTimeoutError, UnknownTopicOrPartitionError,
    UnrecognizedBrokerVersion
)
from kafka.future import Future
from kafka.protocol.admin import DescribeConfigsRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from karapace import version as karapace_version
from karapace.karapace import KarapaceBase
from karapace.rapu import HTTPRequest
from karapace.serialization import (
    InvalidMessageHeader, InvalidMessageSchema, InvalidPayload, SchemaRegistryDeserializer, SchemaRegistrySerializer,
    SchemaRetrievalError
)
from struct import error as UnpackError
from threading import Lock
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import argparse
import asyncio
import base64
import copy
import json
import logging
import os
import six
import sys
import time
import uuid

RECORD_KEYS = ["key", "value"]
RECORD_CODES = [42201, 42202]
FILTERED_TOPICS = {"__consumer_offsets"}
KNOWN_FORMATS = {"json", "avro", "binary"}
OFFSET_RESET_STRATEGIES = {"latest", "earliest"}

TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format", "config"])
empty_response = partial(KarapaceBase.r, body={}, status=204, content_type="application/json")


def internal_error(message, content_type):
    KarapaceBase.r(content_type=content_type, status=500, body={"message": message, "error_code": 50001})


def unprocessable_entity(message, sub_code, content_type):
    KarapaceBase.r(content_type=content_type, status=422, body={"message": message, "error_code": sub_code})


def topic_entity(message, sub_code, content_type):
    KarapaceBase.r(content_type=content_type, status=422, body={"message": message, "error_code": sub_code})


def not_found(message, sub_code, content_type):
    KarapaceBase.r(content_type=content_type, status=404, body={"message": message, "error_code": sub_code})


class FormatError(Exception):
    pass


class KafkaRestAdminClient(KafkaAdminClient):
    def get_topic_config(self, topic: str) -> dict:
        config_version = self._matching_api_version(DescribeConfigsRequest)
        req_cfgs = [ConfigResource(ConfigResourceType.TOPIC, topic)]
        cfgs = self.describe_configs(req_cfgs)
        assert len(cfgs) == 1
        assert len(cfgs[0].resources) == 1
        err, _, _, _, config_values = cfgs[0].resources[0]
        if err != 0:
            raise for_code(err)
        topic_config = {}
        for cv in config_values:
            if config_version == 0:
                name, val, _, _, _ = cv
            else:
                name, val, _, _, _, _ = cv
            topic_config[name] = val
        return topic_config

    def new_topic(self, name: str):
        self.create_topics([NewTopic(name, 1, 1)])

    def cluster_metadata(self, topics: List[str] = None) -> dict:
        """List all kafka topics."""
        resp = {"topics": {}}
        brokers = set()
        metadata_version = self._matching_api_version(MetadataRequest)
        if 1 <= metadata_version <= 6:
            if not topics:
                request = MetadataRequest[metadata_version]()
            else:
                request = MetadataRequest[metadata_version](topics=topics)
            future = self._send_request_to_node(self._client.least_loaded_node(), request)
            self._wait_for_futures([future])
            response = future.value
            resp_brokers = response.brokers
            for b in resp_brokers:
                if metadata_version == 0:
                    node_id, _, _ = b
                else:
                    node_id, _, _, _ = b
                brokers.add(node_id)
            resp["brokers"] = list(brokers)
            if not response.topics:
                return resp

            for tup in response.topics:
                if response.API_KEY != 0:
                    err, topic, _, partitions = tup
                else:
                    err, topic, partitions = tup
                if err:
                    raise for_code(err)
                if topic in FILTERED_TOPICS:
                    continue
                topic_data = []
                for part in partitions:
                    if metadata_version <= 4:
                        _, partition_index, leader_id, replica_nodes, isr_nodes = part
                    else:
                        _, partition_index, leader_id, replica_nodes, isr_nodes, _ = part
                    isr_nodes = set(isr_nodes)
                    topic_response = {"partition": partition_index, "leader": leader_id, "replicas": []}
                    for node in replica_nodes:
                        topic_response["replicas"].append({
                            "broker": node,
                            "leader": node == leader_id,
                            "in_sync": node in isr_nodes
                        })
                    topic_data.append(topic_response)
                resp["topics"][topic] = {"partitions": topic_data}
            return resp
        raise UnrecognizedBrokerVersion(
            "Kafka Admin interface cannot determine the controller using MetadataRequest_v{}.".format(metadata_version)
        )

    def make_offsets_request(self, topic: str, partition_id: int, timestamp: int) -> Future:
        v = self._matching_api_version(OffsetRequest)
        if v == 0:
            request = OffsetRequest[0](-1, list(six.iteritems({topic: [(partition_id, timestamp, 1)]})))
        elif v == 1:
            request = OffsetRequest[1](-1, list(six.iteritems({topic: [(partition_id, timestamp)]})))
        else:
            request = OffsetRequest[2](-1, 1, list(six.iteritems({topic: [(partition_id, timestamp)]})))

        future = self._send_request_to_node(self._client.least_loaded_node(), request)
        return future

    def get_offsets(self, topic: str, partition_id: int) -> dict:
        beginning_f = self.make_offsets_request(topic, partition_id, OffsetResetStrategy.EARLIEST)
        end_f = self.make_offsets_request(topic, partition_id, OffsetResetStrategy.LATEST)
        self._wait_for_futures([beginning_f, end_f])
        beginning_resp = beginning_f.value
        end_resp = end_f.value
        v = self._matching_api_version(OffsetRequest)
        assert len(beginning_resp.topics) == 1
        assert len(end_resp.topics) == 1
        _, beginning_partitions = beginning_resp.topics[0]
        _, end_partitions = end_resp.topics[0]

        assert len(beginning_partitions) == 1
        assert len(end_partitions) == 1
        if v == 0:
            assert len(beginning_partitions[0][2]) == 1
            assert partition_id == beginning_partitions[0][0]
            assert partition_id == end_partitions[0][0]
            start_err = beginning_partitions[0][1]
            end_err = beginning_partitions[0][1]
            for e in [start_err, end_err]:
                if e != 0:
                    raise for_code(e)
            rv = {
                "beginning_offset": beginning_partitions[0][2][0],
                "end_offset": end_partitions[0][2][0],
            }
        else:
            rv = {
                "beginning_offset": beginning_partitions[0][3],
                "end_offset": end_partitions[0][3],
            }
        if any(val < 0 for val in rv.values()):
            raise UnknownTopicOrPartitionError("Invalid values for offsets found")
        return rv


class KafkaRest(KarapaceBase):
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.serializer = SchemaRegistrySerializer(config_path=config_path)
        self.log = logging.getLogger("KarapaceRest")
        self.kafka_timeout = 10
        self.loop = asyncio.get_event_loop()
        self._cluster_metadata = None
        self._metadata_birth = None
        self.metadata_max_age = self.config["admin_metadata_max_age"]
        self.admin_client = None
        self.producer_lock = Lock()
        self.admin_lock = Lock()
        self.metadata_cache = None
        self.executor = ThreadPoolExecutor(max_workers=4)
        # Brokers
        self.route("/brokers", callback=self.list_brokers, method="GET", rest_request=True)
        # Consumers
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.commit_consumer_offsets,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.get_consumer_offsets,
            method="GET",
            rest_request=True,
            with_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.update_consumer_subscription,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.get_consumer_subscription,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.delete_consumer_subscription,
            method="DELETE",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.update_consumer_assignment,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.get_consumer_assignment,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/beginning",
            callback=self.seek_beginning_consumer_offsets,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/end",
            callback=self.seek_end_consumer_offsets,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions",
            callback=self.update_consumer_offsets,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/records",
            callback=self.fetch,
            method="GET",
            rest_request=True,
            with_request=True
        )
        self.route("/consumers/<group_name:path>", callback=self.create_consumer, method="POST", rest_request=True)
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>",
            callback=self.delete_consumer,
            method="DELETE",
            rest_request=True
        )
        # Partitions
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>/offsets",
            callback=self.partition_offsets,
            method="GET",
            rest_request=True
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.partition_details,
            method="GET",
            rest_request=True
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.partition_publish,
            method="POST",
            rest_request=True
        )
        self.route("/topics/<topic:path>/partitions", callback=self.list_partitions, method="GET", rest_request=True)
        # Topics
        self.route("/topics", callback=self.list_topics, method="GET", rest_request=True)
        self.route("/topics/<topic:path>", callback=self.topic_details, method="GET", rest_request=True)
        self.route("/topics/<topic:path>", callback=self.topic_publish, method="POST", rest_request=True)
        self.consumer_manager = ConsumerManager(config_path)
        self.init_admin_client()
        self._create_producer()

    async def root_get(self):
        self.r({"application": "Karapace Schema Registry"}, "application/json")

    # CONSUMERS
    def create_consumer(self, group_name: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.create_consumer(group_name, request.json, content_type)

    def delete_consumer(self, group_name: str, instance: str, content_type: str):
        self.consumer_manager.delete_consumer(ConsumerManager.create_internal_name(group_name, instance), content_type)

    def commit_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.commit_offsets(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json, self.cluster_metadata()
        )

    def get_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.get_offsets(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json
        )

    def update_consumer_subscription(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.set_subscription(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
            request.json,
        )

    def get_consumer_subscription(self, group_name: str, instance: str, content_type: str):
        self.consumer_manager.get_subscription(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
        )

    def delete_consumer_subscription(self, group_name: str, instance: str, content_type: str):
        self.consumer_manager.delete_subscription(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
        )

    def update_consumer_assignment(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.set_assignments(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json
        )

    def get_consumer_assignment(self, group_name: str, instance: str, content_type: str):
        self.consumer_manager.get_assignments(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
        )

    def update_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.seek_to(ConsumerManager.create_internal_name(group_name, instance), content_type, request.json)

    def seek_end_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.seek_limit(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json, beginning=False
        )

    def seek_beginning_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        self.consumer_manager.seek_limit(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json, beginning=True
        )

    async def fetch(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest):
        await self.consumer_manager.fetch(
            internal_name=ConsumerManager.create_internal_name(group_name, instance),
            content_type=content_type,
            query_params=request.query,
            formats=request.formats
        )

    # OFFSETS
    def get_offsets(self, topic: str, partition_id: int) -> dict:
        with self.admin_lock:
            return self.admin_client.get_offsets(topic, partition_id)

    def get_topic_config(self, topic: str) -> dict:
        with self.admin_lock:
            return self.admin_client.get_topic_config(topic)

    def cluster_metadata(self, topics: str = None) -> dict:
        if self._metadata_birth is None or time.time() - self._metadata_birth > self.metadata_max_age:
            self._cluster_metadata = None
        if not self._cluster_metadata:
            self._metadata_birth = time.time()
            with self.admin_lock:
                self._cluster_metadata = self.admin_client.cluster_metadata(topics)
        return copy.deepcopy(self._cluster_metadata)

    def init_admin_client(self) -> KafkaAdminClient:
        self.admin_client = KafkaRestAdminClient(
            bootstrap_servers=self.config["bootstrap_uri"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            api_version=(1, 0, 0),
            metadata_max_age_ms=self.config["metadata_max_age_ms"],
        )

    def close(self):
        super().close()
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None
        if self.consumer_manager:
            self.consumer_manager.close()
            self.consumer_manager = None

    async def publish(self, topic: str, partition_id: Optional[str], content_type: str, formats: dict, data: dict):
        # TODO In order to support the other content types, the client and request need to be updated
        _ = self.get_topic_info(topic, content_type)
        if partition_id is not None:
            _ = self.get_partition_info(topic, partition_id, content_type)
            # by this point the conversion should always succeed
            partition_id = int(partition_id)
        await self.validate_publish_request_format(data, formats, content_type, topic)
        status = 200
        ser_format = formats["embedded_format"]
        prepared_records = []
        try:
            prepared_records = await self._prepare_records(
                data=data,
                ser_format=ser_format,
                key_schema_id=data.get("key_schema_id"),
                value_schema_id=data.get("value_schema_id"),
                default_partition=partition_id
            )
        except FormatError:
            unprocessable_entity(
                message=f"Request includes data improperly formatted given the format {ser_format}",
                content_type=content_type,
                sub_code=42205
            )
        except (SchemaRetrievalError, InvalidMessageSchema) as e:
            self.r(body={"error_code": 42205, "message": str(e)}, content_type=content_type, status=422)
        response = {
            "key_schema_id": data.get("key_schema_id"),
            "value_schema_id": data.get("value_schema_id"),
            "offsets": []
        }
        for key, value, partition in prepared_records:
            publish_result = self.produce_message(topic=topic, key=key, value=value, partition=partition)
            if "error" in publish_result and status == 200:
                status = 500
            response["offsets"].append(publish_result)
        self.r(body=response, content_type=content_type, status=status)

    async def partition_publish(self, topic, partition_id, content_type, *, request):
        await self.publish(topic, partition_id, content_type, request.formats, request.json)

    async def topic_publish(self, topic: str, content_type: str, *, request):
        self.log.debug("Executing topic publish on topic %s", topic)
        await self.publish(topic, None, content_type, request.formats, request.json)

    @staticmethod
    def is_valid_avro_request(data: dict, prefix: str) -> bool:
        schema_id = data.get(f"{prefix}_schema_id")
        schema = data.get(f"{prefix}_schema")
        if schema_id:
            try:
                int(schema_id)
                return True
            except (TypeError, ValueError):
                return False
        return isinstance(schema, str)

    async def get_schema_id(self, data: dict, topic: str, prefix: str) -> int:
        self.log.debug("Retrieving schema id for %r", data)
        if f"{prefix}_schema_id" in data and data[f"{prefix}_schema_id"] is not None:
            self.log.debug(
                "Will use schema id %d for serializing %s on topic %s", data[f"{prefix}_schema_id"], prefix, topic
            )
            return int(data[f"{prefix}_schema_id"])
        self.log.debug("Registering / Retrieving ID for schema %s", data[f"{prefix}_schema"])
        subject_name = self.serializer.get_subject_name(topic, data[f"{prefix}_schema"], prefix)
        return await self.serializer.get_id_for_schema(data[f"{prefix}_schema"], subject_name)

    async def validate_schema_info(self, data: dict, prefix: str, content_type: str, topic: str):
        # will do in place updates of id keys, since calling these twice would be expensive
        try:
            data[f"{prefix}_schema_id"] = await self.get_schema_id(data, topic, prefix)
        except InvalidPayload:
            self.log.exception("Unable to retrieve schema id")
            self.r(body={"error_code": 400, "message": "Invalid schema string"}, content_type=content_type, status=400)

    async def _prepare_records(
        self,
        data: dict,
        ser_format: str,
        key_schema_id: Optional[int],
        value_schema_id: Optional[int],
        default_partition: Optional[int] = None
    ) -> List[Tuple]:
        prepared_records = []
        for record in data["records"]:
            key = record.get("key")
            value = record.get("value")
            key = await self.serialize(key, ser_format, key_schema_id)
            value = await self.serialize(value, ser_format, value_schema_id)
            prepared_records.append((key, value, record.get("partition", default_partition)))
        return prepared_records

    def get_partition_info(self, topic: str, partition: str, content_type: str) -> dict:
        try:
            partition = int(partition)
        except ValueError:
            not_found(message=f"Partition id {partition} is badly formatted", content_type=content_type, sub_code=40402)
        try:
            topic_data = self.get_topic_info(topic, content_type)
            partitions = topic_data["partitions"]
            for p in partitions:
                if p["partition"] == partition:
                    return p
            not_found(message=f"Partition {partition} not found", content_type=content_type, sub_code=40402)
        except UnknownTopicOrPartitionError:
            not_found(message=f"Partition {partition} not found", content_type=content_type, sub_code=40402)
        except KeyError:
            not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
        return {}

    def get_topic_info(self, topic: str, content_type: str) -> dict:
        md = self.cluster_metadata()["topics"]
        if topic not in md:
            not_found(message=f"Topic {topic} not found in {list(md.keys())}", content_type=content_type, sub_code=40401)
        return md[topic]

    @staticmethod
    def all_empty(data: dict, key: str) -> bool:
        return all(key not in item or item[key] is None for item in data["records"])

    async def serialize(
        self,
        obj=None,
        ser_format: Optional[str] = None,
        schema_id: Optional[int] = None,
    ) -> bytes:
        if not obj:
            return b''
        # not pretty
        if ser_format == "json":
            # TODO -> get encoding from headers
            return json.dumps(obj).encode("utf8")
        if ser_format == "binary":
            return obj
        if ser_format == "avro":
            return await self.avro_serialize(obj, schema_id)
        raise FormatError(f"Unknown format: {ser_format}")

    async def avro_serialize(self, obj: dict, schema_id: Optional[int]) -> bytes:
        schema = await self.serializer.get_schema_for_id(schema_id)
        bytes_ = await self.serializer.serialize(schema, obj)
        return bytes_

    async def validate_publish_request_format(self, data: dict, formats: dict, content_type: str, topic: str):
        # this method will do in place updates for binary embedded formats, because the validation itself
        # is equivalent to a parse / attempt to parse operation

        # disallow missing or non empty 'records' key
        if "records" not in data or not data["records"]:
            internal_error(message="Invalid request format", content_type=content_type)
        for prefix in RECORD_KEYS:
            for r in data["records"]:
                # disallow empty records
                if not r or len(set(r.keys()).difference(RECORD_KEYS)) > 0:
                    internal_error(message="Invalid request format", content_type=content_type)
                if prefix in r:
                    # disallow non list / dict for json embedded format
                    if formats["embedded_format"] == "json" and not isinstance(r[prefix], (dict, list)):
                        unprocessable_entity(
                            message=f"Invalid json request {prefix}: {r[prefix]}", content_type=content_type, sub_code=42205
                        )
                    # disallow invalid base64
                    if formats["embedded_format"] == "binary":
                        try:
                            r[prefix] = base64.b64decode(r[prefix])
                        except B64DecodeError:
                            unprocessable_entity(
                                message=f"{prefix} is not a proper base64 encoded value: {r[prefix]}",
                                content_type=content_type,
                                sub_code=42205
                            )
        # disallow missing id and schema for any key/value list that has at least one populated element
        if formats["embedded_format"] == "avro":
            for prefix, code in zip(RECORD_KEYS, RECORD_CODES):
                if self.all_empty(data, prefix):
                    continue
                if not self.is_valid_avro_request(data, prefix):
                    unprocessable_entity(
                        message=f"Request includes {prefix}s and uses a format that requires schemas "
                        f"but does not include the {prefix}_schema or {prefix}_schema_id fields",
                        content_type=content_type,
                        sub_code=code
                    )
                try:
                    await self.validate_schema_info(data, prefix, content_type, topic)
                except InvalidMessageSchema as e:
                    unprocessable_entity(message=str(e), content_type=content_type, sub_code=42205)

    def produce_message(self, *, topic: str, key: bytes, value: bytes, partition: int = None) -> dict:
        try:
            with self.producer_lock:
                f = self.producer.send(topic, key=key, value=value, partition=partition)
                self.producer.flush(timeout=self.kafka_timeout)
            result = f.get()
            return {"offset": result.offset, "partition": result.topic_partition.partition}
        except AssertionError as e:
            self.log.exception("Invalid data")
            return {"error_code": 1, "error": str(e)}
        except KafkaTimeoutError:
            self.log.exception("Timed out waiting for publisher")
            # timeouts are retriable
            return {"error_code": 1, "error": "timed out waiting to publish message"}
        except BrokerResponseError as e:
            self.log.exception(e)
            resp = {"error_code": 1, "error": e.description}
            if hasattr(e, "retriable") and e.retriable:
                resp["error_code"] = 2
            return resp

    def list_topics(self, content_type: str):
        metadata = self.cluster_metadata()
        topics = list(metadata["topics"].keys())
        self.r(topics, content_type)

    def topic_details(self, content_type: str, *, topic: str):
        try:
            metadata = self.cluster_metadata([topic])
            config = self.get_topic_config(topic)
            if topic not in metadata["topics"]:
                not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
            data = metadata["topics"][topic]
            data["name"] = topic
            data["configs"] = config
            self.r(data, content_type)
        except UnknownTopicOrPartitionError:
            not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)

    def list_partitions(self, content_type: str, *, topic: str):
        try:
            topic_details = self.cluster_metadata([topic])["topics"]
            self.r(topic_details[topic]["partitions"], content_type)
        except (UnknownTopicOrPartitionError, KeyError):
            not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)

    def partition_details(self, content_type: str, *, topic: str, partition_id: str):
        p = self.get_partition_info(topic, partition_id, content_type)
        self.r(p, content_type)

    def partition_offsets(self, content_type: str, *, topic: str, partition_id: int):
        try:
            partition_id = int(partition_id)
        except ValueError:
            not_found(message=f"Partition {partition_id} not found", content_type=content_type, sub_code=40402)
        try:
            self.r(self.get_offsets(topic, partition_id), content_type)
        except UnknownTopicOrPartitionError:
            # Do a topics request on failure, figure out faster ways once we get correctness down
            if topic not in self.cluster_metadata()["topics"]:
                not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
            not_found(message=f"Partition {partition_id} not found", content_type=content_type, sub_code=40402)

    def list_brokers(self, content_type: str):
        metadata = self.cluster_metadata()
        metadata.pop("topics")
        self.r(metadata, content_type)


class ConsumerManager:
    def __init__(self, config_path: str):
        self.config = KarapaceBase.read_config(config_path)
        self.hostname = f"http://{self.config['advertised_hostname']}:{self.config['port']}"
        self.log = logging.getLogger("RestConsumerManager")
        self.deserializer = SchemaRegistryDeserializer(config_path=config_path)
        self.consumers = {}
        self.consumer_formats = {}
        self.consumer_locks = defaultdict(Lock)

    def new_name(self) -> str:
        name = str(uuid.uuid4())
        self.log.debug("Generated new consumer name: %s", name)
        return name

    @staticmethod
    def _assert(cond: bool, code: int, sub_code: int, message: str, content_type: str):
        if not cond:
            KarapaceBase.r(content_type=content_type, status=code, body={"message": message, "error_code": sub_code})

    def _assert_consumer_exists(self, internal_name: str, content_type: str):
        if internal_name not in self.consumers:
            not_found(message=f"Consumer for {internal_name} not found", content_type=content_type, sub_code=40403)

    @staticmethod
    def _validate_offset_request_data(topic: str, partition_id: int, offset: int, cluster_metadata: dict):
        cluster_metadata = cluster_metadata["topics"]
        assert topic in cluster_metadata
        assert offset > 0
        assert partition_id in [p["partition"] for p in cluster_metadata[topic]["partitions"]]

    @staticmethod
    def _assert_positive_number(container: dict, key: str, content_type: str, code: int = 500, sub_code: int = 50001):
        ConsumerManager._assert_has_key(container, key, content_type)
        ConsumerManager._assert(
            isinstance(container[key], int) and container[key] > 0,
            code=code,
            sub_code=sub_code,
            content_type=content_type,
            message=f"{key} must be a positive number"
        )

    @staticmethod
    def _assert_has_key(element: dict, key: str, content_type: str):
        ConsumerManager._assert(
            key in element, code=500, sub_code=50001, message=f"{key} missing from {element}", content_type=content_type
        )

    @staticmethod
    def _has_topic_and_partition_keys(topic_data: dict, content_type: str):
        for k in ["topic", "partition"]:
            ConsumerManager._assert_has_key(topic_data, k, content_type)

    @staticmethod
    def _topic_and_partition_valid(cluster_metadata: dict, topic_data: dict, content_type: str):
        ConsumerManager._has_topic_and_partition_keys(topic_data, content_type)
        topic = topic_data["topic"]
        partition = topic_data["partition"]
        if topic not in cluster_metadata["topics"]:
            not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
        partitions = {pi["partition"] for pi in cluster_metadata["topics"][topic]["partitions"]}
        if partition not in partitions:
            not_found(
                message=f"Partition {partition} not found for topic {topic}", content_type=content_type, sub_code=40402
            )

    @staticmethod
    def create_internal_name(group_name: str, consumer_name: str) -> str:
        return f"{group_name}_{consumer_name}"

    @staticmethod
    def _validate_create_consumer(request: dict, content_type: str):
        consumer_data_valid = partial(ConsumerManager._assert, content_type=content_type, code=422, sub_code=42204)
        consumer_data_valid(cond="format" in request and request["format"] in KNOWN_FORMATS, message="format key is missing")

        request["consumer.request.timeout.ms"] = request.get(
            "consumer.request.timeout.ms", KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"]
        )
        for k in ["fetch.min.bytes", "consumer.request.timeout.ms"]:
            ConsumerManager._assert_positive_number(request, k, content_type, code=422, sub_code=42204)

        commit_enable = str(request["auto.commit.enable"]).lower()
        consumer_data_valid(
            cond=commit_enable in ["true", "false"], message=f"auto.commit.enable has an invalid value: {commit_enable}"
        )
        request["auto.commit.enable"] = commit_enable == "true"
        consumer_data_valid(
            cond=request["auto.offset.reset"] in OFFSET_RESET_STRATEGIES,
            message=f"auto.offset.reset has an invalid value: {request['auto.offset.reset']}"
        )

    @staticmethod
    def _illegal_state_fail(message: str, content_type: str):
        return ConsumerManager._assert(cond=False, code=409, sub_code=40903, content_type=content_type, message=message)

    @staticmethod
    def _update_partition_assignments(consumer: KafkaConsumer):
        # This is (should be?) equivalent to calling poll on the consumer.
        # which would return 0 results, since the subscription we just created will mean
        # a rejoin is needed, which skips the actual fetching. Nevertheless, an actual call to poll is to be avoided
        # and a better solution to this is desired (extend the consumer??)
        # pylint: disable=W0212
        consumer._coordinator.poll()
        if not consumer._subscription.has_all_fetch_positions():
            consumer._update_fetch_positions(consumer._subscription.missing_fetch_positions())

    # external api below
    # CONSUMER
    def create_consumer(self, group_name: str, request_data: dict, content_type: str):
        self.log.info("Create consumer request for group  %s", group_name)
        consumer_name = request_data.get("name") or self.new_name()
        internal_name = self.create_internal_name(group_name, consumer_name)
        with self.consumer_locks[internal_name]:
            if internal_name in self.consumers:
                self.log.error("Error creating duplicate consumer in group %s with id %s", group_name, consumer_name)
                KarapaceBase.r(
                    status=409,
                    content_type=content_type,
                    body={
                        "error_code": 40902,
                        "message": f"Consumer {consumer_name} already exists"
                    }
                )
            self._validate_create_consumer(request_data, content_type)
            self.log.info(
                "Creating new consumer in group %s with id %s and request_info %r", group_name, consumer_name, request_data
            )
            c = KafkaConsumer(
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
                group_id=group_name,
                fetch_min_bytes=request_data["fetch.min.bytes"],
                request_timeout_ms=request_data["consumer.request.timeout.ms"],
                enable_auto_commit=request_data["auto.commit.enable"],
                auto_offset_reset=request_data["auto.offset.reset"]
            )
            self.consumers[internal_name] = TypedConsumer(
                consumer=c, serialization_format=request_data["format"], config=request_data
            )
            base_uri = urljoin(self.hostname, f"consumers/{group_name}/instances/{consumer_name}")
            KarapaceBase.r(content_type=content_type, body={"base_uri": base_uri, "instance_id": consumer_name})

    def delete_consumer(self, internal_name: str, content_type: str):
        self.log.info("Deleting consumer for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            try:
                c = self.consumers.pop(internal_name)
                c.consumer.close()
                self.consumer_locks.pop(internal_name)
            finally:
                empty_response()

    # OFFSETS
    def commit_offsets(self, internal_name: str, content_type: str, request_data: dict, cluster_metadata: dict):
        self.log.info("Committing offsets for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "offsets", content_type)
        payload = {}
        for el in request_data["offsets"]:
            # If we commit for a partition that does not belong to this consumer, then the internal error raised
            # is marked as retriable, and thus the commit method will remain blocked in what looks like an infinite loop
            self._topic_and_partition_valid(cluster_metadata, el, content_type)
            payload[TopicPartition(el["topic"], el["partition"])] = OffsetAndMetadata(el["offset"] + 1, None)

        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            payload = payload or None
            try:
                consumer.commit(offsets=payload)
            except KafkaError as e:
                internal_error(message=f"error sending commit request: {e}", content_type=content_type)
        empty_response()

    def get_offsets(self, internal_name: str, content_type: str, request_data: dict):
        self.log.info("Retrieving offsets for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        response = {"offsets": []}
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            for el in request_data["partitions"]:
                tp = TopicPartition(el["topic"], el["partition"])
                commit_info = consumer.committed(tp, metadata=True)
                if not commit_info:
                    continue
                response["offsets"].append({
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "metadata": commit_info.metadata,
                    "offset": commit_info.offset
                })
        KarapaceBase.r(body=response, content_type=content_type)

    # SUBSCRIPTION

    def set_subscription(self, internal_name: str, content_type: str, request_data: dict):
        #  TODO -> ATM this gets updated in a background thread, but otoh we have no notion of partition ids using
        #  TODO -> this method, so seek / offsets are not supposed top towk??
        self.log.info("Updating subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        topics = request_data.get("topics", [])
        topics_pattern = request_data.get("topic_pattern")
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            try:
                consumer.subscribe(topics=topics, pattern=topics_pattern)
                self._update_partition_assignments(consumer)
                empty_response()
            except AssertionError:
                self._illegal_state_fail(
                    message="Neither topic_pattern nor topics are present in request", content_type=content_type
                )
            except IllegalStateError as e:
                self._illegal_state_fail(str(e), content_type=content_type)

    def get_subscription(self, internal_name: str, content_type: str):
        self.log.info("Retrieving subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            # since on a subscribed topic, the actual topic list will get refreshed in the background,
            # we can use the same logic here as the refresh code , and the actual poll code will ensure that
            # the pattern is respected when it comes to fetch requests
            consumer = self.consumers[internal_name].consumer
            sub = consumer._subscription  # pylint: disable=W0212
            if consumer.subscription() is None:
                topics = []
            elif not sub.subscribed_pattern:
                topics = list(consumer.subscription())
            else:
                pattern = sub.subscribed_pattern
                valid_topics = consumer.topics()
                self.log.debug("Filtering %r with %r", valid_topics, pattern)
                topics = [t for t in valid_topics if pattern.match(t)]
            KarapaceBase.r(content_type=content_type, body={"topics": topics})

    def delete_subscription(self, internal_name: str, content_type: str):
        self.log.info("Deleting subscription for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            self.consumers[internal_name].consumer.unsubscribe()
        empty_response()

    # ASSIGNMENTS
    def set_assignments(self, internal_name: str, content_type: str, request_data: dict):
        self.log.info("Updating assignments for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        partitions = []
        # TODO -> do we validate here or skip?
        for el in request_data["partitions"]:
            self._has_topic_and_partition_keys(el, content_type)
            partitions.append(TopicPartition(el["topic"], el["partition"]))
        with self.consumer_locks[internal_name]:
            try:
                consumer = self.consumers[internal_name].consumer
                consumer.assign(partitions)
                self._update_partition_assignments(consumer)
                empty_response()
            except IllegalStateError as e:
                self._illegal_state_fail(message=str(e), content_type=content_type)

    def get_assignments(self, internal_name: str, content_type: str):
        self.log.info("Retrieving assignment for %s", internal_name)
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            KarapaceBase.r(
                content_type=content_type,
                body={"partitions": [{
                    "topic": pd.topic,
                    "partition": pd.partition
                } for pd in consumer.assignment()]}
            )

    # POSITIONS
    def seek_to(self, internal_name: str, content_type: str, request_data: dict):
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "offsets", content_type)
        seeks = []
        for el in request_data["offsets"]:
            for k in ["offset", "topic", "partition"]:
                self._assert_has_key(el, k, content_type)
            self._assert_positive_number(el, "offset", content_type)
            seeks.append((TopicPartition(topic=el["topic"], partition=el["partition"]), el["offset"]))
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            # TODO -> does the java rest app fail fast or does it push through
            for part, offset in seeks:
                try:
                    consumer.seek(part, offset)
                except AssertionError:
                    self._illegal_state_fail(f"Partition {part} is unassigned", content_type)
            empty_response()

    def seek_limit(self, internal_name: str, content_type: str, request_data: dict, beginning: bool = True):
        direction = "beginning" if beginning else "end"
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        resets = []
        for el in request_data["partitions"]:
            for k in ["topic", "partition"]:
                self._assert_has_key(el, k, content_type)
            resets.append(TopicPartition(topic=el["topic"], partition=el["partition"]))

        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            try:
                if beginning:
                    consumer.seek_to_beginning(*resets)
                else:
                    consumer.seek_to_end(*resets)
                empty_response()
            except AssertionError:
                self._illegal_state_fail(f"Trying to reset unassigned partitions to {direction}", content_type)

    # making this a coroutine will save some pain for deserialization but will overall make the whole thing
    # much slower and prone to blocking that it needs to be
    async def fetch(self, internal_name: str, content_type: str, formats: dict, query_params: dict):
        self.log.info("Running fetch for name %s with parameters %r and formats %r", internal_name, query_params, formats)
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            serialization_format = self.consumers[internal_name].serialization_format
            config = self.consumers[internal_name].config
            request_format = formats["embedded_format"]
            self._assert(
                cond=serialization_format == request_format,
                code=406,
                sub_code=40601,
                content_type=content_type,
                message=f"Consumer format {serialization_format} does not match the embedded format {request_format}"
            )
            self.log.info("Fetch request for %s with params %r", internal_name, query_params)
            try:
                timeout = int(query_params["timeout"]) if "timeout" in query_params \
                    else config["consumer.request.timeout.ms"]
                # we get to be more in line with the confluent proxy by doing a bunch of fetches each time and
                # respecting the max fetch request size
                max_bytes = ("max_bytes" in query_params and int(query_params["max_bytes"])) or \
                    consumer.config["fetch_max_bytes"]
            except ValueError:
                internal_error(message=f"Invalid request parameters: {query_params}", content_type=content_type)
            for val in [timeout, max_bytes]:
                if not val:
                    continue
                if val <= 0:
                    internal_error(message=f"Invalid request parameter {val}", content_type=content_type)
            response = []
            self.log.info(
                "Will poll multiple times for a single message with a total timeout of %dms, "
                "until at least %d bytes have been fetched", timeout, max_bytes
            )
            read_bytes = 0
            start_time = time.monotonic()
            poll_data = defaultdict(list)
            message_count = 0
            while read_bytes < max_bytes and start_time + timeout/1000 > time.monotonic():
                time_left = start_time + timeout/1000 - time.monotonic()
                bytes_left = max_bytes - read_bytes
                self.log.info("Polling with %r time left and %d bytes left, gathered %d messages so far",
                              time_left, bytes_left, message_count)
                data = consumer.poll(timeout_ms=timeout, max_records=1)
                for topic, records in data.items():
                    for rec in records:
                        message_count += 1
                        read_bytes += \
                            max(0, rec.serialized_key_size) + \
                            max(0, rec.serialized_value_size) + \
                            max(0, rec.serialized_header_size)
                        poll_data[topic].append(rec)
            for tp in poll_data:
                for msg in poll_data[tp]:
                    try:
                        key = await self.deserialize(msg.key, request_format)
                        value = await self.deserialize(msg.value, request_format)
                    except (UnpackError, InvalidMessageHeader, InvalidPayload) as e:
                        internal_error(message=f"deserialization error: {e}", content_type=content_type)
                    element = {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "offset": msg.offset,
                        "key": key,
                        "value": value,
                    }
                    response.append(element)

            KarapaceBase.r(content_type=content_type, body=response)

    async def deserialize(self, bytes_: bytes, fmt: str):
        if not bytes_:
            return None
        if fmt == "avro":
            return await self.deserializer.deserialize(bytes_)
        if fmt == "json":
            return json.loads(bytes_.decode('utf-8'))
        return base64.b64encode(bytes_).decode('utf-8')

    def close(self):
        for k in list(self.consumers.keys()):
            c = self.consumers.pop(k)
            try:
                c.consumer.close()
            except:  # pylint: disable=bare-except
                pass


def main():
    parser = argparse.ArgumentParser(prog="karapace rest", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path")
    arg = parser.parse_args()

    if not os.path.exists(arg.config_file):
        print("Config file: {} does not exist, exiting".format(arg.config_file))
        return 1

    kc = KafkaRest(arg.config_file)
    try:
        return kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace_rest"})
        raise


if __name__ == "__main__":
    sys.exit(main())
