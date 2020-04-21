from binascii import Error as B64DecodeError
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from kafka import KafkaAdminClient, KafkaConsumer, OffsetAndMetadata, TopicPartition
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import (
    BrokerResponseError, for_code, KafkaTimeoutError, UnknownTopicOrPartitionError,
    UnrecognizedBrokerVersion, KafkaError, IllegalStateError
)
from kafka.protocol.admin import DescribeConfigsRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from karapace import version as karapace_version
from karapace.karapace import KarapaceBase
from karapace.serialization import (
    InvalidMessageSchema, InvalidPayload, SchemaRegistryDeserializer, SchemaRegistrySerializer, SchemaRetrievalError
)
from threading import Lock
from typing import List, Optional, Tuple

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

TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format"])


class FormatError(Exception):
    pass


class KafkaRestAdminClient(KafkaAdminClient):
    def get_topic_config(self, topic):
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

    def new_topic(self, name):
        self.create_topics([NewTopic(name, 1, 1)])

    def cluster_metadata(self, topics=None):
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

    def make_offsets_request(self, topic, partition_id, timestamp):
        v = self._matching_api_version(OffsetRequest)
        if v == 0:
            request = OffsetRequest[0](-1, list(six.iteritems({topic: [(partition_id, timestamp, 1)]})))
        elif v == 1:
            request = OffsetRequest[1](-1, list(six.iteritems({topic: [(partition_id, timestamp)]})))
        else:
            request = OffsetRequest[2](-1, 1, list(six.iteritems({topic: [(partition_id, timestamp)]})))

        future = self._send_request_to_node(self._client.least_loaded_node(), request)
        return future

    def get_offsets(self, topic, partition_id):
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
    def __init__(self, config_path):
        super().__init__(config_path)
        self.serializer = SchemaRegistrySerializer(config_path=config_path)
        self.deserializer = SchemaRegistryDeserializer(config_path=config_path)
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
        self.route("/consumers/<group_name:path>", callback=self.placeholder, method="GET", rest_request=True)
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>",
            callback=self.placeholder,
            method="DELETE",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.placeholder,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.placeholder,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.placeholder,
            method="DELETE",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.placeholder,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/beginning",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/end",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/records",
            callback=self.placeholder,
            method="GET",
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
        self.init_admin_client()
        self._create_producer()

    def get_offsets(self, topic, partition_id):
        with self.admin_lock:
            return self.admin_client.get_offsets(topic, partition_id)

    def get_topic_config(self, topic):
        with self.admin_lock:
            return self.admin_client.get_topic_config(topic)

    def cluster_metadata(self, topics=None):
        if self._metadata_birth is None or time.time() - self._metadata_birth > self.metadata_max_age:
            self._cluster_metadata = None
        if not self._cluster_metadata:
            self._metadata_birth = time.time()
            with self.admin_lock:
                self._cluster_metadata = self.admin_client.cluster_metadata(topics)
        return copy.deepcopy(self._cluster_metadata)

    def init_admin_client(self):
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
        self.admin_client.close()
        self.admin_client = None

    def placeholder(self, content_type):
        self.r(body={"error_code": 40401, "message": "Not implemented"}, content_type=content_type, status=404)

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
            self.r(
                body={
                    "error_code": 42205,
                    "message": f"Request includes data improperly formatted given the format {ser_format}"
                },
                content_type=content_type,
                status=422
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

    async def partition_publish(self, topic, partition_id, content_type, formats, *, request):
        await self.publish(topic, partition_id, content_type, formats, request.json)

    async def topic_publish(self, topic, content_type, formats, *, request):
        await self.publish(topic, None, content_type, formats, request.json)

    @staticmethod
    def is_valid_avro_request(data, prefix):
        schema_id = data.get(f"{prefix}_schema_id")
        schema = data.get(f"{prefix}_schema")
        if schema_id:
            try:
                int(schema_id)
                return True
            except (TypeError, ValueError):
                return False
        return isinstance(schema, str)

    async def get_schema_id(self, data, topic, prefix):
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
            self.r(
                body={
                    "error_code": 40402,
                    "message": f"Partition id {partition} is badly formatted"
                },
                content_type=content_type,
                status=404
            )
        try:
            topic_data = self.get_topic_info(topic, content_type)
            partitions = topic_data["partitions"]
            for p in partitions:
                if p["partition"] == partition:
                    return p
            self.r(
                body={
                    "error_code": 40402,
                    "message": f"Partition {partition} not found"
                },
                content_type=content_type,
                status=404
            )
        except UnknownTopicOrPartitionError:
            self.r(
                body={
                    "error_code": 40402,
                    "message": f"Partition {partition} not found"
                },
                content_type=content_type,
                status=404
            )
        except KeyError:
            self.r(body={"error_code": 40401, "message": f"Topic {topic} not found"}, content_type=content_type, status=404)
        return {}

    def get_topic_info(self, topic: str, content_type: str) -> dict:
        md = self.cluster_metadata()["topics"]
        if topic not in md:
            self.r(
                body={
                    "error_code": 40401,
                    "message": f"Topic {topic} not found in {list(md.keys())}"
                },
                content_type=content_type,
                status=404
            )
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

    async def avro_serialize(self, obj: dict, schema_id: Optional[int]):
        schema = await self.serializer.get_schema_for_id(schema_id)
        bytes_ = await self.serializer.serialize(schema, obj)
        return bytes_

    async def validate_publish_request_format(self, data: dict, formats: dict, content_type: str, topic: str):
        # this method will do in place updates for binary embedded formats, because the validation itself
        # is equivalent to a parse / attempt to parse operation

        # disallow missing or non empty 'records' key
        if "records" not in data or not data["records"]:
            self.r(
                body={
                    "error_code": 50001,  # Choose another code??
                    "message": "Invalid request format"
                },
                content_type=content_type,
                status=500
            )
        for prefix in RECORD_KEYS:
            for r in data["records"]:
                # disallow empty records
                if not r or len(set(r.keys()).difference(RECORD_KEYS)) > 0:
                    self.r(
                        body={
                            "error_code": 50001,
                            "message": "Invalid request format"
                        },
                        content_type=content_type,
                        status=500
                    )
                if prefix in r:
                    # disallow non list / dict for json embedded format
                    if formats["embedded_format"] == "json" and not isinstance(r[prefix], (dict, list)):
                        self.r(
                            body={
                                "error_code": 42205,
                                "message": f"Invalid json request {prefix}: {r[prefix]}"
                            },
                            content_type=content_type,
                            status=422
                        )
                    # disallow invalid base64
                    if formats["embedded_format"] == "binary":
                        try:
                            r[prefix] = base64.b64decode(r[prefix])
                        except B64DecodeError:
                            self.r(
                                body={
                                    "error_code": 42205,
                                    "message": f"{prefix} is not a proper base64 encoded value: {r[prefix]}"
                                },
                                content_type=content_type,
                                status=422
                            )

        # disallow missing id and schema for any key/value list that has at least one populated element
        if formats["embedded_format"] == "avro":
            for prefix, code in zip(RECORD_KEYS, RECORD_CODES):
                if self.all_empty(data, prefix):
                    continue
                if not self.is_valid_avro_request(data, prefix):
                    self.r(
                        body={
                            "error_code": code,
                            "message": f"Request includes {prefix}s and uses a format that requires "
                            f"schemas but does not include the {prefix}_schema or {prefix}_schema_id fields"
                        },
                        content_type=content_type,
                        status=422
                    )
                try:
                    await self.validate_schema_info(data, prefix, content_type, topic)
                except InvalidMessageSchema as e:
                    self.r(body={"error_code": 42205, "message": str(e)}, content_type=content_type, status=422)

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
                self.r(
                    body={
                        "error_code": 40401,
                        "message": f"Topic {topic} not found"
                    }, content_type=content_type, status=404
                )

            data = metadata["topics"][topic]
            data["name"] = topic
            data["configs"] = config
            self.r(data, content_type)
        except UnknownTopicOrPartitionError:
            self.r(body={"error_code": 40401, "message": f"Topic {topic} not found"}, content_type=content_type, status=404)

    def list_partitions(self, content_type, *, topic):
        try:
            topic_details = self.cluster_metadata([topic])["topics"]
            self.r(topic_details[topic]["partitions"], content_type)
        except (UnknownTopicOrPartitionError, KeyError):
            self.r(body={"error_code": 40401, "message": f"Topic {topic} not found"}, content_type=content_type, status=404)

    def partition_details(self, content_type, *, topic, partition_id):
        p = self.get_partition_info(topic, partition_id, content_type)
        self.r(p, content_type)

    def partition_offsets(self, content_type, *, topic, partition_id):
        try:
            partition_id = int(partition_id)
        except ValueError:
            self.r(
                body={
                    "error_code": 40402,
                    "message": f"Partition {partition_id} not found"
                },
                content_type=content_type,
                status=404
            )
        try:
            self.r(self.get_offsets(topic, partition_id), content_type)
        except UnknownTopicOrPartitionError:
            # Do a topics request on failure, figure out faster ways once we get correctness down
            if topic not in self.cluster_metadata()["topics"]:
                self.r(
                    body={
                        "error_code": 40401,
                        "message": f"Topic {topic} not found"
                    }, content_type=content_type, status=404
                )
            self.r(
                body={
                    "error_code": 40402,
                    "message": f"Partition {partition_id} not found"
                },
                content_type=content_type,
                status=404
            )

    def list_brokers(self, content_type):  # pylint: disable=unused-argument
        metadata = self.cluster_metadata()
        metadata.pop("topics")
        self.r(metadata, content_type)


class ConsumerManager:
    def __init__(self, config_path):
        self.config = KarapaceBase.read_config(config_path)
        self.hostname = f"http://{self.config['advertised_hostname']}:{self.config['port']}"
        self.log = logging.getLogger("RestConsumerManager")
        self.consumers = {}
        self.consumer_formats = {}
        self.consumer_locks = defaultdict(Lock)

    def new_name(self):
        name = str(uuid.uuid4())
        self.log.debug("Generated new consumer name: %s", name)
        return name

    @staticmethod
    def _assert(cond, code, sub_code, message, content_type):
        if not cond:
            KarapaceBase.r(content_type=content_type, status=code, body={"message": message, "error_code": sub_code})

    def _assert_consumer_exists(self, internal_name, content_type):
        ConsumerManager._assert(
            internal_name in self.consumers, code=404, sub_code=40403,
            message=f"Consumer not found", content_type=content_type
        )

    @staticmethod
    def _validate_offset_request_data(topic, partition_id, offset, cluster_metadata):
        cluster_metadata = cluster_metadata["topics"]
        assert topic in cluster_metadata
        assert offset > 0
        assert partition_id in [p["partition"] for p in cluster_metadata[topic]["partitions"]]

    @staticmethod
    def _assert_positive_number(container, key, content_type, code=500, sub_code=50001):
        ConsumerManager._assert_has_key(container, key, content_type)
        ConsumerManager._assert(
            isinstance(container[key], int) and container[key] > 0, code=code,
            sub_code=sub_code, content_type=content_type, message=f"{key} must be a positive number"
        )

    @staticmethod
    def _assert_has_key(element, key, content_type):
        ConsumerManager._assert(
            key in element, code=500, sub_code=50001, message=f"{key} missing from {element}", content_type=content_type
        )

    @staticmethod
    def _has_topic_and_partition_keys(topic_data, content_type):
        [ConsumerManager._assert_has_key(topic_data, k, content_type) for k in ["topic", "partition"]]

    @staticmethod
    def _topic_and_partition_valid(cluster_metadata, topic_data, content_type):
        ConsumerManager._has_topic_and_partition_keys(topic_data, content_type)
        topic = topic_data["topic"]
        partition = topic_data["partition"]
        ConsumerManager._assert(
            topic in cluster_metadata["topics"], code=404, sub_code=40401,
            message=f"topic {topic} not found", content_type=content_type
        )
        partitions = {pi["partition"] for pi in cluster_metadata["topics"][topic]["partitions"]}
        ConsumerManager._assert(
            partition in partitions, code=404, sub_code=40402,
            message=f"Partition {partition} not found for topic {topic}", content_type=content_type
        )

    @staticmethod
    def create_internal_name(group_name, consumer_name):
        return f"{group_name}_{consumer_name}"

    @staticmethod
    def _validate_create_consumer(request, content_type):
        _assert = partial(ConsumerManager._assert, content_type=content_type, code=422, sub_code=42204)
        _assert(cond="format" in request and request["format"] in KNOWN_FORMATS, message="format key is missing")
        request["consumer.request.timeout.ms"] = request.get(
            "consumer.request.timeout.ms", KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"]
        )
        for k in ["fetch.min.bytes", "consumer.request.timeout.ms"]:
            ConsumerManager._assert_positive_number(request, k, content_type, code=422, sub_code=42204)

        commit_enable = str(request["auto.commit.enable"]).lower()
        _assert(cond=commit_enable in ["true", "false"],
                message=f"auto.commit.enable has an invalid value: {commit_enable}")
        request["auto.commit.enable"] = commit_enable == "true"
        _assert(cond=request["auto.offset.reset"] in OFFSET_RESET_STRATEGIES,
                message=f"auto.offset.reset has an invalid value: {request['auto.offset.reset']}")

    @staticmethod
    def _illegal_state_fail(message, content_type):
        return ConsumerManager._assert(cond=False, code=409, sub_code=40903, content_type=content_type, message=message)

    # external api below
    # CONSUMER
    def create_consumer(self, group_name, request_data, content_type):
        consumer_name = request_data.get("name") or self.new_name()
        internal_name = self.create_internal_name(group_name, consumer_name)
        with self.consumer_locks[internal_name]:
            if internal_name in self.consumers:
                self.log.debug("")
                KarapaceBase.r(
                    status=409,
                    content_type=content_type,
                    body={"error_code": 40902, "message": f"Consumer {consumer_name} already exists"}
                )
            self._validate_create_consumer(request_data, content_type)
            c = KafkaConsumer(
                group_id=group_name,
                fetch_min_bytes=request_data["fetch.min.bytes"],
                request_timeout_ms=request_data["consumer.request.timeout.ms"],
                enable_auto_commit=request_data["auto.commit.enable"],
                auto_offset_reset=request_data["auto.offset.reset"]
            )
            self.consumers[internal_name] = TypedConsumer(consumer=c, serialization_format=request_data["format"])
            KarapaceBase.r(
                content_type=content_type,
                body={"base_uri": self.hostname, "instance_id": consumer_name}
            )

    def delete_consumer(self, internal_name, content_type):
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            try:
                c = self.consumers.pop(internal_name)
                c.close()
                self.consumer_locks.pop(internal_name)
            finally:
                KarapaceBase.r(status=204, content_type=content_type, body={})

    # OFFSETS
    def commit_offsets(self, internal_name, content_type, request_data, cluster_metadata):
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "offsets", content_type)
        payload = {}
        for el in request_data["offsets"]:
            # TODO -> Should we validate though ? IF the topic or partition is missing, the client will return None
            # TODO -> instead of a dict, which means we could potentially return a partial result and skip validation
            self._topic_and_partition_valid(cluster_metadata, el, content_type)
            payload[TopicPartition(el["topic"], el["partition"])] = OffsetAndMetadata(el["offset"])

        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            payload = payload or None
            try:
                consumer.commit(offsets=payload)
            except KafkaError as e:
                KarapaceBase.r(
                    body={"message": f"error sending commit request: {e}", "error_code": 50001},
                    content_type=content_type,
                    status=500
                )

    def get_offsets(self, internal_name, content_type, request_data, cluster_metadata):
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        for el in request_data["partitions"]:
            # TODO -> see validation Q above
            self._topic_and_partition_valid(cluster_metadata, el, content_type)
        response = {"offsets": []}
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            for el in request_data["partitions"]:
                tp = TopicPartition(el["topic"], el["partition"])
                commit_info = consumer.committed(tp, metadata=True)
                response["offsets"].append({
                    "topic": tp.topic, "partition": tp.partition,
                    "metadata": commit_info.metadata, "offset": commit_info.offset
                })
        KarapaceBase.r(body=response, content_type=content_type)

    # SUBSCRIPTION
    def set_subscription(self, internal_name, content_type, request_data):
        self._assert_consumer_exists(internal_name, content_type)
        topics = request_data.get("topics", [])
        topics_pattern = request_data.get("topic_pattern")
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            try:
                consumer.subscribe(topics=topics, pattern=topics_pattern)
                KarapaceBase.r(body={}, status=204, content_type=content_type)
            except AssertionError:
                self._illegal_state_fail("Neither topic_pattern nor topics are present in request", content_type)
            except IllegalStateError as e:
                self._illegal_state_fail(str(e), content_type=content_type)

    def get_subscription(self, group_name, consumer_name, content_type):
        internal_name = self.create_internal_name(group_name, consumer_name)
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            topics = self.consumers[internal_name].subscription()
            KarapaceBase.r(
                content_type=content_type,
                body={"topics": list(topics)}
            )

    def delete_subscription(self, internal_name, content_type):
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            self.consumers[internal_name].consumer.unsubscribe()

    # ASSIGNMENTS
    def set_assignments(self, internal_name, content_type, request_data):
        self._assert_consumer_exists(internal_name, content_type)
        self._assert_has_key(request_data, "partitions", content_type)
        partitions = []
        for el in request_data["partitions"]:
            self._has_topic_and_partition_keys(el, content_type)
            partitions.append(TopicPartition(el["topic"], el["partition"]))
        with self.consumer_locks[internal_name]:
            try:
                self.consumers[internal_name].consumer.assign(partitions)
                KarapaceBase.r(body={}, status=204, content_type=content_type)
            except IllegalStateError as e:
                self._illegal_state_fail(message=str(e), content_type=content_type)

    def get_assignments(self, internal_name, content_type):
        self._assert_consumer_exists(internal_name, content_type)
        with self.consumer_locks[internal_name]:
            consumer = self.consumers[internal_name].consumer
            KarapaceBase.r(
                content_type=content_type, status=200,
                body={"partitions": [{"topic": pd.topic, "partition": pd.partition} for pd in consumer.assignment()]}
            )

    # POSITIONS
    def seek_to(self, internal_name, content_type, request_data):
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

    def seek_reset(self, internal_name, content_type, request_data, beginning=True):
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
                KarapaceBase.r(body={}, status=204, content_type=content_type)
            except AssertionError:
                self._illegal_state_fail(f"Trying to reset unassigned partitions to {direction}", content_type)

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
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise


if __name__ == "__main__":
    sys.exit(main())
