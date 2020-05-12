from .admin import KafkaRestAdminClient
from .consumer_manager import ConsumerManager
from binascii import Error as B64DecodeError
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from kafka.errors import BrokerResponseError, KafkaTimeoutError, UnknownTopicOrPartitionError
from karapace.karapace import KarapaceBase
from karapace.rapu import HTTPRequest
from karapace.serialization import InvalidMessageSchema, InvalidPayload, SchemaRegistrySerializer, SchemaRetrievalError
from threading import Lock
from typing import List, Optional, Tuple

import asyncio
import base64
import copy
import json
import logging
import time

RECORD_KEYS = ["key", "value"]
PUBLISH_KEYS = {"records", "value_schema", "value_schema_id", "key_schema", "key_schema_id"}
RECORD_CODES = [42201, 42202]
KNOWN_FORMATS = {"json", "avro", "binary"}
OFFSET_RESET_STRATEGIES = {"latest", "earliest"}

TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format", "config"])


class FormatError(Exception):
    pass


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

    def cluster_metadata(self, topics: Optional[List[str]] = None) -> dict:
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
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None
        if self.consumer_manager:
            self.consumer_manager.close()
            self.consumer_manager = None

    async def publish(self, topic: str, partition_id: Optional[str], content_type: str, formats: dict, data: dict):
        _ = self.get_topic_info(topic, content_type)
        if partition_id is not None:
            _ = self.get_partition_info(topic, partition_id, content_type)
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
        except (FormatError, B64DecodeError):
            self.unprocessable_entity(
                message=f"Request includes data improperly formatted given the format {ser_format}",
                content_type=content_type,
                sub_code=42205
            )
        except InvalidMessageSchema as e:
            self.r(body={"error_code": 42205, "message": str(e)}, content_type=content_type, status=422)
        except SchemaRetrievalError as e:
            self.r(body={"error_code": 40801, "message": str(e)}, content_type=content_type, status=408)
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
    def validate_partition_id(partition_id: str, content_type: str) -> int:
        try:
            return int(partition_id)
        except ValueError:
            KafkaRest.not_found(message=f"Partition {partition_id} not found", content_type=content_type, sub_code=404)

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
        partition = self.validate_partition_id(partition, content_type)
        try:
            topic_data = self.get_topic_info(topic, content_type)
            partitions = topic_data["partitions"]
            for p in partitions:
                if p["partition"] == partition:
                    return p
            self.not_found(message=f"Partition {partition} not found", content_type=content_type, sub_code=40402)
        except UnknownTopicOrPartitionError:
            self.not_found(message=f"Partition {partition} not found", content_type=content_type, sub_code=40402)
        except KeyError:
            self.not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
        return {}

    def get_topic_info(self, topic: str, content_type: str) -> dict:
        md = self.cluster_metadata()["topics"]
        if topic not in md:
            self.not_found(
                message=f"Topic {topic} not found in {list(md.keys())}", content_type=content_type, sub_code=40401
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
            return base64.b64decode(obj)
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

        # disallow missing or non empty 'records' key , plus any other keys
        if "records" not in data or set(data.keys()).difference(PUBLISH_KEYS) or not data["records"]:
            self.unprocessable_entity(message="Invalid request format", content_type=content_type, sub_code=422)
        for r in data["records"]:
            if set(r.keys()).difference(RECORD_KEYS):
                self.unprocessable_entity(message=f"Invalid request format", content_type=content_type, sub_code=422)
        # disallow missing id and schema for any key/value list that has at least one populated element
        if formats["embedded_format"] == "avro":
            for prefix, code in zip(RECORD_KEYS, RECORD_CODES):
                if self.all_empty(data, prefix):
                    continue
                if not self.is_valid_avro_request(data, prefix):
                    self.unprocessable_entity(
                        message=f"Request includes {prefix}s and uses a format that requires schemas "
                        f"but does not include the {prefix}_schema or {prefix}_schema_id fields",
                        content_type=content_type,
                        sub_code=code
                    )
                try:
                    await self.validate_schema_info(data, prefix, content_type, topic)
                except InvalidMessageSchema as e:
                    self.unprocessable_entity(message=str(e), content_type=content_type, sub_code=42205)

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
                self.not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
            data = metadata["topics"][topic]
            data["name"] = topic
            data["configs"] = config
            self.r(data, content_type)
        except UnknownTopicOrPartitionError:
            self.not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)

    def list_partitions(self, content_type: str, *, topic: str):
        try:
            topic_details = self.cluster_metadata([topic])["topics"]
            self.r(topic_details[topic]["partitions"], content_type)
        except (UnknownTopicOrPartitionError, KeyError):
            self.not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)

    def partition_details(self, content_type: str, *, topic: str, partition_id: str):
        p = self.get_partition_info(topic, partition_id, content_type)
        self.r(p, content_type)

    def partition_offsets(self, content_type: str, *, topic: str, partition_id: str):
        partition_id = self.validate_partition_id(partition_id, content_type)
        try:
            self.r(self.get_offsets(topic, partition_id), content_type)
        except UnknownTopicOrPartitionError:
            # Do a topics request on failure, figure out faster ways once we get correctness down
            if topic not in self.cluster_metadata()["topics"]:
                self.not_found(message=f"Topic {topic} not found", content_type=content_type, sub_code=40401)
            self.not_found(message=f"Partition {partition_id} not found", content_type=content_type, sub_code=40402)

    def list_brokers(self, content_type: str):
        metadata = self.cluster_metadata()
        metadata.pop("topics")
        self.r(metadata, content_type)
