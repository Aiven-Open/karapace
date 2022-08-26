from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from binascii import Error as B64DecodeError
from collections import namedtuple
from contextlib import AsyncExitStack, closing
from http import HTTPStatus
from kafka.errors import (
    BrokerResponseError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    NodeNotReadyError,
    TopicAuthorizationFailedError,
    UnknownTopicOrPartitionError,
)
from karapace.config import Config, create_client_ssl_context
from karapace.kafka_rest_apis.admin import KafkaRestAdminClient
from karapace.kafka_rest_apis.consumer_manager import ConsumerManager
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.karapace import KarapaceBase
from karapace.rapu import HTTPRequest, HTTPResponse, JSON_CONTENT_TYPE
from karapace.schema_reader import SchemaType
from karapace.serialization import InvalidMessageSchema, InvalidPayload, SchemaRegistrySerializer, SchemaRetrievalError
from karapace.utils import convert_to_int, json_encode, KarapaceKafkaClient
from typing import Dict, List, Optional, Tuple

import aiohttp.web
import asyncio
import base64
import json
import logging
import time

RECORD_KEYS = ["key", "value", "partition"]
PUBLISH_KEYS = {"records", "value_schema", "value_schema_id", "key_schema", "key_schema_id"}
RECORD_CODES = [42201, 42202]
KNOWN_FORMATS = {"json", "avro", "protobuf", "binary"}
OFFSET_RESET_STRATEGIES = {"latest", "earliest"}
SCHEMA_MAPPINGS = {"avro": SchemaType.AVRO, "jsonschema": SchemaType.JSONSCHEMA, "protobuf": SchemaType.PROTOBUF}
TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format", "config"])

log = logging.getLogger(__name__)


class FormatError(Exception):
    pass


class KafkaRest(KarapaceBase):
    def __init__(self, config: Config) -> None:
        super().__init__(config=config)
        self._add_kafka_rest_routes()
        self.serializer = SchemaRegistrySerializer(config=config)
        self.proxies: Dict[str, "UserRestProxy"] = {}
        log.info("REST proxy starting with (delegated authorization=%s)", self.config.get("rest_authorization", False))

    async def close(self) -> None:
        async with AsyncExitStack() as stack:
            stack.push_async_callback(super().close)
            stack.push_async_callback(self.serializer.close)

            for proxy in self.proxies.values():
                stack.push_async_callback(proxy.aclose)

    def _add_kafka_rest_routes(self) -> None:
        # Brokers
        self.route(
            "/brokers",
            callback=self.list_brokers,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )

        # Consumers
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.commit_consumer_offsets,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.get_consumer_offsets,
            method="GET",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.update_consumer_subscription,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.get_consumer_subscription,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.delete_consumer_subscription,
            method="DELETE",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.update_consumer_assignment,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.get_consumer_assignment,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/beginning",
            callback=self.seek_beginning_consumer_offsets,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/end",
            callback=self.seek_end_consumer_offsets,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions",
            callback=self.update_consumer_offsets,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/records",
            callback=self.fetch,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/consumers/<group_name:path>",
            callback=self.create_consumer,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>",
            callback=self.delete_consumer,
            method="DELETE",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        # Partitions
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>/offsets",
            callback=self.partition_offsets,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.partition_details,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.partition_publish,
            method="POST",
            rest_request=True,
            with_request=True,
        )
        self.route(
            "/topics/<topic:path>/partitions",
            callback=self.list_partitions,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        # Topics
        self.route(
            "/topics",
            callback=self.list_topics,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/topics/<topic:path>",
            callback=self.topic_details,
            method="GET",
            rest_request=True,
            with_request=True,
            json_body=False,
        )
        self.route("/topics/<topic:path>", callback=self.topic_publish, method="POST", rest_request=True, with_request=True)

    def get_user_proxy(self, request: HTTPRequest) -> "UserRestProxy":
        key = ""
        try:
            if self.config.get("rest_authorization", False):
                auth_header = request.headers.get("Authorization")

                if auth_header is None:
                    raise HTTPResponse(
                        body='{"message": "Unauthorized"}',
                        status=HTTPStatus.UNAUTHORIZED,
                        content_type=JSON_CONTENT_TYPE,
                        headers={"WWW-Authenticate": 'Basic realm="Karapace REST Proxy"'},
                    )
                key = auth_header
                if self.proxies.get(key) is None:
                    auth = aiohttp.BasicAuth.decode(auth_header)
                    config = self.config.copy()
                    config["bootstrap_uri"] = config["sasl_bootstrap_uri"]
                    config["security_protocol"] = (
                        "SASL_SSL" if config["security_protocol"] in ("SSL", "SASL_SSL") else "SASL_PLAINTEXT"
                    )
                    config["sasl_mechanism"] = "PLAIN"
                    config["sasl_plain_username"] = auth.login
                    config["sasl_plain_password"] = auth.password
                    self.proxies[key] = UserRestProxy(config, self.kafka_timeout, self.serializer)
            else:
                if self.proxies.get(key) is None:
                    self.proxies[key] = UserRestProxy(self.config, self.kafka_timeout, self.serializer)
        except NoBrokersAvailable:
            # This can be caused also due misconfigration, but kafka-python's
            # KafkaAdminClient cannot currently distinguish those two cases
            log.exception("Failed to connect to Kafka with the credentials")
            self.r(body={"message": "Forbidden"}, content_type=JSON_CONTENT_TYPE, status=HTTPStatus.FORBIDDEN)
        return self.proxies[key]

    async def list_brokers(self, content_type: str, *, request: HTTPRequest):
        proxy = self.get_user_proxy(request)
        await proxy.list_brokers(content_type)

    async def commit_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.commit_consumer_offsets(group_name, instance, content_type, request=request)

    async def get_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.get_consumer_offsets(group_name, instance, content_type, request=request)

    async def update_consumer_subscription(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.update_consumer_subscription(group_name, instance, content_type, request=request)

    async def get_consumer_subscription(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.get_consumer_subscription(group_name, instance, content_type)

    async def delete_consumer_subscription(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.delete_consumer_subscription(group_name, instance, content_type)

    async def update_consumer_assignment(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.update_consumer_assignment(group_name, instance, content_type, request=request)

    async def get_consumer_assignment(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.get_consumer_assignment(group_name, instance, content_type)

    async def seek_beginning_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.seek_beginning_consumer_offsets(group_name, instance, content_type, request=request)

    async def seek_end_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.seek_end_consumer_offsets(group_name, instance, content_type, request=request)

    async def update_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.update_consumer_offsets(group_name, instance, content_type, request=request)

    async def fetch(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.fetch(group_name, instance, content_type, request=request)

    async def create_consumer(self, group_name: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.create_consumer(group_name, content_type, request=request)

    async def delete_consumer(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.delete_consumer(group_name, instance, content_type)

    async def partition_offsets(self, content_type: str, *, topic: str, partition_id: str, request: HTTPRequest):
        proxy = self.get_user_proxy(request)
        await proxy.partition_offsets(content_type, topic=topic, partition_id=partition_id)

    async def partition_details(self, content_type: str, *, topic: str, partition_id: str, request: HTTPRequest):
        proxy = self.get_user_proxy(request)
        await proxy.partition_details(content_type, topic=topic, partition_id=partition_id)

    async def partition_publish(self, topic: str, partition_id: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.partition_publish(topic, partition_id, content_type, request=request)

    async def list_partitions(self, content_type: str, *, topic: str, request: HTTPRequest):
        proxy = self.get_user_proxy(request)
        await proxy.list_partitions(content_type, topic=topic)

    async def list_topics(self, content_type: str, *, request: HTTPRequest):
        proxy = self.get_user_proxy(request)
        await proxy.list_topics(content_type)

    async def topic_details(self, content_type: str, *, topic: str, request: HTTPRequest):
        proxy = self.get_user_proxy(request)
        await proxy.topic_details(content_type, topic=topic)

    async def topic_publish(self, topic: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = self.get_user_proxy(request)
        await proxy.topic_publish(topic, content_type, request=request)


class UserRestProxy:
    def __init__(self, config: Config, kafka_timeout: int, serializer):
        self.config = config
        self.kafka_timeout = kafka_timeout
        self.serializer = serializer
        self._cluster_metadata = None
        self._cluster_metadata_complete = False
        self._metadata_birth = None
        self.metadata_max_age = self.config["admin_metadata_max_age"]
        self.admin_client = None
        self.admin_lock = asyncio.Lock()
        self.metadata_cache = None
        self.schemas_cache = {}
        self.consumer_manager = ConsumerManager(config=config, deserializer=self.serializer)
        self.init_admin_client()

        self._async_producer_lock = asyncio.Lock()
        self._async_producer: Optional[AIOKafkaProducer] = None

    async def _maybe_create_async_producer(self) -> AIOKafkaProducer:
        if self._async_producer is not None:
            return self._async_producer

        if self.config["producer_acks"] == "all":
            acks = "all"
        else:
            acks = int(self.config["producer_acks"])

        async with self._async_producer_lock:
            for retry in [True, True, False]:
                if self._async_producer is not None:
                    break

                log.info("Creating async producer")

                # Don't retry if creating the SSL context fails, likely a configuration issue with
                # ciphers or certificate chains
                ssl_context = create_client_ssl_context(self.config)

                # Don't retry if instantiating the producer fails, likely a configuration error.
                producer = AIOKafkaProducer(
                    bootstrap_servers=self.config["bootstrap_uri"],
                    security_protocol=self.config["security_protocol"],
                    ssl_context=ssl_context,
                    sasl_mechanism=self.config["sasl_mechanism"],
                    sasl_plain_username=self.config["sasl_plain_username"],
                    sasl_plain_password=self.config["sasl_plain_password"],
                    metadata_max_age_ms=self.config["metadata_max_age_ms"],
                    acks=acks,
                    compression_type=self.config["producer_compression_type"],
                    linger_ms=self.config["producer_linger_ms"],
                    connections_max_idle_ms=self.config["connections_max_idle_ms"],
                )

                try:
                    await producer.start()
                except KafkaConnectionError:
                    if retry:
                        log.exception("Unable to connect to the bootstrap servers, retrying")
                    else:
                        log.exception("Giving up after trying to connect to the bootstrap servers")
                        raise
                    await asyncio.sleep(1)
                else:
                    self._async_producer = producer

        return self._async_producer

    async def create_consumer(self, group_name: str, content_type: str, *, request: HTTPRequest) -> None:
        await self.consumer_manager.create_consumer(group_name, request.json, content_type)

    async def delete_consumer(self, group_name: str, instance: str, content_type: str) -> None:
        await self.consumer_manager.delete_consumer(ConsumerManager.create_internal_name(group_name, instance), content_type)

    async def commit_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        await self.consumer_manager.commit_offsets(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
            request.json,
            await self.cluster_metadata(),
        )

    async def get_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        await self.consumer_manager.get_offsets(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json
        )

    async def update_consumer_subscription(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        await self.consumer_manager.set_subscription(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
            request.json,
        )

    async def get_consumer_subscription(self, group_name: str, instance: str, content_type: str) -> None:
        await self.consumer_manager.get_subscription(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
        )

    async def delete_consumer_subscription(self, group_name: str, instance: str, content_type: str) -> None:
        await self.consumer_manager.delete_subscription(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
        )

    async def update_consumer_assignment(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        await self.consumer_manager.set_assignments(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json
        )

    async def get_consumer_assignment(self, group_name: str, instance: str, content_type: str) -> None:
        await self.consumer_manager.get_assignments(
            ConsumerManager.create_internal_name(group_name, instance),
            content_type,
        )

    async def update_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        await self.consumer_manager.seek_to(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json
        )

    async def seek_end_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        await self.consumer_manager.seek_limit(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json, beginning=False
        )

    async def seek_beginning_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        await self.consumer_manager.seek_limit(
            ConsumerManager.create_internal_name(group_name, instance), content_type, request.json, beginning=True
        )

    async def fetch(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        await self.consumer_manager.fetch(
            internal_name=ConsumerManager.create_internal_name(group_name, instance),
            content_type=content_type,
            query_params=request.query,
            formats=request.accepts,
        )

    # OFFSETS
    async def get_offsets(self, topic: str, partition_id: int) -> dict:
        async with self.admin_lock:
            return self.admin_client.get_offsets(topic, partition_id)

    async def get_topic_config(self, topic: str) -> dict:
        async with self.admin_lock:
            return self.admin_client.get_topic_config(topic)

    async def cluster_metadata(self, topics: Optional[List[str]] = None) -> dict:
        async with self.admin_lock:
            if self._metadata_birth is None or time.monotonic() - self._metadata_birth > self.metadata_max_age:
                self._cluster_metadata = None

            if self._cluster_metadata:
                # Return from metadata only if all queried topics have cached metadata
                if topics is None:
                    if self._cluster_metadata_complete:
                        return self._cluster_metadata
                elif all(topic in self._cluster_metadata["topics"] for topic in topics):
                    return {
                        **self._cluster_metadata,
                        "topics": {topic: self._cluster_metadata["topics"][topic] for topic in topics},
                    }

            try:
                metadata_birth = time.monotonic()
                metadata = self.admin_client.cluster_metadata(topics)
                self._metadata_birth = metadata_birth
                self._cluster_metadata = metadata
                self._cluster_metadata_complete = topics is None
            except NodeNotReadyError:
                log.exception("Could not refresh cluster metadata")
                KafkaRest.r(
                    body={
                        "message": "Kafka node not ready",
                        "code": RESTErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    },
                    content_type="application/json",
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return metadata

    def init_admin_client(self):
        for retry in [True, True, False]:
            try:
                self.admin_client = KafkaRestAdminClient(
                    bootstrap_servers=self.config["bootstrap_uri"],
                    security_protocol=self.config["security_protocol"],
                    ssl_cafile=self.config["ssl_cafile"],
                    ssl_certfile=self.config["ssl_certfile"],
                    ssl_keyfile=self.config["ssl_keyfile"],
                    sasl_mechanism=self.config["sasl_mechanism"],
                    sasl_plain_username=self.config["sasl_plain_username"],
                    sasl_plain_password=self.config["sasl_plain_password"],
                    api_version=(1, 0, 0),
                    metadata_max_age_ms=self.config["metadata_max_age_ms"],
                    connections_max_idle_ms=self.config["connections_max_idle_ms"],
                    kafka_client=KarapaceKafkaClient,
                )
                break
            except:  # pylint: disable=bare-except
                if retry:
                    log.exception("Unable to start admin client, retrying")
                else:
                    log.exception("Giving up after failing to start admin client")
                    raise
                time.sleep(1)

    async def aclose(self) -> None:
        async with AsyncExitStack() as stack, self._async_producer_lock:
            if self._async_producer is not None:
                log.info("Disposing async producer")
                stack.push_async_callback(self._async_producer.stop)

            if self.admin_client is not None:
                stack.enter_context(closing(self.admin_client))

            if self.consumer_manager is not None:
                stack.push_async_callback(self.consumer_manager.aclose)

            self.admin_client = None
            self.consumer_manager = None

    async def publish(self, topic: str, partition_id: Optional[str], content_type: str, request: HTTPRequest) -> None:
        formats: dict = request.content_type
        data: dict = request.json
        _ = await self.get_topic_info(topic, content_type)
        if partition_id is not None:
            _ = await self.get_partition_info(topic, partition_id, content_type)
            partition_id = int(partition_id)
        for k in ["key_schema_id", "value_schema_id"]:
            convert_to_int(data, k, content_type)
        await self.validate_publish_request_format(data, formats, content_type, topic)
        status = HTTPStatus.OK
        ser_format = formats["embedded_format"]
        prepared_records = []
        try:
            prepared_records = await self._prepare_records(
                content_type=content_type,
                data=data,
                ser_format=ser_format,
                key_schema_id=data.get("key_schema_id"),
                value_schema_id=data.get("value_schema_id"),
                default_partition=partition_id,
            )
        except (FormatError, B64DecodeError):
            KafkaRest.unprocessable_entity(
                message=f"Request includes data improperly formatted given the format {ser_format}",
                content_type=content_type,
                sub_code=RESTErrorCodes.INVALID_DATA.value,
            )
        except InvalidMessageSchema as e:
            KafkaRest.r(
                body={"error_code": RESTErrorCodes.INVALID_DATA.value, "message": str(e)},
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        except SchemaRetrievalError as e:
            KafkaRest.r(
                body={"error_code": RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value, "message": str(e)},
                content_type=content_type,
                status=HTTPStatus.REQUEST_TIMEOUT,
            )
        response = {
            "key_schema_id": data.get("key_schema_id"),
            "value_schema_id": data.get("value_schema_id"),
            "offsets": [],
        }

        publish_results = await self.produce_messages(topic=topic, prepared_records=prepared_records)
        for publish_result in publish_results:
            if "error" in publish_result and status == HTTPStatus.OK:
                status = HTTPStatus.UNPROCESSABLE_ENTITY
            response["offsets"].append(publish_result)
        KafkaRest.r(body=response, content_type=content_type, status=status)

    async def partition_publish(self, topic: str, partition_id: str, content_type: str, *, request: HTTPRequest) -> None:
        log.debug("Executing partition publish on topic %s and partition %s", topic, partition_id)
        await self.publish(topic, partition_id, content_type, request)

    async def topic_publish(self, topic: str, content_type: str, *, request: HTTPRequest) -> None:
        log.debug("Executing topic publish on topic %s", topic)
        await self.publish(topic, None, content_type, request)

    @staticmethod
    def validate_partition_id(partition_id: str, content_type: str) -> int:  # pylint: disable=inconsistent-return-statements
        try:
            return int(partition_id)
        except ValueError:
            KafkaRest.not_found(
                message=f"Partition {partition_id} not found",
                content_type=content_type,
                sub_code=RESTErrorCodes.HTTP_NOT_FOUND.value,
            )

    @staticmethod
    def is_valid_schema_request(data: dict, prefix: str) -> bool:
        schema_id = data.get(f"{prefix}_schema_id")
        schema = data.get(f"{prefix}_schema")
        if schema_id:
            try:
                int(schema_id)
                return True
            except (TypeError, ValueError):
                return False
        return isinstance(schema, str)

    async def get_schema_id(self, data: dict, topic: str, prefix: str, schema_type: SchemaType) -> int:
        log.debug("Retrieving schema id for %r", data)
        if f"{prefix}_schema_id" in data and data[f"{prefix}_schema_id"] is not None:
            log.debug("Will use schema id %d for serializing %s on topic %s", data[f"{prefix}_schema_id"], prefix, topic)
            return int(data[f"{prefix}_schema_id"])
        schema_str = data[f"{prefix}_schema"]
        log.debug("Registering / Retrieving ID for schema %s", schema_str)
        if schema_str not in self.schemas_cache:
            subject_name = self.serializer.get_subject_name(topic, data[f"{prefix}_schema"], prefix, schema_type)
            schema_id = await self.serializer.get_id_for_schema(data[f"{prefix}_schema"], subject_name, schema_type)
            self.schemas_cache[schema_str] = schema_id
        return self.schemas_cache[schema_str]

    async def validate_schema_info(self, data: dict, prefix: str, content_type: str, topic: str, schema_type: str):
        try:
            schema_type = SCHEMA_MAPPINGS[schema_type]
        except KeyError:
            KafkaRest.r(
                body={
                    "error_code": RESTErrorCodes.HTTP_NOT_FOUND.value,
                    "message": f"Unknown schema type {schema_type}",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        # will do in place updates of id keys, since calling these twice would be expensive
        try:
            data[f"{prefix}_schema_id"] = await self.get_schema_id(data, topic, prefix, schema_type)
        except InvalidPayload:
            log.exception("Unable to retrieve schema id")
            KafkaRest.r(
                body={
                    "error_code": RESTErrorCodes.HTTP_BAD_REQUEST.value,
                    "message": "Invalid schema string",
                },
                content_type=content_type,
                status=HTTPStatus.BAD_REQUEST,
            )
        except SchemaRetrievalError:
            KafkaRest.r(
                body={
                    "error_code": RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value,
                    "message": f"Error when registering schema. format = {schema_type}, subject = {topic}-{prefix}",
                },
                content_type=content_type,
                status=HTTPStatus.REQUEST_TIMEOUT,
            )

    async def _prepare_records(
        self,
        content_type: str,
        data: dict,
        ser_format: str,
        key_schema_id: Optional[int],
        value_schema_id: Optional[int],
        default_partition: Optional[int] = None,
    ) -> List[Tuple]:
        prepared_records = []
        for record in data["records"]:
            key = record.get("key")
            value = record.get("value")
            key = await self.serialize(content_type, key, ser_format, key_schema_id)
            value = await self.serialize(content_type, value, ser_format, value_schema_id)
            prepared_records.append((key, value, record.get("partition", default_partition)))
        return prepared_records

    async def get_partition_info(self, topic: str, partition: str, content_type: str) -> dict:
        partition = self.validate_partition_id(partition, content_type)
        try:
            topic_data = await self.get_topic_info(topic, content_type)
            partitions = topic_data["partitions"]
            for p in partitions:
                if p["partition"] == partition:
                    return p
            KafkaRest.not_found(
                message=f"Partition {partition} not found",
                content_type=content_type,
                sub_code=RESTErrorCodes.PARTITION_NOT_FOUND.value,
            )
        except (KeyError, UnknownTopicOrPartitionError, TopicAuthorizationFailedError):
            KafkaRest.not_found(
                message=f"Topic {topic} not found",
                content_type=content_type,
                sub_code=RESTErrorCodes.TOPIC_NOT_FOUND.value,
            )
        return {}

    async def get_topic_info(self, topic: str, content_type: str) -> dict:
        try:
            metadata = await self.cluster_metadata([topic])
            return metadata["topics"][topic]
        except (KeyError, UnknownTopicOrPartitionError, TopicAuthorizationFailedError):
            KafkaRest.not_found(
                message=f"Topic {topic} not found",
                content_type=content_type,
                sub_code=RESTErrorCodes.TOPIC_NOT_FOUND.value,
            )

    @staticmethod
    def all_empty(data: dict, key: str) -> bool:
        return all(key not in item or item[key] is None for item in data["records"])

    async def serialize(
        self,
        content_type: str,
        obj=None,
        ser_format: Optional[str] = None,
        schema_id: Optional[int] = None,
    ) -> bytes:
        if not obj:
            return b""
        # not pretty
        if ser_format == "json":
            # TODO -> get encoding from headers
            return json.dumps(obj).encode("utf8")
        if ser_format == "binary":
            try:
                return base64.b64decode(obj)
            except:  # pylint: disable=bare-except
                KafkaRest.r(
                    body={
                        "error_code": RESTErrorCodes.HTTP_BAD_REQUEST.value,
                        "message": f"data={json_encode(obj, sort_keys=False, compact=True)}  is not a base64 string.",
                    },
                    content_type=content_type,
                    status=HTTPStatus.BAD_REQUEST,
                )
        if ser_format in {"avro", "jsonschema", "protobuf"}:
            return await self.schema_serialize(obj, schema_id)
        raise FormatError(f"Unknown format: {ser_format}")

    async def schema_serialize(self, obj: dict, schema_id: Optional[int]) -> bytes:
        schema = await self.serializer.get_schema_for_id(schema_id)
        bytes_ = await self.serializer.serialize(schema, obj)
        return bytes_

    async def validate_publish_request_format(self, data: dict, formats: dict, content_type: str, topic: str):
        # this method will do in place updates for binary embedded formats, because the validation itself
        # is equivalent to a parse / attempt to parse operation

        # disallow missing or non empty 'records' key , plus any other keys
        if "records" not in data or set(data.keys()).difference(PUBLISH_KEYS) or not data["records"]:
            KafkaRest.unprocessable_entity(
                message="Invalid request format",
                content_type=content_type,
                sub_code=RESTErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
            )
        if not isinstance(data["records"], list):
            KafkaRest.r(
                body={
                    "error_code": RESTErrorCodes.HTTP_BAD_REQUEST.value,
                    "message": "'records' must be an array",
                },
                content_type=content_type,
                status=HTTPStatus.BAD_REQUEST,
            )
        for r in data["records"]:
            if not isinstance(r, dict):
                KafkaRest.r(
                    body={
                        "error_code": RESTErrorCodes.HTTP_BAD_REQUEST.value,
                        "message": "Produce record must be an object",
                    },
                    content_type=content_type,
                    status=HTTPStatus.BAD_REQUEST,
                )
            convert_to_int(r, "partition", content_type)
            if set(r.keys()).difference(RECORD_KEYS):
                KafkaRest.unprocessable_entity(
                    message="Invalid request format",
                    content_type=content_type,
                    sub_code=RESTErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                )
        # disallow missing id and schema for any key/value list that has at least one populated element
        if formats["embedded_format"] in {"avro", "jsonschema", "protobuf"}:
            for prefix, code in zip(RECORD_KEYS, RECORD_CODES):
                if self.all_empty(data, prefix):
                    continue
                if not self.is_valid_schema_request(data, prefix):
                    KafkaRest.unprocessable_entity(
                        message=f"Request includes {prefix}s and uses a format that requires schemas "
                        f"but does not include the {prefix}_schema or {prefix}_schema_id fields",
                        content_type=content_type,
                        sub_code=code,
                    )
                try:
                    await self.validate_schema_info(data, prefix, content_type, topic, formats["embedded_format"])
                except InvalidMessageSchema as e:
                    KafkaRest.unprocessable_entity(
                        message=str(e),
                        content_type=content_type,
                        sub_code=RESTErrorCodes.INVALID_DATA.value,
                    )

    async def produce_messages(self, *, topic: str, prepared_records: List) -> List:
        producer = await self._maybe_create_async_producer()

        produce_futures = []
        for key, value, partition in prepared_records:
            # Cancelling the returned future **will not** stop event from being sent, but cancelling
            # the ``send`` coroutine itself **will**.
            coroutine = producer.send(topic, key=key, value=value, partition=partition)

            # Schedule the co-routine, it will be cancelled if is not complete in
            # `kafka_timeout` seconds.
            future = asyncio.wait_for(fut=coroutine, timeout=self.kafka_timeout)
            produce_futures.append(future)

        # Gather the results of `asyncio.wait_for`
        send_results = []
        for result in await asyncio.gather(*produce_futures, return_exceptions=True):
            if not isinstance(result, Exception):
                send_results.append(result)
            else:
                completed_exception_future = asyncio.Future()
                completed_exception_future.set_exception(result)
                send_results.append(completed_exception_future)

        # Gather the results from Kafka producer `send`
        produce_results = []
        for result in await asyncio.gather(*send_results, return_exceptions=True):
            if not isinstance(result, Exception):
                produce_results.append(
                    {
                        "offset": result.offset if result else -1,
                        "partition": result.topic_partition.partition if result else 0,
                    }
                )

            # Exceptions below are raised before data is sent to Kafka
            elif isinstance(result, asyncio.TimeoutError):
                log.warning("Timed out waiting for publisher buffer", exc_info=result)
                # timeouts are retriable
                produce_results.append(
                    {"error_code": 1, "error": "timed out waiting to publish message, producer buffer full"}
                )
            elif isinstance(result, AssertionError):
                log.error("Invalid data", exc_info=result)
                produce_results.append({"error_code": 1, "error": str(result)})

            # Exceptions below are raised after data is sent to Kafka
            elif isinstance(result, KafkaTimeoutError):
                log.warning("Timed out waiting for publisher", exc_info=result)
                # timeouts are retriable
                produce_results.append({"error_code": 1, "error": "timed out waiting to publish message"})
            elif isinstance(result, asyncio.CancelledError):
                log.warning("Async task cancelled", exc_info=result)
                # cancel is retriable
                produce_results.append({"error_code": 1, "error": "Publish message cancelled"})
            elif isinstance(result, BrokerResponseError):
                log.error("Broker error", exc_info=result)
                resp = {"error_code": 1, "error": result.description}
                if hasattr(result, "retriable") and result.retriable:
                    resp["error_code"] = 2
                produce_results.append(resp)
            else:
                log.error("Unexpected exception", exc_info=result)
                produce_results.append({"error_code": 1, "error": str(result)})

        return produce_results

    async def list_topics(self, content_type: str):
        metadata = await self.cluster_metadata()
        topics = list(metadata["topics"].keys())
        KafkaRest.r(topics, content_type)

    async def topic_details(self, content_type: str, *, topic: str):
        log.info("Retrieving topic details for %s", topic)
        try:
            metadata = await self.cluster_metadata([topic])
            config = await self.get_topic_config(topic)
            if topic not in metadata["topics"]:
                KafkaRest.not_found(
                    message=f"Topic {topic} not found",
                    content_type=content_type,
                    sub_code=RESTErrorCodes.TOPIC_NOT_FOUND.value,
                )
            data = metadata["topics"][topic]
            data["name"] = topic
            data["configs"] = config
            KafkaRest.r(data, content_type)
        except (UnknownTopicOrPartitionError, TopicAuthorizationFailedError):
            # Respond 'not found' to TopicAuthorizationFailedError to avoid leaking information of topics
            KafkaRest.not_found(
                message=f"Topic {topic} not found",
                content_type=content_type,
                sub_code=RESTErrorCodes.UNKNOWN_TOPIC_OR_PARTITION.value,
            )

    async def list_partitions(self, content_type: str, *, topic: str):
        log.info("Retrieving partition details for topic %s", topic)
        try:
            metadata = await self.cluster_metadata([topic])
            topic_details = metadata["topics"]
            KafkaRest.r(topic_details[topic]["partitions"], content_type)
        except (UnknownTopicOrPartitionError, TopicAuthorizationFailedError, KeyError):
            KafkaRest.not_found(
                message=f"Topic {topic} not found",
                content_type=content_type,
                sub_code=RESTErrorCodes.TOPIC_NOT_FOUND.value,
            )

    async def partition_details(self, content_type: str, *, topic: str, partition_id: str):
        log.info("Retrieving partition details for topic %s and partition %s", topic, partition_id)
        p = await self.get_partition_info(topic, partition_id, content_type)
        KafkaRest.r(p, content_type)

    async def partition_offsets(self, content_type: str, *, topic: str, partition_id: str):
        log.info("Retrieving partition offsets for topic %s and partition %s", topic, partition_id)
        partition_id = self.validate_partition_id(partition_id, content_type)
        try:
            KafkaRest.r(await self.get_offsets(topic, partition_id), content_type)
        except UnknownTopicOrPartitionError as e:
            # Do a topics request on failure, figure out faster ways once we get correctness down
            metadata = await self.cluster_metadata()
            if topic not in metadata["topics"]:
                KafkaRest.not_found(
                    message=f"Topic {topic} not found: {e}",
                    content_type=content_type,
                    sub_code=RESTErrorCodes.TOPIC_NOT_FOUND.value,
                )
            KafkaRest.not_found(
                message=f"Partition {partition_id} not found: {e}",
                content_type=content_type,
                sub_code=RESTErrorCodes.PARTITION_NOT_FOUND.value,
            )

    async def list_brokers(self, content_type: str):
        metadata = await self.cluster_metadata()
        metadata = metadata.copy()  # shallow copy as we want to mutate it
        metadata.pop("topics")
        KafkaRest.r(metadata, content_type)
