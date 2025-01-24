"""
karapace - Rest Proxy API

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiokafka.errors import (
    AuthenticationFailedError,
    BrokerResponseError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    TopicAuthorizationFailedError,
    UnknownTopicOrPartitionError,
)
from binascii import Error as B64DecodeError
from collections import namedtuple
from collections.abc import Callable
from confluent_kafka.error import KafkaException
from contextlib import AsyncExitStack
from http import HTTPStatus
from karapace.core.config import Config
from karapace.core.errors import InvalidSchema
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka.producer import AsyncKafkaProducer
from karapace.core.kafka_rest_apis.authentication import (
    get_auth_config_from_header,
    get_expiration_time_from_header,
    get_kafka_client_auth_parameters_from_config,
)
from karapace.core.kafka_rest_apis.consumer_manager import ConsumerManager
from karapace.core.kafka_rest_apis.error_codes import RESTErrorCodes
from karapace.core.kafka_rest_apis.schema_cache import TopicSchemaCache
from karapace.core.karapace import KarapaceBase
from karapace.core.rapu import HTTPRequest, JSON_CONTENT_TYPE
from karapace.core.schema_models import TypedSchema, ValidatedTypedSchema
from karapace.core.schema_type import SchemaType
from karapace.core.serialization import (
    get_subject_name,
    InvalidMessageSchema,
    InvalidPayload,
    SchemaRegistrySerializer,
    SchemaRetrievalError,
)
from karapace.core.typing import NameStrategy, SchemaId, Subject, SubjectType
from karapace.core.utils import convert_to_int, json_encode
from typing import TypedDict

import asyncio
import base64
import datetime
import logging
import time

SUBJECT_VALID_POSTFIX = [SubjectType.key, SubjectType.value]
PUBLISH_KEYS = {"records", "value_schema", "value_schema_id", "key_schema", "key_schema_id"}
RECORD_CODES = [42201, 42202]
KNOWN_FORMATS = {"json", "avro", "protobuf", "binary"}
OFFSET_RESET_STRATEGIES = {"latest", "earliest"}
SCHEMA_MAPPINGS = {"avro": SchemaType.AVRO, "jsonschema": SchemaType.JSONSCHEMA, "protobuf": SchemaType.PROTOBUF}
TypedConsumer = namedtuple("TypedConsumer", ["consumer", "serialization_format", "config"])
IDLE_PROXY_TIMEOUT = 5 * 60
AUTH_EXPIRY_TOLERANCE = datetime.timedelta(seconds=IDLE_PROXY_TIMEOUT)

log = logging.getLogger(__name__)


class FormatError(Exception):
    pass


class KafkaRest(KarapaceBase):
    def __init__(self, config: Config) -> None:
        super().__init__(config=config)
        self._add_kafka_rest_routes()
        self.serializer = SchemaRegistrySerializer(config=config)
        self.proxies: dict[str, UserRestProxy] = {}
        self._proxy_lock = asyncio.Lock()
        log.info("REST proxy starting with (delegated authorization=%s)", self.config.rest_authorization)
        self._idle_proxy_janitor_task: asyncio.Task | None = None

    async def close(self) -> None:
        log.info("Closing REST proxy application")
        if self._idle_proxy_janitor_task is not None:
            self._idle_proxy_janitor_task.cancel()
            self._idle_proxy_janitor_task = None
        async with AsyncExitStack() as stack:
            stack.push_async_callback(super().close)
            stack.push_async_callback(self.serializer.close)

            for proxy in self.proxies.values():
                stack.push_async_callback(proxy.aclose)

    async def _idle_proxy_janitor(self) -> None:
        while True:
            await asyncio.sleep(IDLE_PROXY_TIMEOUT / 2)

            try:
                await self._disconnect_idle_proxy_if_any()
            except Exception:
                log.exception("Disconnecting idle proxy failure")

    async def _disconnect_idle_proxy_if_any(self) -> None:
        idle_consumer_timeout = self.config.consumer_idle_disconnect_timeout

        key, proxy = None, None
        async with self._proxy_lock:
            # Always clean one at time, don't mutate dict while iterating
            for _key, _proxy in self.proxies.items():
                # In case of an OAuth2/OIDC token, the proxy is to be cleaned up _before_ the token expires
                # If the token is still valid within the tolerance time, idleness is still checked
                now = datetime.datetime.now(datetime.timezone.utc)
                if _proxy.auth_expiry and _proxy.auth_expiry < now + AUTH_EXPIRY_TOLERANCE:
                    key, proxy = _key, _proxy
                    log.warning("Releasing unused connection for %s due to token expiry at %s", _proxy, _proxy.auth_expiry)
                    break
                # If UserRestProxy has consumers with state, disconnecting loses state
                if _proxy.num_consumers() > 0:
                    if idle_consumer_timeout > 0 and _proxy.last_used + idle_consumer_timeout < time.monotonic():
                        key, proxy = _key, _proxy
                        log.warning("Disconnecting idle consumers for %s", _proxy)
                        break
                # If there are no consumers, connection can be recreated without losing any state
                else:
                    if _proxy.last_used + IDLE_PROXY_TIMEOUT < time.monotonic():
                        key, proxy = _key, _proxy
                        log.info("Releasing unused connection for %s", _proxy)
                        break
            if key is not None:
                del self.proxies[key]
        if proxy is not None:
            await proxy.aclose()

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

    async def get_user_proxy(self, request: HTTPRequest) -> "UserRestProxy":
        key = ""
        async with self._proxy_lock:
            if self._idle_proxy_janitor_task is None:
                self._idle_proxy_janitor_task = asyncio.create_task(self._idle_proxy_janitor())

            try:
                if self.config.rest_authorization:
                    auth_header = request.headers.get("Authorization")
                    auth_config = get_auth_config_from_header(auth_header, self.config)
                    auth_expiry = get_expiration_time_from_header(auth_header)

                    key = auth_header
                    if self.proxies.get(key) is None:
                        config = self.config.copy()
                        config.bootstrap_uri = config.sasl_bootstrap_uri
                        config.security_protocol = (
                            "SASL_SSL" if config.security_protocol in ("SSL", "SASL_SSL") else "SASL_PLAINTEXT"
                        )

                        config.sasl_mechanism = auth_config["sasl_mechanism"]
                        if "sasl_oauth_token" in auth_config:
                            config.sasl_oauth_token = auth_config["sasl_oauth_token"]
                        else:
                            config.sasl_plain_username = auth_config["sasl_plain_username"]
                            config.sasl_plain_password = auth_config["sasl_plain_password"]

                        self.proxies[key] = UserRestProxy(config, self.kafka_timeout, self.serializer, auth_expiry)
                else:
                    if self.proxies.get(key) is None:
                        self.proxies[key] = UserRestProxy(self.config, self.kafka_timeout, self.serializer)
            except (NoBrokersAvailable, AuthenticationFailedError):
                log.warning("Failed to connect to Kafka with the credentials")
                self.r(body={"message": "Forbidden"}, content_type=JSON_CONTENT_TYPE, status=HTTPStatus.FORBIDDEN)
            proxy = self.proxies[key]
            proxy.mark_used()
            return proxy

    async def list_brokers(self, content_type: str, *, request: HTTPRequest):
        proxy = await self.get_user_proxy(request)
        await proxy.list_brokers(content_type)

    async def commit_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.commit_consumer_offsets(group_name, instance, content_type, request=request)

    async def get_consumer_offsets(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.get_consumer_offsets(group_name, instance, content_type, request=request)

    async def update_consumer_subscription(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.update_consumer_subscription(group_name, instance, content_type, request=request)

    async def get_consumer_subscription(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.get_consumer_subscription(group_name, instance, content_type)

    async def delete_consumer_subscription(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.delete_consumer_subscription(group_name, instance, content_type)

    async def update_consumer_assignment(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.update_consumer_assignment(group_name, instance, content_type, request=request)

    async def get_consumer_assignment(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.get_consumer_assignment(group_name, instance, content_type)

    async def seek_beginning_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.seek_beginning_consumer_offsets(group_name, instance, content_type, request=request)

    async def seek_end_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.seek_end_consumer_offsets(group_name, instance, content_type, request=request)

    async def update_consumer_offsets(
        self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest
    ) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.update_consumer_offsets(group_name, instance, content_type, request=request)

    async def fetch(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.fetch(group_name, instance, content_type, request=request)

    async def create_consumer(self, group_name: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.create_consumer(group_name, content_type, request=request)

    async def delete_consumer(self, group_name: str, instance: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.delete_consumer(group_name, instance, content_type)

    async def partition_offsets(self, content_type: str, *, topic: str, partition_id: str, request: HTTPRequest):
        proxy = await self.get_user_proxy(request)
        await proxy.partition_offsets(content_type, topic=topic, partition_id=partition_id)

    async def partition_details(self, content_type: str, *, topic: str, partition_id: str, request: HTTPRequest):
        proxy = await self.get_user_proxy(request)
        await proxy.partition_details(content_type, topic=topic, partition_id=partition_id)

    async def partition_publish(self, topic: str, partition_id: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.partition_publish(topic, partition_id, content_type, request=request)

    async def list_partitions(self, content_type: str, *, topic: str, request: HTTPRequest):
        proxy = await self.get_user_proxy(request)
        await proxy.list_partitions(content_type, topic=topic)

    async def list_topics(self, content_type: str, *, request: HTTPRequest):
        proxy = await self.get_user_proxy(request)
        await proxy.list_topics(content_type)

    async def topic_details(self, content_type: str, *, topic: str, request: HTTPRequest):
        proxy = await self.get_user_proxy(request)
        await proxy.topic_details(content_type, topic=topic)

    async def topic_publish(self, topic: str, content_type: str, *, request: HTTPRequest) -> None:
        proxy = await self.get_user_proxy(request)
        await proxy.topic_publish(topic, content_type, request=request)


class _ReplicaMetadata(TypedDict):
    broker: int
    leader: bool
    in_sync: bool


class _PartitionMetadata(TypedDict):
    partition: int
    leader: int
    replicas: list[_ReplicaMetadata]


class _TopicMetadata(TypedDict):
    partitions: list[_PartitionMetadata]


class _ClusterMetadata(TypedDict):
    topics: dict[str, _TopicMetadata]
    brokers: list[int]


class UserRestProxy:
    def __init__(
        self,
        config: Config,
        kafka_timeout: int,
        serializer: SchemaRegistrySerializer,
        auth_expiry: datetime.datetime | None = None,
        verify_connection: bool = True,
    ):
        self.config = config
        self.kafka_timeout = kafka_timeout
        self.serializer = serializer
        self._cluster_metadata: _ClusterMetadata = self._empty_cluster_metadata_cache()
        self._cluster_metadata_complete = False
        # birth of all the metadata (when the request was requiring all the metadata available in the cluster)
        self._global_metadata_birth: float = 0.0  # set to this value will always require a refresh at the first call.
        self._cluster_metadata_topic_birth: dict[str, float] = {}
        self.metadata_max_age = self.config.admin_metadata_max_age
        self.admin_client = None
        self.admin_lock = asyncio.Lock()
        self.metadata_cache = None
        self.topic_schema_cache = TopicSchemaCache()
        self.consumer_manager = ConsumerManager(config=config, deserializer=self.serializer)
        self.init_admin_client(verify_connection)
        self._last_used = time.monotonic()
        self._auth_expiry = auth_expiry

        self._async_producer_lock = asyncio.Lock()
        self._async_producer: AsyncKafkaProducer | None = None
        self.naming_strategy = NameStrategy(self.config.name_strategy)

    def __str__(self) -> str:
        return f"UserRestProxy(username={self.config.sasl_plain_username})"

    @property
    def last_used(self) -> int:
        return self._last_used

    def mark_used(self) -> None:
        self._last_used = time.monotonic()

    @property
    def auth_expiry(self) -> datetime.datetime:
        return self._auth_expiry

    def num_consumers(self) -> int:
        return len(self.consumer_manager.consumers)

    async def _maybe_create_async_producer(self) -> AsyncKafkaProducer:
        """
        :raises NoBrokersAvailable:
        :raises AuthenticationFailedError:
        """
        if self._async_producer is not None:
            return self._async_producer

        if self.config.producer_acks == "all":
            acks = -1
        else:
            acks = int(self.config.producer_acks)

        async with self._async_producer_lock:
            for retry in [True, True, False]:
                if self._async_producer is not None:
                    break

                log.info("Creating async producer")

                producer = AsyncKafkaProducer(
                    acks=acks,
                    bootstrap_servers=self.config.bootstrap_uri,
                    compression_type=self.config.producer_compression_type,
                    connections_max_idle_ms=self.config.connections_max_idle_ms,
                    linger_ms=self.config.producer_linger_ms,
                    message_max_bytes=self.config.producer_max_request_size,
                    metadata_max_age_ms=self.config.metadata_max_age_ms,
                    security_protocol=self.config.security_protocol,
                    ssl_cafile=self.config.ssl_cafile,
                    ssl_certfile=self.config.ssl_certfile,
                    ssl_keyfile=self.config.ssl_keyfile,
                    ssl_crlfile=self.config.ssl_crlfile,
                    **get_kafka_client_auth_parameters_from_config(self.config),
                )
                try:
                    await producer.start()
                except (NoBrokersAvailable, AuthenticationFailedError):
                    await producer.stop()
                    if retry:
                        log.warning("Unable to connect to the bootstrap servers, retrying")
                    else:
                        log.warning("Giving up after trying to connect to the bootstrap servers")
                        raise
                    await asyncio.sleep(1)
                except Exception:
                    await producer.stop()
                    raise
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

    def is_global_metadata_old(self) -> bool:
        return (time.monotonic() - self._global_metadata_birth) > self.metadata_max_age

    def is_metadata_of_topics_old(self, topics: list[str]) -> bool:
        # Return from metadata only if all queried topics have cached metadata
        are_all_topic_queried_at_least_once = all(topic in self._cluster_metadata_topic_birth for topic in topics)

        if not are_all_topic_queried_at_least_once:
            return True

        oldest_requested_topic_update_timestamp = min(self._cluster_metadata_topic_birth[topic] for topic in topics)
        return (
            are_all_topic_queried_at_least_once
            and (time.monotonic() - oldest_requested_topic_update_timestamp) > self.metadata_max_age
        )

    def _update_all_metadata(self) -> _ClusterMetadata:
        if not self.is_global_metadata_old() and self._cluster_metadata_complete:
            return self._cluster_metadata

        metadata_birth = time.monotonic()
        metadata = self.admin_client.cluster_metadata(None)
        for topic in metadata["topics"]:
            self._cluster_metadata_topic_birth[topic] = metadata_birth

        self._global_metadata_birth = metadata_birth
        self._cluster_metadata = metadata
        self._cluster_metadata_complete = True
        return metadata

    def _empty_cluster_metadata_cache(self) -> _ClusterMetadata:
        return {"topics": {}, "brokers": []}

    def _update_metadata_for_topics(self, topics: list[str]) -> _ClusterMetadata:
        if not self.is_metadata_of_topics_old(topics):
            return {
                **self._cluster_metadata,
                "topics": {topic: self._cluster_metadata["topics"][topic] for topic in topics},
            }

        metadata_birth = time.monotonic()
        metadata = self.admin_client.cluster_metadata(topics)

        if self._cluster_metadata is None:
            self._cluster_metadata = self._empty_cluster_metadata_cache()

        # we need to refresh if at least 1 broker isn't present in the current metadata
        need_refresh = not all(broker in self._cluster_metadata["brokers"] for broker in metadata["brokers"])

        for topic in metadata["topics"]:
            # or if there is a new topic
            need_refresh = (
                need_refresh
                or (topic not in self._cluster_metadata["topics"])
                # or if a topic has new/different data.
                # nb: equality its valid since the _ClusterMetadata object its structurally
                # composed only of primitives lists and dicts
                or (self._cluster_metadata["topics"][topic] != metadata["topics"][topic])
            )
            self._cluster_metadata_topic_birth[topic] = metadata_birth
            self._cluster_metadata["topics"][topic] = metadata["topics"][topic]

        if need_refresh:
            # we don't need to reason about expiration time since at each request
            # for the global metadata it's checked before performing the request,
            # so we need to guard only for new missing pieces of info
            self._cluster_metadata_complete = False
        else:
            # for malicious actors we may also cache that a certain topic (that do not exist) it has been queried
            # and for a while the reply isn't present. not implementing this now since its an additional complexity
            # that may be unrequired. Leaving a comment and a warning there, if its present often in the logs the feature
            # may be needed.
            log.warning(
                "Requested metadata for topics %s but the reply didn't triggered a cache invalidation. "
                "Data not present on server side",
                topics,
            )
        return metadata

    async def cluster_metadata(self, topics: list[str] | None = None) -> _ClusterMetadata:
        async with self.admin_lock:
            try:
                if topics is None or len(topics) == 0:
                    metadata = self._update_all_metadata()
                else:
                    metadata = self._update_metadata_for_topics(topics)
            except KafkaException:
                log.warning("Could not refresh cluster metadata")
                KafkaRest.r(
                    body={
                        "message": "Kafka node not ready",
                        "code": RESTErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    },
                    content_type="application/json",
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return metadata

    def init_admin_client(self, verify_connection: bool = True) -> KafkaAdminClient:
        for retry in [True, True, False]:
            try:
                self.admin_client = KafkaAdminClient(
                    bootstrap_servers=self.config.bootstrap_uri,
                    security_protocol=self.config.security_protocol,
                    ssl_cafile=self.config.ssl_cafile,
                    ssl_certfile=self.config.ssl_certfile,
                    ssl_keyfile=self.config.ssl_keyfile,
                    metadata_max_age_ms=self.config.metadata_max_age_ms,
                    connections_max_idle_ms=self.config.connections_max_idle_ms,
                    verify_connection=verify_connection,
                    **get_kafka_client_auth_parameters_from_config(self.config),
                )
                break
            except Exception:
                if retry:
                    log.warning("Unable to start admin client, retrying")
                else:
                    log.warning("Giving up after failing to start admin client")
                    raise
                time.sleep(1)

    async def aclose(self) -> None:
        async with AsyncExitStack() as stack, self._async_producer_lock:
            if self._async_producer is not None:
                log.info("Disposing async producer")
                stack.push_async_callback(self._async_producer.stop)

            if self.consumer_manager is not None:
                stack.push_async_callback(self.consumer_manager.aclose)

            self.admin_client = None
            self.consumer_manager = None

    async def publish(self, topic: str, partition_id: str | None, content_type: str, request: HTTPRequest) -> None:
        """
        :raises NoBrokersAvailable:
        :raises AuthenticationFailedError:
        """
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
        except InvalidPayload as e:
            cause = str(e.__cause__)
            KafkaRest.r(
                body={"error_code": RESTErrorCodes.INVALID_DATA.value, "message": cause},
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
        try:
            await self.publish(topic, partition_id, content_type, request)
        except (NoBrokersAvailable, AuthenticationFailedError):
            KafkaRest.service_unavailable(
                message="Service unavailable",
                content_type=content_type,
                sub_code=RESTErrorCodes.HTTP_SERVICE_UNAVAILABLE.value,
            )

    async def topic_publish(self, topic: str, content_type: str, *, request: HTTPRequest) -> None:
        log.debug("Executing topic publish on topic %s", topic)
        try:
            await self.publish(topic, None, content_type, request)
        except (NoBrokersAvailable, AuthenticationFailedError):
            KafkaRest.service_unavailable(
                message="Service unavailable",
                content_type=content_type,
                sub_code=RESTErrorCodes.HTTP_SERVICE_UNAVAILABLE.value,
            )

    @staticmethod
    def validate_partition_id(partition_id: str, content_type: str) -> int:
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

    async def get_schema_id(
        self,
        data: dict,
        topic: str,
        subject_type: SubjectType,
        schema_type: SchemaType,
    ) -> SchemaId:
        """
        This method search and validate the SchemaId for a request, it acts as a guard (In case of something wrong
        throws an error).

        :raises InvalidSchema:
        """
        log.debug("[resolve schema id] Retrieving schema id for %r", data)
        schema_id: SchemaId | None = (
            SchemaId(int(data[f"{subject_type}_schema_id"])) if f"{subject_type}_schema_id" in data else None
        )
        schema_str = data.get(f"{subject_type}_schema")

        if schema_id is None and schema_str is None:
            raise InvalidSchema()

        if schema_id is None:
            parsed_schema = ValidatedTypedSchema.parse(schema_type, schema_str)

            subject_name = get_subject_name(
                topic,
                parsed_schema,
                subject_type,
                self.naming_strategy,
            )
            schema_id = await self._query_schema_id_from_cache_or_registry(parsed_schema, schema_str, subject_name)
        else:

            def subject_not_included(schema: TypedSchema, subjects: list[Subject]) -> bool:
                subject = get_subject_name(topic, schema, subject_type, self.naming_strategy)
                return subject not in subjects

            parsed_schema, valid_subjects = await self._query_schema_and_subjects(
                schema_id,
                need_new_call=subject_not_included,
            )

            if self.config.name_strategy_validation and subject_not_included(parsed_schema, valid_subjects):
                raise InvalidSchema()

        return schema_id

    async def _query_schema_and_subjects(
        self, schema_id: SchemaId, *, need_new_call: Callable[[TypedSchema, list[Subject]], bool] | None
    ) -> tuple[TypedSchema, list[Subject]]:
        try:
            return await self.serializer.get_schema_for_id(schema_id, need_new_call=need_new_call)
        except SchemaRetrievalError as schema_error:
            # if the schema doesn't exist we treated as if the error was due to an invalid schema
            raise InvalidSchema() from schema_error

    async def _query_schema_id_from_cache_or_registry(
        self,
        parsed_schema: ValidatedTypedSchema,
        schema_str: str,
        subject_name: Subject,
    ) -> SchemaId:
        """
        Checks if the schema registered with a certain id match with the schema provided (you can provide
        a valid id but place in the body a totally unrelated schema).
        Also, here if we don't have a match we query the registry  since the cache could be evicted in the meanwhile
        or the schema could be registered without passing though the http proxy.
        """
        schema_id = self.topic_schema_cache.get_schema_id(subject_name, parsed_schema)
        if schema_id is None:
            log.debug("[resolve schema id] Registering / Retrieving ID for %s and schema %s", subject_name, schema_str)
            schema_id = await self.serializer.upsert_id_for_schema(parsed_schema, subject_name)
            log.debug("[resolve schema id] Found schema id %s from registry for subject %s", schema_id, subject_name)
            self.topic_schema_cache.set_schema(subject_name, schema_id, parsed_schema)
        else:
            log.debug(
                "[resolve schema id] schema ID %s found from cache for %s and schema %s",
                schema_id,
                subject_name,
                schema_str,
            )
        return schema_id

    async def validate_schema_info(
        self, data: dict, subject_type: SubjectType, content_type: str, topic: str, schema_type: str
    ):
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
            data[f"{subject_type}_schema_id"] = await self.get_schema_id(data, topic, subject_type, schema_type)
        except InvalidPayload:
            log.warning("Unable to retrieve schema id")
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
                    "message": f"Error when registering schema."
                    f"format = {schema_type.value}, subject = {topic}-{subject_type}",
                },
                content_type=content_type,
                status=HTTPStatus.REQUEST_TIMEOUT,
            )
        except InvalidSchema:
            if f"{subject_type}_schema" in data:
                err = f'schema = {data[f"{subject_type}_schema"]}'
            else:
                err = f'schema_id = {data[f"{subject_type}_schema_id"]}'
            KafkaRest.r(
                body={
                    "error_code": RESTErrorCodes.INVALID_DATA.value,
                    "message": f"Invalid schema. format = {schema_type.value}, {err}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

    async def _prepare_records(
        self,
        content_type: str,
        data: dict,
        ser_format: str,
        key_schema_id: int | None,
        value_schema_id: int | None,
        default_partition: int | None = None,
    ) -> list[tuple]:
        prepared_records = []
        for record in data["records"]:
            key = record.get("key")
            value = record.get("value")
            if key is not None:
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
        ser_format: str | None = None,
        schema_id: int | None = None,
    ) -> bytes:
        if not obj:
            return b""
        # not pretty
        if ser_format == "json":
            # TODO -> get encoding from headers
            return json_encode(obj, binary=True, compact=True)
        if ser_format == "binary":
            try:
                return base64.b64decode(obj)
            except Exception:
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

    async def schema_serialize(self, obj: dict, schema_id: int | None) -> bytes:
        schema, _ = await self.serializer.get_schema_for_id(schema_id)
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
            if set(r.keys()).difference({subject_type.value for subject_type in SubjectType}):
                KafkaRest.unprocessable_entity(
                    message="Invalid request format",
                    content_type=content_type,
                    sub_code=RESTErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                )
        # disallow missing id and schema for any key/value list that has at least one populated element
        if formats["embedded_format"] in {"avro", "jsonschema", "protobuf"}:
            for subject_type, code in zip(SUBJECT_VALID_POSTFIX, RECORD_CODES):
                if self.all_empty(data, subject_type):
                    continue
                if not self.is_valid_schema_request(data, subject_type):
                    KafkaRest.unprocessable_entity(
                        message=f"Request includes {subject_type}s and uses a format that requires schemas "
                        f"but does not include the {subject_type}_schema or {subject_type.value}_schema_id fields",
                        content_type=content_type,
                        sub_code=code,
                    )
                try:
                    await self.validate_schema_info(data, subject_type, content_type, topic, formats["embedded_format"])
                except InvalidMessageSchema as e:
                    KafkaRest.unprocessable_entity(
                        message=str(e),
                        content_type=content_type,
                        sub_code=RESTErrorCodes.INVALID_DATA.value,
                    )

    async def produce_messages(self, *, topic: str, prepared_records: list) -> list:
        """
        :raises NoBrokersAvailable:
        :raises AuthenticationFailedError:
        """
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
                        # In case the offset is not available, `confluent_kafka.Message.offset()` is
                        # `None`. To preserve backwards compatibility, we replace this with -1.
                        # -1 was the default `aiokafka` behaviour.
                        "offset": result.offset() if result and result.offset() is not None else -1,
                        "partition": result.partition() if result else 0,
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
        except TopicAuthorizationFailedError:
            KafkaRest.r(body={"message": "Forbidden"}, content_type=JSON_CONTENT_TYPE, status=HTTPStatus.FORBIDDEN)
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
