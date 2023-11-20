"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import Future
from confluent_kafka import TopicPartition
from confluent_kafka.admin import (
    AdminClient,
    BrokerMetadata,
    ClusterMetadata,
    ConfigResource,
    ConfigSource,
    NewTopic,
    OffsetSpec,
    ResourceType,
    TopicMetadata,
)
from confluent_kafka.error import KafkaError, KafkaException
from kafka.errors import AuthenticationFailedError, for_code, NoBrokersAvailable, UnknownTopicOrPartitionError
from karapace.constants import TOPIC_CREATION_TIMEOUT_S
from typing import Any, Callable, Container, NoReturn, Protocol, TypedDict, TypeVar
from typing_extensions import Unpack

import logging

LOG = logging.getLogger(__name__)


T = TypeVar("T")


def single_futmap_result(futmap: dict[Any, Future[T]]) -> T:
    """Extract the result of a future wrapped in a dict.

    Bulk operations of the `confluent_kafka` library's Kafka clients return results
    wrapped in a dictionary of futures. Most often we use these bulk operations to
    operate on a single resource/entity. This function makes sure the dictionary of
    futures contains a single future and returns its result.
    """
    (future,) = futmap.values()
    return future.result()


def raise_from_kafkaexception(exc: KafkaException) -> NoReturn:
    """Raises a more developer-friendly error from a `KafkaException`.

    The `confluent_kafka` library's `KafkaException` is a wrapper around its internal
    `KafkaError`. The resulting, raised exception however is coming from
    `kafka-python`, due to these exceptions having human-readable names, providing
    better context for error handling.

    `kafka.errors.for_code` is used to translate the original exception's error code
    to a domain specific error class from `kafka-python`.
    """
    raise for_code(exc.args[0].code()) from exc


# For now this is a bit of a trick to replace an explicit usage of
# `karapace.kafka_rest_apis.authentication.SimpleOauthTokenProvider`
# to avoid circular imports
class TokenWithExpiryProvider(Protocol):
    def token_with_expiry(self, config: str | None) -> tuple[str, int | None]:
        ...


class AdminClientParams(TypedDict, total=False):
    client_id: str | None
    connections_max_idle_ms: int | None
    metadata_max_age_ms: int | None
    sasl_mechanism: str | None
    sasl_plain_password: str | None
    sasl_plain_username: str | None
    security_protocol: str | None
    ssl_cafile: str | None
    ssl_certfile: str | None
    ssl_keyfile: str | None
    sasl_oauth_token_provider: TokenWithExpiryProvider


class KafkaAdminClient(AdminClient):
    def __init__(self, bootstrap_servers: Iterable[str] | str, **params: Unpack[AdminClientParams]) -> None:
        self._errors: set[KafkaError] = set()

        super().__init__(self._get_config_from_params(bootstrap_servers, **params))
        self._activate_callbacks()
        self._verify_connection()

    def _get_config_from_params(self, bootstrap_servers: Iterable[str] | str, **params: Unpack[AdminClientParams]) -> dict:
        if not isinstance(bootstrap_servers, str):
            bootstrap_servers = ",".join(bootstrap_servers)

        config: dict[str, int | str | Callable | None] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": params.get("client_id"),
            "connections.max.idle.ms": params.get("connections_max_idle_ms"),
            "metadata.max.age.ms": params.get("metadata_max_age_ms"),
            "sasl.mechanism": params.get("sasl_mechanism"),
            "sasl.password": params.get("sasl_plain_password"),
            "sasl.username": params.get("sasl_plain_username"),
            "security.protocol": params.get("security_protocol"),
            "ssl.ca.location": params.get("ssl_cafile"),
            "ssl.certificate.location": params.get("ssl_certfile"),
            "ssl.key.location": params.get("ssl_keyfile"),
            "error_cb": self._error_callback,
        }
        config = {key: value for key, value in config.items() if value is not None}

        if "sasl_oauth_token_provider" in params:
            config["oauth_cb"] = params["sasl_oauth_token_provider"].token_with_expiry

        return config

    def _error_callback(self, error: KafkaError) -> None:
        self._errors.add(error)

    def _activate_callbacks(self) -> None:
        # Any client in the `confluent_kafka` library needs `poll` called to
        # trigger any callbacks registered (eg. for errors, OAuth tokens, etc.)
        self.poll(timeout=0.0)

    def _verify_connection(self) -> None:
        """Attempts to call `AdminClient.list_topics` a few times.

        The `list_topics` method is the only meaningful synchronous method of
        the `AdminClient` class that can be used to verify that a connection and
        authentication has been established with a Kafka cluster.

        Just instantiating and initializing the admin client doesn't result in
        anything in its main thread in case of errors, only error logs from another
        thread otherwise.
        """
        for _ in range(3):
            try:
                self.list_topics(timeout=1)
            except KafkaException as exc:
                # Other than `list_topics` throwing a `KafkaException` with an underlying
                # `KafkaError` with code `_TRANSPORT` (`-195`), if the address or port is
                # incorrect, we get no symptoms
                # Authentication errors however do show up in the errors passed
                # to the callback function defined in the `error_cb` config
                self._activate_callbacks()
                LOG.info("Could not establish connection due to errors: %s", self._errors)
                if any(
                    error.code() == KafkaError._AUTHENTICATION for error in self._errors  # pylint: disable=protected-access
                ):
                    raise AuthenticationFailedError() from exc
                continue
            else:
                break
        else:
            raise NoBrokersAvailable()

    def new_topic(
        self,
        name: str,
        *,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: dict[str, str] | None = None,
        request_timeout: float = TOPIC_CREATION_TIMEOUT_S,
    ) -> NewTopic:
        new_topic = NewTopic(
            topic=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config if config is not None else {},
        )
        LOG.info("Creating new topic %s with replication factor %s", new_topic, replication_factor)
        futmap: dict[str, Future] = self.create_topics([new_topic], request_timeout=request_timeout)
        try:
            single_futmap_result(futmap)
            return new_topic
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def update_topic_config(self, name: str, config: dict[str, str]) -> None:
        LOG.info("Updating topic '%s' configuration with %s", name, config)
        futmap = self.alter_configs([ConfigResource(ResourceType.TOPIC, name, set_config=config)])
        try:
            single_futmap_result(futmap)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def delete_topic(self, name: str) -> None:
        LOG.info("Deleting topic '%s'", name)
        futmap = self.delete_topics([name])
        try:
            single_futmap_result(futmap)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def cluster_metadata(self, topics: Iterable[str] | None = None) -> dict:
        """Fetch cluster metadata and topic information for given topics or all topics if not given.

        Using the `list_topics` method of the `AdminClient`, as this actually provides
        metadata for the entire cluster, not just topics, as suggested by the name.

        The topics filter is only applied _after_ fetching the cluster metadata,
        due to `list_topics` only accepting a single topic as a filter.
        """
        LOG.info("Fetching cluster metadata with topic filter: %s", topics)
        cluster_metadata: ClusterMetadata = self.list_topics()
        topics_metadata: dict[str, TopicMetadata] = cluster_metadata.topics
        brokers_metadata: dict[int, BrokerMetadata] = cluster_metadata.brokers

        if topics is not None and any(topic not in topics_metadata.keys() for topic in topics):
            raise UnknownTopicOrPartitionError()

        topics_data: dict[str, dict] = {}
        for topic, topic_metadata in topics_metadata.items():
            if topics is not None and topic not in topics:
                continue

            partitions_data = []
            for partition_id, partition_metadata in topic_metadata.partitions.items():
                partition_data = {
                    "partition": partition_id,
                    "leader": partition_metadata.leader,
                    "replicas": [
                        {
                            "broker": replica_id,
                            "leader": replica_id == partition_metadata.leader,
                            "in_sync": replica_id in partition_metadata.isrs,
                        }
                        for replica_id in partition_metadata.replicas
                    ],
                }
                partitions_data.append(partition_data)

            topics_data[topic] = {"partitions": partitions_data}

        return {"topics": topics_data, "brokers": list(brokers_metadata.keys())}

    def get_topic_config(
        self,
        name: str,
        config_name_filter: Container[str] | None = None,
        config_source_filter: Container[ConfigSource] | None = None,
    ) -> dict[str, str]:
        """Fetches, filters and returns topic configuration.

        The two filters, `config_name_filter` and `config_source_filter` work together
        so if a config entry matches either of them, it'll be returned.
        If a filter is not provided (ie. is `None`), it'll act as if matching all
        config entries.
        """
        LOG.info(
            "Fetching config for topic '%s' with name filter %s and source filter %s",
            name,
            config_name_filter,
            config_source_filter,
        )
        futmap: dict[ConfigResource, Future] = self.describe_configs([ConfigResource(ResourceType.TOPIC, name)])
        try:
            topic_configs = single_futmap_result(futmap)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

        config: dict[str, str] = {}
        for config_name, config_entry in topic_configs.items():
            matches_name_filter: bool = config_name_filter is None or config_name in config_name_filter
            matches_source_filter: bool = (
                config_source_filter is None or ConfigSource(config_entry.source) in config_source_filter
            )

            if matches_name_filter or matches_source_filter:
                config[config_name] = config_entry.value

        return config

    def get_offsets(self, topic: str, partition_id: int) -> dict[str, int]:
        """Returns the beginning and end offsets for a topic partition.

        Making two separate requests for beginning and end offsets, due to the
        `AdminClient.list_offsets` behaviour: it expects a dictionary of topic
        partitions as keys, thus unable to fetch different values in one request
        for the same topic and partition.
        """
        try:
            LOG.info("Fetching latest offset for topic '%s' partition %s", topic, partition_id)
            latest_offset_futmap: dict[TopicPartition, Future] = self.list_offsets(
                {
                    TopicPartition(topic, partition_id): OffsetSpec.latest(),
                }
            )
            endoffset = single_futmap_result(latest_offset_futmap)

            LOG.info("Fetching earliest offset for topic '%s' partition %s", topic, partition_id)
            earliest_offset_futmap: dict[TopicPartition, Future] = self.list_offsets(
                {
                    TopicPartition(topic, partition_id): OffsetSpec.earliest(),
                }
            )
            startoffset = single_futmap_result(earliest_offset_futmap)
        except KafkaException as exc:
            code = exc.args[0].code()
            # In some cases `list_offsets` raises an error with a `_NOENT`, code `-156` error
            # with the message "Failed to query partition leaders: No leaders found", which is
            # "internal" to `confluent_kafka` and has to be handled separately.
            if code == KafkaError._NOENT:  # pylint: disable=protected-access
                raise UnknownTopicOrPartitionError() from exc
            raise_from_kafkaexception(exc)
        return {"beginning_offset": startoffset.offset, "end_offset": endoffset.offset}
