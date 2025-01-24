"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Container, Iterable
from concurrent.futures import Future
from confluent_kafka import TopicCollection, TopicPartition
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
from confluent_kafka.error import KafkaException
from karapace.core.constants import TOPIC_CREATION_TIMEOUT_S
from karapace.core.kafka.common import (
    _KafkaConfigMixin,
    KafkaClientParams,
    raise_from_kafkaexception,
    single_futmap_result,
    UnknownTopicOrPartitionError,
)
from karapace.api.telemetry.tracer import Tracer
from typing_extensions import Unpack


class KafkaAdminClient(_KafkaConfigMixin, AdminClient):
    def __init__(
        self,
        bootstrap_servers: Iterable[str] | str,
        **params: Unpack[KafkaClientParams],
    ) -> None:
        self._tracer = Tracer()
        super().__init__(bootstrap_servers, **params)

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
        self.log.info("Creating new topic %s with replication factor %s", new_topic, replication_factor)
        futmap: dict[str, Future] = self.create_topics([new_topic], request_timeout=request_timeout)
        try:
            single_futmap_result(futmap)
            return new_topic
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def update_topic_config(self, name: str, config: dict[str, str]) -> None:
        self.log.info("Updating topic '%s' configuration with %s", name, config)
        futmap = self.alter_configs([ConfigResource(ResourceType.TOPIC, name, set_config=config)])
        try:
            single_futmap_result(futmap)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def delete_topic(self, name: str) -> None:
        self.log.info("Deleting topic '%s'", name)
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
        self.log.info("Fetching cluster metadata with topic filter: %s", topics)
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
        self.log.info(
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
            self.log.info("Fetching latest offset for topic '%s' partition %s", topic, partition_id)
            latest_offset_futmap: dict[TopicPartition, Future] = self.list_offsets(
                {
                    TopicPartition(topic, partition_id): OffsetSpec.latest(),
                }
            )
            endoffset = single_futmap_result(latest_offset_futmap)

            self.log.info("Fetching earliest offset for topic '%s' partition %s", topic, partition_id)
            earliest_offset_futmap: dict[TopicPartition, Future] = self.list_offsets(
                {
                    TopicPartition(topic, partition_id): OffsetSpec.earliest(),
                }
            )
            startoffset = single_futmap_result(earliest_offset_futmap)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)
        return {"beginning_offset": startoffset.offset, "end_offset": endoffset.offset}

    def describe_topics(self, topics: TopicCollection) -> dict[str, Future]:
        with self._tracer.get_tracer().start_as_current_span(
            self._tracer.get_name_from_caller_with_class(self, self.describe_topics)
        ):
            return super().describe_topics(topics)
