"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from kafka.errors import InvalidReplicationFactorError, TopicAlreadyExistsError, UnknownTopicOrPartitionError
from karapace.kafka.admin import ConfigSource, KafkaAdminClient, NewTopic
from karapace.kafka.producer import KafkaProducer
from tests.utils import new_topic as create_new_topic

import pytest


class TestNewTopic:
    def test_new_topic_raises_for_duplicate(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        with pytest.raises(TopicAlreadyExistsError):
            admin_client.new_topic(new_topic.topic)

    def test_new_topic_raises_for_invalid_replication_factor(self, admin_client: KafkaAdminClient) -> None:
        with pytest.raises(InvalidReplicationFactorError):
            admin_client.new_topic("some-new-topic", replication_factor=99)


class TestClusterMetadata:
    def test_cluster_metadata_brokers(self, admin_client: KafkaAdminClient) -> None:
        cluster_metadata = admin_client.cluster_metadata()

        assert len(cluster_metadata) == 2, "Cluster metadata should have keys topics and brokers"
        assert len(cluster_metadata["brokers"]) == 1, "Only one broker during tests"

    def test_cluster_metadata_all_topics(self, admin_client: KafkaAdminClient) -> None:
        topic_names = [create_new_topic(admin_client) for _ in range(5)]

        topics_metadata = admin_client.cluster_metadata()["topics"]

        assert set(topic_names) - set(topics_metadata.keys()) == set(), "All created topics should be returned"
        for topic in topic_names:
            partitions_data = topics_metadata[topic]["partitions"]

            assert len(partitions_data) == 1, "Should only have data for one partition"
            assert len(partitions_data[0]["replicas"]) == 1, "Should only have 1 replica"

    def test_cluster_metadata_specific_topic(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        topics_metadata = admin_client.cluster_metadata([new_topic.topic])["topics"]

        assert list(topics_metadata.keys()) == [new_topic.topic]

    def test_cluster_metadata_raises_for_unknown_topic(self, admin_client: KafkaAdminClient) -> None:
        with pytest.raises(UnknownTopicOrPartitionError):
            admin_client.cluster_metadata(["nonexistent_topic"])


class TestGetTopicConfig:
    def test_get_topic_config_no_filters(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        cfg = admin_client.get_topic_config(new_topic.topic)
        assert "cleanup.policy" in cfg

    def test_get_topic_config_empty_filters(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        topic_config_filtered = admin_client.get_topic_config(
            new_topic.topic,
            config_name_filter=(),
            config_source_filter=(),
        )

        assert topic_config_filtered == {}

    def test_get_topic_config_name_filter_only(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        topic_config_filtered = admin_client.get_topic_config(
            new_topic.topic,
            config_name_filter=("flush.ms"),
            config_source_filter=(),
        )

        assert list(topic_config_filtered.keys()) == ["flush.ms"]

    def test_get_topic_config_source_filter_only_noresult(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        topic_config_filtered = admin_client.get_topic_config(
            new_topic.topic,
            config_name_filter=(),
            config_source_filter=(ConfigSource.DYNAMIC_TOPIC_CONFIG,),
        )

        assert topic_config_filtered == {}

    def test_get_topic_config_source_filter_only(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        admin_client.update_topic_config(new_topic.topic, {"flush.ms": "12345"})

        topic_config_filtered = admin_client.get_topic_config(
            new_topic.topic,
            config_name_filter=(),
            config_source_filter=(ConfigSource.DYNAMIC_TOPIC_CONFIG,),
        )

        assert topic_config_filtered == {"flush.ms": "12345"}

    def test_get_topic_config_raises_for_unknown_topic(self, admin_client: KafkaAdminClient) -> None:
        with pytest.raises(UnknownTopicOrPartitionError):
            admin_client.get_topic_config("nonexistent_topic")


class TestUpdateTopicConfig:
    def test_update_topic_config(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        admin_client.update_topic_config(new_topic.topic, {"flush.ms": "12345"})

        topic_config_filtered = admin_client.get_topic_config(
            new_topic.topic,
            config_name_filter=("flush.ms",),
            config_source_filter=(),
        )
        assert topic_config_filtered == {"flush.ms": "12345"}

    def test_update_topic_config_raises_for_unknown_topic(self, admin_client: KafkaAdminClient) -> None:
        with pytest.raises(UnknownTopicOrPartitionError):
            admin_client.update_topic_config("nonexistent_topic", {"flush.ms": "12345"})


class TestGetOffsets:
    def test_get_offsets(self, admin_client: KafkaAdminClient, new_topic: NewTopic, producer: KafkaProducer) -> None:
        topic_name = new_topic.topic
        partition_id = 0
        number_of_messages = 5
        for _ in range(number_of_messages):
            producer.send(topic_name)
        producer.flush()

        offsets = admin_client.get_offsets(topic_name, partition_id)

        assert offsets == {"beginning_offset": 0, "end_offset": number_of_messages}

    def test_get_offsets_raises_for_unknown_topic(self, admin_client: KafkaAdminClient) -> None:
        with pytest.raises(UnknownTopicOrPartitionError):
            admin_client.get_offsets("nonexistent_topic", 0)

    def test_get_offsets_raises_for_unknown_partition(self, admin_client: KafkaAdminClient, new_topic: NewTopic) -> None:
        with pytest.raises(UnknownTopicOrPartitionError):
            admin_client.get_offsets(new_topic.topic, 10)


class TestDeleteTopic:
    def test_delete_topic(self, admin_client: KafkaAdminClient) -> None:
        topic_name = create_new_topic(admin_client)
        topics_metadata_before_delete = admin_client.cluster_metadata()["topics"]

        admin_client.delete_topic(topic_name)

        topics_metadata_after_delete = admin_client.cluster_metadata()["topics"]

        assert topic_name in topics_metadata_before_delete
        assert topic_name not in topics_metadata_after_delete

    def test_delete_topic_raises_for_unknown_topic(self, admin_client: KafkaAdminClient) -> None:
        with pytest.raises(UnknownTopicOrPartitionError):
            admin_client.delete_topic("nonexistent_topic")
