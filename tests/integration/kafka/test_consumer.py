"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from confluent_kafka import TopicPartition
from confluent_kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from karapace.kafka.consumer import KafkaConsumer
from karapace.kafka.producer import KafkaProducer
from tests.integration.utils.kafka_server import KafkaServers

import pytest


class TestPartitionsForTopic:
    def test_partitions_for_returns_empty_for_unknown_topic(self, kafka_servers: KafkaServers) -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_servers.bootstrap_servers,
            topic="nonexistent",
        )

        assert consumer.partitions_for_topic("nonexistent") == {}

    def test_partitions_for(self, kafka_servers: KafkaServers, new_topic: NewTopic) -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_servers.bootstrap_servers,
            topic=new_topic.topic,
        )

        partitions = consumer.partitions_for_topic(new_topic.topic)

        assert len(partitions) == 1
        assert partitions[0].id == 0
        assert partitions[0].replicas == [1]
        assert partitions[0].isrs == [1]


class TestGetWatermarkOffsets:
    def test_get_watermark_offsets_unkown_topic(self, kafka_servers: KafkaServers) -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_servers.bootstrap_servers,
            topic="nonexistent",
        )

        with pytest.raises(UnknownTopicOrPartitionError):
            _, _ = consumer.get_watermark_offsets(TopicPartition("nonexistent", 0))

    def test_get_watermark_offsets_empty_topic(self, kafka_servers: KafkaServers, new_topic: NewTopic) -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_servers.bootstrap_servers,
            topic=new_topic.topic,
            auto_offset_reset="earliest",
        )

        beginning, end = consumer.get_watermark_offsets(TopicPartition(new_topic.topic, 0))

        assert beginning == 0
        assert end == 0

    def test_get_watermark_offsets_topic_with_one_message(
        self,
        kafka_servers: KafkaServers,
        producer: KafkaProducer,
        new_topic: NewTopic,
    ) -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_servers.bootstrap_servers,
            topic=new_topic.topic,
            auto_offset_reset="earliest",
        )
        producer.send(new_topic.topic)
        producer.flush()

        beginning, end = consumer.get_watermark_offsets(TopicPartition(new_topic.topic, 0))

        assert beginning == 0
        assert end == 1
