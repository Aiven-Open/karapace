"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from typing import Final

import pytest
from aiokafka.errors import IllegalStateError, UnknownTopicOrPartitionError
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, TopicPartition
from confluent_kafka.admin import NewTopic
from confluent_kafka.error import KafkaError

from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka.consumer import AsyncKafkaConsumer, KafkaConsumer
from karapace.core.kafka.producer import AsyncKafkaProducer, KafkaProducer
from karapace.core.utils import Expiration
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import new_topic as create_new_topic

POLL_TIMEOUT_S: Final = 10


class TestPartitionsForTopic:
    def test_partitions_for_returns_empty_for_unknown_topic(self, consumer: KafkaConsumer) -> None:
        assert consumer.partitions_for_topic("nonexistent") == {}

    def test_partitions_for(
        self,
        new_topic: NewTopic,
        consumer: KafkaConsumer,
    ) -> None:
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

    def test_get_watermark_offsets_empty_topic(
        self,
        consumer: KafkaConsumer,
        new_topic: NewTopic,
    ) -> None:
        beginning, end = consumer.get_watermark_offsets(TopicPartition(new_topic.topic, 0))

        assert beginning == 0
        assert end == 0

    def test_get_watermark_offsets_topic_with_one_message(
        self,
        new_topic: NewTopic,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
    ) -> None:
        producer.send(new_topic.topic)
        producer.flush()

        beginning, end = consumer.get_watermark_offsets(TopicPartition(new_topic.topic, 0))

        assert beginning == 0
        assert end == 1


class TestCommit:
    def test_commit_message_and_offset_mutual_exclusion(
        self,
        new_topic: NewTopic,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
    ) -> None:
        fut = producer.send(new_topic.topic)
        producer.flush()
        message = fut.result()

        with pytest.raises(ValueError):
            consumer.commit(message=message, offsets=[])

    def test_commit_message(
        self,
        new_topic: NewTopic,
        producer: KafkaProducer,
        consumer: KafkaConsumer,
    ) -> None:
        consumer.subscribe([new_topic.topic])
        first_fut = producer.send(new_topic.topic)
        second_fut = producer.send(new_topic.topic)
        producer.flush()
        first_fut.result()
        second_fut.result()
        consumer.poll(timeout=POLL_TIMEOUT_S)
        message = consumer.poll(timeout=POLL_TIMEOUT_S)

        [topic_partition] = consumer.commit(message)
        [committed_partition] = consumer.committed([TopicPartition(new_topic.topic, partition=0)])

        assert topic_partition.topic == new_topic.topic
        assert topic_partition.partition == 0
        assert topic_partition.offset == 2
        assert committed_partition.topic == new_topic.topic
        assert committed_partition.partition == 0
        assert committed_partition.offset == 2

    def test_commit_offsets(
        self,
        new_topic: NewTopic,
        producer: KafkaProducer,
        consumer: KafkaConsumer,
    ) -> None:
        consumer.subscribe([new_topic.topic])
        first_fut = producer.send(new_topic.topic)
        second_fut = producer.send(new_topic.topic)
        producer.flush()
        first_fut.result()
        second_fut.result()
        consumer.poll(timeout=POLL_TIMEOUT_S)
        message = consumer.poll(timeout=POLL_TIMEOUT_S)

        [topic_partition] = consumer.commit(
            offsets=[
                TopicPartition(
                    new_topic.topic,
                    partition=0,
                    offset=message.offset() + 1,
                ),
            ]
        )
        [committed_partition] = consumer.committed([TopicPartition(new_topic.topic, partition=0)])

        assert topic_partition.topic == new_topic.topic
        assert topic_partition.partition == 0
        assert topic_partition.offset == 2
        assert committed_partition.topic == new_topic.topic
        assert committed_partition.partition == 0
        assert committed_partition.offset == 2

    def test_commit_offsets_empty(
        self,
        new_topic: NewTopic,
        producer: KafkaProducer,
        consumer: KafkaConsumer,
    ) -> None:
        consumer.subscribe([new_topic.topic])
        first_fut = producer.send(new_topic.topic)
        second_fut = producer.send(new_topic.topic)
        producer.flush()
        first_fut.result()
        second_fut.result()
        consumer.poll(timeout=POLL_TIMEOUT_S)
        consumer.poll(timeout=POLL_TIMEOUT_S)

        [topic_partition] = consumer.commit(offsets=None, message=None)  # default parameters
        [committed_partition] = consumer.committed([TopicPartition(new_topic.topic, partition=0)])

        assert topic_partition.topic == new_topic.topic
        assert topic_partition.partition == 0
        assert topic_partition.offset == 2
        assert committed_partition.topic == new_topic.topic
        assert committed_partition.partition == 0
        assert committed_partition.offset == 2

    def test_commit_raises_for_unknown_partition(
        self,
        new_topic: NewTopic,
        consumer: KafkaConsumer,
    ) -> None:
        consumer.subscribe([new_topic.topic])
        with pytest.raises(UnknownTopicOrPartitionError):
            consumer.commit(offsets=[TopicPartition(new_topic.topic, partition=99, offset=0)])


class TestSubscribe:
    def _poll_until_no_message(self, consumer: KafkaConsumer) -> None:
        """Polls until there is no message returned.

        When verifying subscriptions this can be used to wait until the topics
        subscribed to are all ready. Until a topic is not ready, a message is
        returned with an error indicating that certain topics are not ready to
        be consumed yet.
        """
        msg = consumer.poll(timeout=POLL_TIMEOUT_S)
        while msg is not None:
            msg = consumer.poll(timeout=POLL_TIMEOUT_S)

    def test_subscription_is_recorded(
        self,
        new_topic: NewTopic,
        admin_client: KafkaAdminClient,
        consumer: KafkaConsumer,
    ) -> None:
        prefix = "subscribe"
        topics = [create_new_topic(admin_client, prefix=prefix) for _ in range(3)]

        consumer.subscribe(topics=[new_topic.topic], patterns=[f"{prefix}.*"])
        self._poll_until_no_message(consumer)

        assert consumer.subscription() == frozenset(topics + [new_topic.topic])

    def test_unsubscribe_empties_subscription(
        self,
        new_topic: NewTopic,
        admin_client: KafkaAdminClient,
        consumer: KafkaConsumer,
    ) -> None:
        prefix = "unsubscribe"
        _ = [create_new_topic(admin_client, prefix=prefix) for _ in range(3)]
        consumer.subscribe(topics=[new_topic.topic], patterns=[f"{prefix}.*"])
        self._poll_until_no_message(consumer)

        consumer.unsubscribe()

        self._poll_until_no_message(consumer)

        assert consumer.subscription() == frozenset()

    def test_resubscribe_modifies_subscription(
        self,
        new_topic: NewTopic,
        admin_client: KafkaAdminClient,
        consumer: KafkaConsumer,
    ) -> None:
        prefix = "resubscribe"
        _ = [create_new_topic(admin_client, prefix=prefix) for _ in range(3)]
        consumer.subscribe(topics=[new_topic.topic], patterns=[f"{prefix}.*"])
        self._poll_until_no_message(consumer)

        consumer.subscribe(topics=[new_topic.topic])

        self._poll_until_no_message(consumer)

        assert consumer.subscription() == frozenset([new_topic.topic])


class TestAssign:
    def test_assign(
        self,
        new_topic: NewTopic,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
    ) -> None:
        first_fut = producer.send(new_topic.topic)
        second_fut = producer.send(new_topic.topic, value=b"message-value")
        producer.flush()
        first_fut.result()
        second_fut.result()
        consumer.assign([TopicPartition(new_topic.topic, partition=0, offset=1)])

        [assigned_partition] = consumer.assignment()
        first_message = consumer.poll(POLL_TIMEOUT_S)
        second_message = consumer.poll(POLL_TIMEOUT_S)

        assert first_message.offset() == 1
        assert first_message.topic() == new_topic.topic
        assert first_message.partition() == 0
        assert first_message.key() is None
        assert first_message.value() == b"message-value"
        assert second_message is None

        assert assigned_partition.topic == new_topic.topic
        assert assigned_partition.partition == 0
        assert assigned_partition.offset == 1

    def test_assign_raises_illegal_state_after_subscribe(
        self,
        new_topic: NewTopic,
        consumer: KafkaConsumer,
    ) -> None:
        consumer.subscribe([new_topic.topic])
        consumer.poll(timeout=POLL_TIMEOUT_S)

        with pytest.raises(IllegalStateError):
            consumer.assign([TopicPartition("some-topic", 0)])


class TestSeek:
    def test_seek(
        self,
        new_topic: NewTopic,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
    ) -> None:
        consumer.subscribe([new_topic.topic])
        fut = producer.send(new_topic.topic, value=b"message-value")
        producer.flush()
        fut.result()

        message = consumer.poll(timeout=POLL_TIMEOUT_S)
        consumer.seek(TopicPartition(new_topic.topic, partition=0, offset=OFFSET_BEGINNING))
        same_message = consumer.poll(timeout=POLL_TIMEOUT_S)

        assert message.offset() == same_message.offset()
        assert message.topic() == same_message.topic()
        assert message.partition() == same_message.partition()
        assert message.key() is None
        assert same_message.key() is None
        assert message.value() == same_message.value()

    def test_seek_unassigned_partition_raises(self, consumer: KafkaConsumer, new_topic: NewTopic) -> None:
        with pytest.raises(UnknownTopicOrPartitionError):
            consumer.seek(TopicPartition(new_topic.topic, partition=0, offset=OFFSET_END))


class TestAsyncPoll:
    async def test_async_poll(
        self,
        new_topic: NewTopic,
        asyncproducer: AsyncKafkaProducer,
        asyncconsumer: AsyncKafkaConsumer,
    ) -> None:
        await asyncconsumer.subscribe([new_topic.topic])
        aiofut = await asyncproducer.send(new_topic.topic)
        await aiofut

        message = await asyncconsumer.poll(timeout=POLL_TIMEOUT_S)

        assert message.offset() == 0
        assert message.topic() == new_topic.topic
        assert message.partition() == 0
        assert message.key() is None
        assert message.value() is None

    async def test_async_poll_no_message(
        self,
        new_topic: NewTopic,
        asyncconsumer: AsyncKafkaConsumer,
    ) -> None:
        await asyncconsumer.subscribe([new_topic.topic])

        message = await asyncconsumer.poll(timeout=1)

        assert message is None

    async def test_async_poll_unknown_topic(self, asyncconsumer: AsyncKafkaConsumer) -> None:
        await asyncconsumer.subscribe(["nonexistent"])

        message = await asyncconsumer.poll(timeout=POLL_TIMEOUT_S)

        assert message.topic() == "nonexistent"
        assert message.partition() == 0
        assert message.error() is not None
        assert message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART
        assert message.offset() is None
        assert message.key() is None
        assert message.value().decode() == message.error().str()

    async def test_async_poll_existing_topic_and_unknown_topic_pattern(
        self,
        new_topic: NewTopic,
        asyncproducer: AsyncKafkaProducer,
        asyncconsumer: AsyncKafkaConsumer,
    ) -> None:
        await asyncconsumer.subscribe(topics=[new_topic.topic], patterns=["nonexistent.*"])
        aiofut = await asyncproducer.send(new_topic.topic, value="message-value")
        sent_message = await aiofut

        message = await asyncconsumer.poll(timeout=POLL_TIMEOUT_S)
        while message.error() is not None:
            assert message.topic() in ("^nonexistent.*", new_topic.topic)
            assert message.partition() == 0
            assert message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART
            assert message.offset() is None
            assert message.key() is None
            assert message.value().decode() == message.error().str()
            message = await asyncconsumer.poll(timeout=POLL_TIMEOUT_S)

        message_none = await asyncconsumer.poll(timeout=POLL_TIMEOUT_S)

        assert message.key() == sent_message.key()
        assert message.value() == sent_message.value()

        assert message_none is None


async def test_pattern_subscription_async(
    admin_client: KafkaAdminClient,
    asyncproducer: AsyncKafkaProducer,
    asyncconsumer: AsyncKafkaConsumer,
) -> None:
    prefix = "patterntest"
    number_of_topics = 3
    topics = [create_new_topic(admin_client, prefix=prefix) for _ in range(number_of_topics)]
    await asyncconsumer.subscribe(patterns=[f"{prefix}.*"])
    for i, topic in enumerate(topics):
        aiofut = await asyncproducer.send(topic, value=f"{i}-value")
        await aiofut

    messages = []
    expiration = Expiration.from_timeout(30)
    while len(messages) != 3:
        expiration.raise_timeout_if_expired(
            "Timeout elapsed waiting for messages from topic pattern. Only received {messages}",
            messages=messages,
        )
        message = await asyncconsumer.poll(timeout=POLL_TIMEOUT_S)
        if message is not None and message.error() is None:
            messages.append(message)

    assert sorted(message.value().decode() for message in messages) == [f"{i}-value" for i in range(number_of_topics)]
