"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiokafka.errors import IllegalStateError, KafkaTimeoutError
from collections.abc import Callable, Iterable
from confluent_kafka import Consumer, Message, TopicPartition
from confluent_kafka.admin import PartitionMetadata
from confluent_kafka.error import KafkaException
from karapace.core.kafka.common import _KafkaConfigMixin, KafkaClientParams, raise_from_kafkaexception
from typing import Any, TypeVar
from typing_extensions import Unpack

import asyncio
import secrets


class KafkaConsumer(_KafkaConfigMixin, Consumer):
    def __init__(
        self,
        bootstrap_servers: Iterable[str] | str,
        topic: str | None = None,
        verify_connection: bool = True,
        **params: Unpack[KafkaClientParams],
    ) -> None:
        # The `confluent_kafka.Consumer` does not allow for a missing group id
        # if the client of this class does not provide one, we'll generate a
        # unique group id to achieve the groupless behaviour
        if "group_id" not in params:
            params["group_id"] = self._create_group_id()

        super().__init__(bootstrap_servers, verify_connection, **params)

        self._subscription: frozenset[str] = frozenset()
        if topic is not None:
            self.subscribe([topic])

    @staticmethod
    def _create_group_id() -> str:
        return f"karapace-autogenerated-{secrets.token_hex(6)}"

    def partitions_for_topic(self, topic: str) -> dict[int, PartitionMetadata]:
        """Returns all partition metadata for the given topic."""
        try:
            return self.list_topics(topic).topics[topic].partitions
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float | None = None, cached: bool = False
    ) -> tuple[int, int]:
        """Wrapper around `Consumer.get_watermark_offsets` to handle error cases and exceptions.

        confluent-kafka is somewhat inconsistent with error-related behaviours,
        `get_watermark_offsets` returns `None` on timeouts, so we are translating it to an
        exception.
        """
        try:
            if timeout is not None:
                result = super().get_watermark_offsets(partition, timeout, cached)
            else:
                result = super().get_watermark_offsets(partition, cached=cached)

            if result is None:
                raise KafkaTimeoutError()

            return result
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def commit(
        self,
        message: Message | None = None,
        offsets: list[TopicPartition] | None = None,
    ) -> list[TopicPartition] | None:
        """Commit offsets based on a message or offsets (topic partitions).

        The `message` and `offsets` parameters are mutually exclusive.
        """
        if message is not None and offsets is not None:
            raise ValueError("Parameters message and offsets are mutually exclusive.")

        try:
            if message is not None:
                return super().commit(message=message, asynchronous=False)

            if offsets is not None:
                return super().commit(offsets=offsets, asynchronous=False)

            return super().commit(asynchronous=False)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def committed(self, partitions: list[TopicPartition], timeout: float | None = None) -> list[TopicPartition]:
        try:
            if timeout is not None:
                return super().committed(partitions, timeout)

            return super().committed(partitions)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def subscribe(
        self,
        topics: list[str] | None = None,
        patterns: list[str] | None = None,
    ) -> None:
        """Subscribe to a list of topics and/or topic patterns.

        Subscriptions are not incremental.
        For `Consumer.subscribe`, Topic patterns must start with "^", eg.
        "^this-is-a-regex-[0-9]", thus we prefix all strings in the `patterns`
        list with "^".

        The `on_assign` and `on_revoke` callbacks are set to keep track of
        subscriptions (topics).

        More in the confluent-kafka documentation:
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.subscribe
        """
        topics = topics or []
        patterns = patterns or []
        self.log.info("Subscribing to topics %s and patterns %s", topics, patterns)
        if self._subscription:
            self.log.warning("Overriding existing subscription: %s", self.subscription())
        try:
            super().subscribe(
                topics + [f"^{pattern}" for pattern in patterns],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

        # Prefill the subscription set with fixed topic names, so only topic
        # pattern updates have the need to wait for the callback.
        self._subscription = frozenset(topics)

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        topics = frozenset(partition.topic for partition in partitions)
        self._subscription = self._subscription.union(topics)

    def _on_revoke(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        topics = frozenset(partition.topic for partition in partitions)
        self._subscription = self._subscription.difference(topics)

    def subscription(self) -> frozenset[str]:
        """Returns the list of topic names the consumer is subscribed to.

        The topic list is maintained by the `_on_assign` and `_on_revoke` callback
        methods, which are set in `subscribe`. These callbacks are only called
        when `poll` is called.
        """
        return self._subscription

    def unsubscribe(self) -> None:
        try:
            super().unsubscribe()
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

        self._subscription = frozenset()

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Assign a list of topic partitions to the consumer.

        Raises an `IllegalStateError` if `subscribe` has been previously called.
        This is partly to match previous behaviour from `aiokafka`, but more
        importantly to make sure we do not eventually reset the consumer by
        calling `assign` after `subscribe` for the same topic - which would
        result in the consumer starting from the beginning of a topic after an
        unspecified time.
        """
        if self._subscription:
            raise IllegalStateError

        try:
            super().assign(partitions)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def assignment(self) -> list[TopicPartition]:
        try:
            return super().assignment()
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

    def seek(self, partition: TopicPartition) -> None:
        try:
            super().seek(partition)
        except KafkaException as exc:
            raise_from_kafkaexception(exc)


T = TypeVar("T")


class AsyncKafkaConsumer:
    """An async wrapper around `KafkaConsumer` built on confluent-kafka.

    Async methods are ran in the event loop's executor. Calling `start`
    instantiates the underlying `KafkaConsumer`.
    """

    _START_ERROR: str = "Async consumer must be started"

    def __init__(
        self,
        bootstrap_servers: Iterable[str] | str,
        topic: str | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        **params: Unpack[KafkaClientParams],
    ) -> None:
        self.loop = loop or asyncio.get_running_loop()

        self.consumer: KafkaConsumer | None = None
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._consumer_params = params

    async def _run_in_executor(self, func: Callable[..., T], *args: Any) -> T:
        return await self.loop.run_in_executor(None, func, *args)

    def _start(self) -> None:
        self.consumer = KafkaConsumer(
            self._bootstrap_servers,
            topic=self._topic,
            **self._consumer_params,
        )

    async def start(self) -> None:
        await self._run_in_executor(self._start)

    async def poll(self, timeout: float) -> Message | None:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.poll, timeout)

    async def commit(
        self,
        message: Message | None = None,
        offsets: list[TopicPartition] | None = None,
    ) -> list[TopicPartition] | None:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.commit, message, offsets)

    async def committed(self, partitions: list[TopicPartition], timeout: float | None = None) -> list[TopicPartition]:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.committed, partitions, timeout)

    async def subscribe(self, topics: list[str] | None = None, patterns: list[str] | None = None) -> None:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.subscribe, topics, patterns)

    def subscription(self) -> frozenset[str]:
        assert self.consumer is not None, self._START_ERROR
        return self.consumer.subscription()

    async def unsubscribe(self) -> None:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.unsubscribe)

    async def assign(self, partitions: list[TopicPartition]) -> None:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.assign, partitions)

    async def assignment(self) -> list[TopicPartition]:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.assignment)

    async def seek(self, partition: TopicPartition) -> None:
        assert self.consumer is not None, self._START_ERROR
        return await self._run_in_executor(self.consumer.seek, partition)

    async def stop(self) -> None:
        assert self.consumer is not None, self._START_ERROR
        # After the `KafkaConsumer` is closed, there is no further action to
        # be taken, as it has its own checks and errors are raised if a closed
        # consumer is tried to be used
        return await self._run_in_executor(self.consumer.close)
