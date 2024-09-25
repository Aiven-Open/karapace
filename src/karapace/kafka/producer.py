"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import Future
from confluent_kafka import Message, Producer
from confluent_kafka.admin import PartitionMetadata
from confluent_kafka.error import KafkaError, KafkaException
from functools import partial
from karapace.kafka.common import _KafkaConfigMixin, KafkaClientParams, raise_from_kafkaexception, translate_from_kafkaerror
from threading import Event, Thread
from typing import cast, TypedDict
from typing_extensions import Unpack

import asyncio
import logging

LOG = logging.getLogger(__name__)


def _on_delivery_callback(future: Future, error: KafkaError | None, msg: Message | None) -> None:
    if error is not None:
        LOG.info("Kafka producer delivery error: %s", error)
        future.set_exception(translate_from_kafkaerror(error))
    else:
        future.set_result(msg)


class ProducerSendParams(TypedDict, total=False):
    value: str | bytes | None
    key: str | bytes | None
    partition: int
    timestamp: int | None
    headers: dict[str | None, bytes | None] | list[tuple[str | None, bytes | None]] | None


class KafkaProducer(_KafkaConfigMixin, Producer):
    def send(self, topic: str, **params: Unpack[ProducerSendParams]) -> Future[Message]:
        """A convenience wrapper around `Producer.produce`, to be able to access the message via a Future."""
        result: Future[Message] = Future()

        params = cast(ProducerSendParams, {key: value for key, value in params.items() if value is not None})

        try:
            self.produce(
                topic,
                on_delivery=partial(_on_delivery_callback, result),
                **params,
            )
        except KafkaException as exc:
            raise_from_kafkaexception(exc)

        return result

    def partitions_for(self, topic: str) -> dict[int, PartitionMetadata]:
        """Returns all partition metadata for the given topic."""
        try:
            return self.list_topics(topic).topics[topic].partitions
        except KafkaException as exc:
            raise_from_kafkaexception(exc)


class AsyncKafkaProducer:
    """An async wrapper around `KafkaProducer` built on top of confluent-kafka.

    Calling `start` on an `AsyncKafkaProducer` instantiates a `KafkaProducer`
    and starts a poll-thread.

    The poll-thread continuously polls the underlying producer so buffered messages
    are sent and asyncio futures returned by the `send` method can be awaited.
    """

    def __init__(
        self,
        bootstrap_servers: Iterable[str] | str,
        loop: asyncio.AbstractEventLoop | None = None,
        **params: Unpack[KafkaClientParams],
    ) -> None:
        self.loop = loop or asyncio.get_running_loop()

        self.stopped = Event()
        self.poll_thread = Thread(target=self.poll_loop)

        self.producer: KafkaProducer | None = None
        self._bootstrap_servers = bootstrap_servers
        self._producer_params = params

    def _start(self) -> None:
        assert not self.stopped.is_set(), "The async producer cannot be restarted"

        self.producer = KafkaProducer(self._bootstrap_servers, **self._producer_params)
        self.poll_thread.start()

    async def start(self) -> None:
        # The `KafkaProducer` instantiation tries to establish a connection with
        # retries, thus can block for a relatively long time. Running in the
        # default executor and awaiting makes it async compatible.
        await self.loop.run_in_executor(None, self._start)

    def _stop(self) -> None:
        self.stopped.set()
        if self.poll_thread.is_alive():
            self.poll_thread.join()
        self.producer = None

    async def stop(self) -> None:
        # Running all actions needed to stop in the default executor, since
        # some can be blocking.
        await self.loop.run_in_executor(None, self._stop)

    def poll_loop(self) -> None:
        """Target of the poll-thread."""
        assert self.producer is not None, "The async producer must be started"

        while not self.stopped.is_set():
            # The call to `poll` is blocking, necessitating running this loop in its own thread.
            # In case there is messages to be sent, this loop will do just that (equivalent to
            # a `flush` call), otherwise it'll sleep for the given timeout (seconds).
            self.producer.poll(timeout=0.1)

    async def send(self, topic: str, **params: Unpack[ProducerSendParams]) -> asyncio.Future[Message]:
        assert self.producer is not None, "The async producer must be started"

        return asyncio.wrap_future(
            self.producer.send(topic, **params),
            loop=self.loop,
        )
