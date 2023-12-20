"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from concurrent.futures import Future
from confluent_kafka import Message, Producer
from confluent_kafka.admin import PartitionMetadata
from confluent_kafka.error import KafkaError, KafkaException
from functools import partial
from karapace.kafka.common import _KafkaConfigMixin, raise_from_kafkaexception, translate_from_kafkaerror
from typing import cast, TypedDict
from typing_extensions import Unpack

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
