"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import asyncio
import pytest
import threading

from aiokafka.errors import KafkaTimeoutError
from confluent_kafka import TopicPartition
from pytest import MonkeyPatch

from karapace.core.kafka import common
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.kafka.consumer import AsyncKafkaConsumer, KafkaConsumer
from karapace.core.kafka.producer import KafkaProducer

# Nothing listens here, so requests to the "cluster" can never be answered.
UNREACHABLE = "127.0.0.1:1"
TEST_TIMEOUT_SECONDS = 0.5


def _consumer() -> KafkaConsumer:
    return KafkaConsumer(bootstrap_servers=UNREACHABLE, verify_connection=False)


def _admin() -> KafkaAdminClient:
    return KafkaAdminClient(bootstrap_servers=UNREACHABLE, verify_connection=False)


def _producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=UNREACHABLE, verify_connection=False)


CALLS: dict[str, Callable[[], Any]] = {
    "consumer.list_topics": lambda: _consumer().list_topics(),
    "consumer.partitions_for_topic": lambda: _consumer().partitions_for_topic("topic"),
    "consumer.committed": lambda: _consumer().committed([TopicPartition("topic", 0)]),
    "consumer.get_watermark_offsets": lambda: _consumer().get_watermark_offsets(TopicPartition("topic", 0)),
    "admin.list_topics": lambda: _admin().list_topics(),
    "admin.cluster_metadata": lambda: _admin().cluster_metadata(),
    "producer.list_topics": lambda: _producer().list_topics(),
    "producer.partitions_for": lambda: _producer().partitions_for("topic"),
}


@pytest.mark.parametrize("call", CALLS.values(), ids=CALLS.keys())
def test_call_without_explicit_timeout_does_not_block_forever(monkeypatch: MonkeyPatch, call: Callable[[], Any]) -> None:
    monkeypatch.setattr(common, "DEFAULT_KAFKA_API_TIMEOUT_SECONDS", TEST_TIMEOUT_SECONDS)
    outcome: list[str] = []

    def run() -> None:
        try:
            call()
        except Exception as exc:  # noqa: BLE001 - only interested in the call returning at all
            outcome.append(type(exc).__name__)
        else:
            outcome.append("returned")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=10)

    assert not thread.is_alive(), "call blocked far beyond the default timeout"


async def test_async_consumer_sync_commit_gives_up_when_broker_never_answers(monkeypatch: MonkeyPatch) -> None:
    """`Consumer.commit(asynchronous=False)` has no timeout parameter, so the async wrapper has to bound the wait."""
    monkeypatch.setattr(common, "DEFAULT_KAFKA_API_TIMEOUT_SECONDS", TEST_TIMEOUT_SECONDS)
    release = threading.Event()
    consumer = AsyncKafkaConsumer(UNREACHABLE)
    consumer.consumer = MagicMock()
    consumer.consumer.commit.side_effect = lambda *_args: release.wait(timeout=10)  # never answers, like a stuck commit

    try:
        with pytest.raises(KafkaTimeoutError):
            await asyncio.wait_for(consumer.commit(), timeout=5)
    finally:
        release.set()


async def test_async_consumer_does_not_start_another_commit_while_a_timed_out_one_is_still_running(
    monkeypatch: MonkeyPatch,
) -> None:
    """Giving up on a commit doesn't stop it: retries must not pile up more stuck workers on the same consumer."""
    monkeypatch.setattr(common, "DEFAULT_KAFKA_API_TIMEOUT_SECONDS", TEST_TIMEOUT_SECONDS)
    release = threading.Event()
    consumer = AsyncKafkaConsumer(UNREACHABLE)
    consumer.consumer = MagicMock()
    consumer.consumer.commit.side_effect = lambda *_args: release.wait(timeout=10)

    try:
        with pytest.raises(KafkaTimeoutError):
            await consumer.commit()

        with pytest.raises(KafkaTimeoutError):
            await consumer.commit()
        assert consumer.consumer.commit.call_count == 1  # no second worker was started
    finally:
        release.set()

    await asyncio.sleep(0.2)  # let the first commit finish
    consumer.consumer.commit.side_effect = None
    await consumer.commit()
    assert consumer.consumer.commit.call_count == 2


def test_producer_flush_keeps_waiting_for_delivery_reports(monkeypatch: MonkeyPatch) -> None:
    """`flush()` must not give up early: callers then read the delivery futures without a timeout.

    librdkafka already bounds a flush through `message.timeout.ms`, so unlike the other calls no default is applied.
    """
    monkeypatch.setattr(common, "DEFAULT_KAFKA_API_TIMEOUT_SECONDS", TEST_TIMEOUT_SECONDS)
    producer = _producer()
    producer.produce("topic", b"value")  # can't be delivered: nothing listens, so it stays queued
    flushed = threading.Event()

    def run() -> None:
        producer.flush()
        flushed.set()

    threading.Thread(target=run, daemon=True).start()

    assert not flushed.wait(timeout=TEST_TIMEOUT_SECONDS * 3), "flush() returned before the message was delivered or failed"
