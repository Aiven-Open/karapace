"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections import deque
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from karapace.core.kafka.types import Timestamp
from karapace.kafka_rest_apis.consumer_manager import ConsumerManager, KarapaceBase, TypedConsumer


class FakeConsumer:
    def __init__(self, batches: list[list[MagicMock]]) -> None:
        self.batches = deque(batches)
        self.requested_counts: list[int] = []

    async def consume(self, num_messages: int, timeout: float) -> list[MagicMock]:
        self.requested_counts.append(num_messages)
        batch = self.batches.popleft() if self.batches else []
        assert len(batch) <= num_messages
        return batch


def _message(offset: int, size: int) -> MagicMock:
    message = MagicMock()
    message.error.return_value = None
    message.key.return_value = None
    message.value.return_value = b"x" * size
    message.headers.return_value = []
    message.topic.return_value = "synthetic-topic"
    message.partition.return_value = 0
    message.offset.return_value = offset
    message.timestamp.return_value = (Timestamp.CREATE_TIME, offset)
    return message


@pytest.mark.asyncio
async def test_fetch_preserves_messages_remaining_after_max_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    first_batch = [_message(0, 100), _message(1, 100)]
    second_batch = [_message(offset, 200) for offset in range(2, 21)]
    consumer = FakeConsumer([first_batch, second_batch])
    manager = ConsumerManager(
        config=SimpleNamespace(rest_base_uri="http://localhost/", consumer_request_max_bytes=2048), deserializer=None
    )
    internal_name = ("synthetic-group", "synthetic-consumer")
    manager.consumers[internal_name] = TypedConsumer(
        consumer=consumer,
        serialization_format="binary",
        config={"consumer.request.timeout.ms": 1000},
    )
    responses: list[list[dict]] = []

    def capture_response(*, content_type: str, body: list[dict]) -> None:
        responses.append(body)

    monkeypatch.setattr(KarapaceBase, "r", staticmethod(capture_response))
    for _ in range(2):
        await manager.fetch(
            internal_name,
            content_type="application/vnd.kafka.binary.v2+json",
            formats={"embedded_format": "binary"},
            query_params={"max_bytes": "2048", "timeout": "1000"},
        )

    assert [record["offset"] for record in responses[0]] == list(range(12))
    assert [record["offset"] for record in responses[1]] == list(range(12, 21))
    assert not manager.pending_messages[internal_name]
    assert consumer.requested_counts == [2, 19, 2]
