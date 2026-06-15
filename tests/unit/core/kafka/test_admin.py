"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from confluent_kafka.error import KafkaError, KafkaException
import pytest

from karapace.core.kafka.admin import KafkaAdminClient


def _make_admin_client() -> KafkaAdminClient:
    admin_client = KafkaAdminClient.__new__(KafkaAdminClient)
    admin_client.log = MagicMock()
    admin_client.create_topics = MagicMock(return_value={"topic": "future"})
    admin_client.list_topics = MagicMock()
    return admin_client


@patch("karapace.core.kafka.admin.single_futmap_result")
@patch("karapace.core.kafka.admin.time.sleep")
def test_new_topic_waits_for_metadata_propagation(mock_sleep: MagicMock, mock_single_futmap_result: MagicMock) -> None:
    admin_client = _make_admin_client()
    admin_client.list_topics.side_effect = [
        SimpleNamespace(topics={}),
        SimpleNamespace(topics={"topic": SimpleNamespace(error=None, partitions={0: object()})}),
    ]

    topic = admin_client.new_topic("topic")

    assert topic.topic == "topic"
    mock_single_futmap_result.assert_called_once_with({"topic": "future"})
    assert admin_client.list_topics.call_args_list == [call(topic="topic", timeout=1), call(topic="topic", timeout=1)]
    mock_sleep.assert_called_once_with(0.1)


@patch("karapace.core.kafka.admin.time.sleep")
def test_wait_for_topic_metadata_retries_after_kafkaexception(mock_sleep: MagicMock) -> None:
    admin_client = _make_admin_client()
    admin_client.list_topics.side_effect = [
        KafkaException(KafkaError(KafkaError._TIMED_OUT)),
        SimpleNamespace(topics={"topic": SimpleNamespace(error=None, partitions={0: object()})}),
    ]

    admin_client._wait_for_topic_metadata(name="topic", num_partitions=1, timeout=1.0)

    assert admin_client.list_topics.call_args_list == [call(topic="topic", timeout=1), call(topic="topic", timeout=1)]
    mock_sleep.assert_called_once_with(0.1)


@patch("karapace.core.kafka.admin.time.sleep")
@patch("karapace.core.kafka.admin.time.monotonic")
def test_wait_for_topic_metadata_raises_timeout_when_topic_never_appears(
    mock_monotonic: MagicMock, mock_sleep: MagicMock
) -> None:
    admin_client = _make_admin_client()
    admin_client.list_topics.return_value = SimpleNamespace(topics={})
    mock_monotonic.side_effect = [0.0, 0.0, 0.2]

    with pytest.raises(TimeoutError, match="Topic metadata for 'topic' did not propagate within 0.1 seconds"):
        admin_client._wait_for_topic_metadata(name="topic", num_partitions=1, timeout=0.1)

    admin_client.list_topics.assert_called_once_with(topic="topic", timeout=1)
    mock_sleep.assert_called_once_with(0.1)
