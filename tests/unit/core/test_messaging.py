"""
Tests for the Karapace producer (``karapace.core.messaging``).

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from aiokafka.errors import MessageSizeTooLargeError

from karapace.core.config import Config
from karapace.core.errors import SchemaTooLargeException
from karapace.core.key_format import KeyFormatter
from karapace.core.messaging import X_REGISTRY_VERSION_HEADER, KarapaceProducer
from karapace.core.offset_watcher import OffsetWatcher


def _make_producer(config: Config | None = None) -> KarapaceProducer:
    return KarapaceProducer(
        config=config or Config(),
        offset_watcher=OffsetWatcher(),
        key_formatter=KeyFormatter(),
    )


class TestInitializeKarapaceProducer:
    def test_creates_producer_on_first_attempt(self) -> None:
        producer = _make_producer()
        with patch("karapace.core.messaging.KafkaProducer") as mock_kafka_producer_cls:
            producer.initialize_karapace_producer()

        mock_kafka_producer_cls.assert_called_once()
        assert producer._producer is mock_kafka_producer_cls.return_value

    def test_retries_until_producer_construction_succeeds(self) -> None:
        producer = _make_producer()
        with (
            patch("karapace.core.messaging.KafkaProducer", side_effect=[RuntimeError("boom"), MagicMock()]) as mock_cls,
            patch("karapace.core.messaging.time.sleep") as mock_sleep,
        ):
            producer.initialize_karapace_producer()

        assert mock_cls.call_count == 2
        mock_sleep.assert_called_once_with(1)
        assert producer._producer is not None


class TestClose:
    def test_close_without_producer_is_noop(self) -> None:
        producer = _make_producer()
        # Should not raise even though no producer was ever initialized.
        producer.close()

    def test_close_flushes_existing_producer(self) -> None:
        producer = _make_producer()
        producer._producer = MagicMock()

        producer.close()

        producer._producer.flush.assert_called_once_with()


class TestSendKafkaMessage:
    def _producer_with_mock_kafka(self) -> tuple[KarapaceProducer, MagicMock]:
        producer = _make_producer()
        mock_kafka_producer = MagicMock()
        producer._producer = mock_kafka_producer
        return producer, mock_kafka_producer

    def test_encodes_str_key_and_value_to_bytes(self) -> None:
        producer, mock_kafka_producer = self._producer_with_mock_kafka()
        mock_kafka_producer.send.return_value.result.return_value.offset.return_value = 1
        producer._offset_watcher.wait_for_offset = MagicMock(return_value=True)

        producer._send_kafka_message(key="my-key", value="my-value")

        _, kwargs = mock_kafka_producer.send.call_args
        assert kwargs["key"] == b"my-key"
        assert kwargs["value"] == b"my-value"
        assert kwargs["headers"] == [X_REGISTRY_VERSION_HEADER, producer._x_origin_host_header]

    def test_passes_through_bytes_key_and_value_unchanged(self) -> None:
        producer, mock_kafka_producer = self._producer_with_mock_kafka()
        mock_kafka_producer.send.return_value.result.return_value.offset.return_value = 1
        producer._offset_watcher.wait_for_offset = MagicMock(return_value=True)

        producer._send_kafka_message(key=b"raw-key", value=b"raw-value")

        _, kwargs = mock_kafka_producer.send.call_args
        assert kwargs["key"] == b"raw-key"
        assert kwargs["value"] == b"raw-value"

    def test_flushes_after_send_with_configured_timeout(self) -> None:
        producer, mock_kafka_producer = self._producer_with_mock_kafka()
        mock_kafka_producer.send.return_value.result.return_value.offset.return_value = 1
        producer._offset_watcher.wait_for_offset = MagicMock(return_value=True)

        producer._send_kafka_message(key=b"k", value=b"v")

        mock_kafka_producer.flush.assert_called_once_with(timeout=producer._kafka_timeout)

    def test_message_size_too_large_is_translated_to_schema_too_large(self) -> None:
        producer, mock_kafka_producer = self._producer_with_mock_kafka()
        mock_kafka_producer.send.return_value.result.side_effect = MessageSizeTooLargeError("too big")

        with pytest.raises(SchemaTooLargeException):
            producer._send_kafka_message(key=b"k", value=b"v")

    def test_waits_for_schema_reader_and_returns_when_offset_seen(self) -> None:
        producer, mock_kafka_producer = self._producer_with_mock_kafka()
        mock_kafka_producer.send.return_value.result.return_value.offset.return_value = 5
        producer._offset_watcher.wait_for_offset = MagicMock(return_value=True)

        producer._send_kafka_message(key=b"k", value=b"v")

        producer._offset_watcher.wait_for_offset.assert_called_once_with(5, timeout=60)

    def test_raises_runtime_error_when_schema_reader_never_catches_up(self) -> None:
        producer, mock_kafka_producer = self._producer_with_mock_kafka()
        mock_kafka_producer.send.return_value.result.return_value.offset.return_value = 7
        producer._offset_watcher.wait_for_offset = MagicMock(return_value=False)

        with pytest.raises(RuntimeError, match="Schema reader timed out"):
            producer._send_kafka_message(key=b"k", value=b"v")

    def test_raises_runtime_error_when_sent_offset_is_none(self) -> None:
        # A `None` offset short-circuits the `sent_offset is not None and ...` check,
        # so the reader-timeout branch fires without ever consulting the offset watcher.
        producer, mock_kafka_producer = self._producer_with_mock_kafka()
        mock_kafka_producer.send.return_value.result.return_value.offset.return_value = None
        producer._offset_watcher.wait_for_offset = MagicMock()

        with pytest.raises(RuntimeError, match="Schema reader timed out"):
            producer._send_kafka_message(key=b"k", value=b"v")

        producer._offset_watcher.wait_for_offset.assert_not_called()


class TestSendMessage:
    def test_send_message_with_value_encodes_key_and_value(self) -> None:
        producer = _make_producer()
        producer._send_kafka_message = MagicMock()

        producer.send_message(
            key={"subject": "s", "magic": 0, "keytype": "SCHEMA"},
            value={"foo": "bar"},
        )

        _, kwargs = producer._send_kafka_message.call_args
        assert isinstance(kwargs["key"], bytes)
        assert isinstance(kwargs["value"], bytes)
        assert b"foo" in kwargs["value"]

    def test_send_message_with_none_value_sends_empty_bytes_tombstone(self) -> None:
        producer = _make_producer()
        producer._send_kafka_message = MagicMock()

        producer.send_message(key={"subject": "s", "magic": 0, "keytype": "SCHEMA"}, value=None)

        _, kwargs = producer._send_kafka_message.call_args
        assert kwargs["value"] == b""
