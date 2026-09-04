"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import base64
from unittest.mock import MagicMock

from karapace.kafka_rest_apis.consumer_manager import ConsumerManager


def _msg(headers):
    msg = MagicMock()
    msg.headers.return_value = headers
    return msg


def test_encode_headers_none_returns_empty() -> None:
    assert ConsumerManager._encode_headers(_msg(None)) == []


def test_encode_headers_empty_returns_empty() -> None:
    assert ConsumerManager._encode_headers(_msg([])) == []


def test_encode_headers_encodes_values_as_base64() -> None:
    result = ConsumerManager._encode_headers(_msg([("traceId", b"abc123"), ("tombstone", None)]))
    assert result == [
        {"name": "traceId", "value": base64.b64encode(b"abc123").decode("utf-8")},
        {"name": "tombstone", "value": None},
    ]


def test_encode_headers_handles_bytes_key() -> None:
    result = ConsumerManager._encode_headers(_msg([(b"traceId", b"v")]))
    assert result == [{"name": "traceId", "value": base64.b64encode(b"v").decode("utf-8")}]


def test_encode_headers_non_utf8_key_does_not_raise() -> None:
    # A non-UTF-8 header key (e.g. written by a non-REST client) must not break consume.
    result = ConsumerManager._encode_headers(_msg([(b"\xff\xfe", b"v")]))
    assert len(result) == 1
    assert result[0]["value"] == base64.b64encode(b"v").decode("utf-8")


def test_encode_headers_swallows_headers_call_failure() -> None:
    # If msg.headers() itself raises, degrade to empty headers rather than failing the batch.
    msg = MagicMock()
    msg.headers.side_effect = RuntimeError("boom")
    assert ConsumerManager._encode_headers(msg) == []


def test_encode_headers_skips_only_the_bad_header() -> None:
    # A single unencodable header (value is not bytes) is skipped; its siblings survive.
    result = ConsumerManager._encode_headers(_msg([("good", b"ok"), ("bad", "not-bytes"), ("also_good", None)]))
    assert result == [
        {"name": "good", "value": base64.b64encode(b"ok").decode("utf-8")},
        {"name": "also_good", "value": None},
    ]


def test_header_bytes_sums_key_and_value_lengths() -> None:
    # 7 (traceId) + 6 (abc123) + 5 (count) + 0 (None value) = 18
    assert ConsumerManager._header_bytes(_msg([("traceId", b"abc123"), ("count", None)])) == 18


def test_header_bytes_none_or_empty_is_zero() -> None:
    assert ConsumerManager._header_bytes(_msg(None)) == 0
    assert ConsumerManager._header_bytes(_msg([])) == 0


def test_header_bytes_never_raises() -> None:
    msg = MagicMock()
    msg.headers.side_effect = RuntimeError("boom")
    assert ConsumerManager._header_bytes(msg) == 0
