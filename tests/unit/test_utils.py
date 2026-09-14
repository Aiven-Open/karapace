"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import io
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType
from unittest.mock import MagicMock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from karapace.core.utils import (
    DebugAccessLogger,
    Expiration,
    Timeout,
    default_json_serialization,
    get_project_root,
    json,
    json_decode,
    json_encode,
    shutdown,
)


def test_shutdown(caplog: LogCaptureFixture) -> None:
    with (
        caplog.at_level(logging.WARNING, logger="karapace.core.utils"),
        patch("karapace.core.utils.signal") as mock_signal,
    ):
        mock_signal.SIGTERM = 15

        shutdown()
        mock_signal.raise_signal.assert_called_once_with(15)
        for log in caplog.records:
            assert log.name == "karapace.core.utils"
            assert log.levelname == "WARNING"
            assert log.message == "=======> Sending shutdown signal `SIGTERM` to Application process <======="


class TestJsonModuleWrapper:
    """Exercises the orjson-backed `json` shim's `load`/`dump`/indent handling."""

    def test_dumps_with_indent_uses_two_space_indent(self) -> None:
        result = json.dumps({"b": 1, "a": 2}, indent=2)
        assert isinstance(result, str)
        assert "\n" in result

    def test_load_reads_str_content_from_file_object(self) -> None:
        fp = io.StringIO('{"key": "value"}')
        assert json.load(fp) == {"key": "value"}

    def test_load_reads_bytes_content_from_file_object(self) -> None:
        fp = io.BytesIO(b'{"key": "value"}')
        assert json.load(fp) == {"key": "value"}

    def test_dump_writes_json_bytes_to_file_object(self) -> None:
        fp = io.BytesIO()
        json.dump({"a": 1}, fp)
        assert json.loads(fp.getvalue()) == {"a": 1}

    def test_dump_with_indent_and_sort_keys(self) -> None:
        fp = io.BytesIO()
        json.dump({"b": 1, "a": 2}, fp, indent=2, sort_keys=True)
        assert json.loads(fp.getvalue()) == {"a": 2, "b": 1}


class TestDefaultJsonSerialization:
    def test_naive_datetime_is_treated_as_utc(self) -> None:
        naive = datetime(2024, 1, 1, 12, 0, 0)  # noqa: DTZ001 - intentionally naive for testing
        assert default_json_serialization(naive) == "2024-01-01T12:00:00Z"

    def test_timezone_aware_datetime_is_converted_to_utc(self) -> None:
        aware = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
        assert default_json_serialization(aware) == "2024-01-01T10:00:00Z"

    def test_timedelta_is_converted_to_total_seconds(self) -> None:
        assert default_json_serialization(timedelta(seconds=90)) == 90.0

    def test_decimal_is_converted_to_str(self) -> None:
        assert default_json_serialization(Decimal("1.50")) == "1.50"

    def test_mapping_proxy_is_converted_to_dict(self) -> None:
        proxy = MappingProxyType({"a": 1})
        assert default_json_serialization(proxy) == {"a": 1}

    def test_unsupported_type_raises_runtime_error(self) -> None:
        with pytest.raises(RuntimeError, match="not JSON serializable"):
            default_json_serialization(object())


class TestJsonEncode:
    def test_encode_with_indent_omits_compact_separators(self) -> None:
        result = json_encode({"a": 1, "b": 2}, indent=2)
        assert isinstance(result, str)
        assert "\n" in result

    def test_encode_binary_true_returns_bytes(self) -> None:
        result = json_encode({"a": 1}, binary=True)
        assert isinstance(result, bytes)

    def test_encode_sort_keys(self) -> None:
        result = json_encode({"b": 1, "a": 2}, sort_keys=True)
        assert result.index('"a"') < result.index('"b"')


class TestJsonDecode:
    def test_decode_from_str(self) -> None:
        assert json_decode('{"a": 1}') == {"a": 1}

    def test_decode_from_bytes(self) -> None:
        assert json_decode(b'{"a": 1}') == {"a": 1}

    def test_decode_from_file_object(self) -> None:
        fp = io.StringIO('{"a": 1}')
        assert json_decode(fp) == {"a": 1}


def test_get_project_root_points_at_karapace_package() -> None:
    root = get_project_root()
    assert root.name == "karapace"
    assert (root / "core").is_dir()


class TestExpiration:
    def test_from_timeout_sets_deadline_in_the_future(self) -> None:
        with patch("karapace.core.utils.time.monotonic", return_value=100.0):
            expiration = Expiration.from_timeout(30.0)
        assert expiration.start_time == 100.0
        assert expiration.deadline == 130.0

    def test_elapsed_reflects_time_since_start(self) -> None:
        expiration = Expiration(start_time=100.0, deadline=200.0)
        with patch("karapace.core.utils.time.monotonic", return_value=115.0):
            assert expiration.elapsed == 15.0

    def test_is_expired_false_before_deadline(self) -> None:
        expiration = Expiration(start_time=100.0, deadline=200.0)
        with patch("karapace.core.utils.time.monotonic", return_value=150.0):
            assert expiration.is_expired() is False

    def test_is_expired_true_after_deadline(self) -> None:
        expiration = Expiration(start_time=100.0, deadline=200.0)
        with patch("karapace.core.utils.time.monotonic", return_value=250.0):
            assert expiration.is_expired() is True

    def test_raise_timeout_if_expired_noop_when_not_expired(self) -> None:
        expiration = Expiration(start_time=100.0, deadline=200.0)
        with patch("karapace.core.utils.time.monotonic", return_value=150.0):
            expiration.raise_timeout_if_expired("still waiting on {}", "data")

    def test_raise_timeout_if_expired_raises_formatted_message(self) -> None:
        expiration = Expiration(start_time=100.0, deadline=200.0)
        with (
            patch("karapace.core.utils.time.monotonic", return_value=250.0),
            pytest.raises(Timeout, match="timed out on foo"),
        ):
            expiration.raise_timeout_if_expired("timed out on {}", "foo")


class TestDebugAccessLogger:
    def _make_logger(self) -> DebugAccessLogger:
        return DebugAccessLogger(logger=MagicMock(), log_format="%s")

    def test_log_with_plain_string_keys(self) -> None:
        access_logger = self._make_logger()
        access_logger._format_line = MagicMock(return_value=[("status", "200")])

        access_logger.log(MagicMock(), MagicMock(), 0.1)

        access_logger.logger.debug.assert_called_once()
        _, kwargs = access_logger.logger.debug.call_args
        assert kwargs["extra"] == {"status": "200"}

    def test_log_with_tuple_keys_groups_nested_extras(self) -> None:
        access_logger = self._make_logger()
        access_logger._log_format = "%s %s"
        access_logger._format_line = MagicMock(
            return_value=[(("i", "Referer"), "http://example.com"), (("i", "User-Agent"), "pytest")]
        )

        access_logger.log(MagicMock(), MagicMock(), 0.1)

        _, kwargs = access_logger.logger.debug.call_args
        assert kwargs["extra"] == {"i": {"Referer": "http://example.com", "User-Agent": "pytest"}}

    def test_log_swallows_exceptions_and_logs_them(self) -> None:
        access_logger = self._make_logger()
        access_logger._format_line = MagicMock(side_effect=RuntimeError("boom"))

        # Must not propagate: access logging must never crash the request handler.
        access_logger.log(MagicMock(), MagicMock(), 0.1)

        access_logger.logger.exception.assert_called_once_with("Error in logging")
