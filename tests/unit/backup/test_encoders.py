"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.backup.encoders import encode_key, encode_value
from karapace.core.key_format import KeyFormatter
from unittest import mock

import pytest


class TestEncodeKey:
    @pytest.mark.parametrize("formatter", (KeyFormatter(), None))
    def test_returns_none_for_string_null(self, formatter: KeyFormatter | None) -> None:
        assert encode_key("null", formatter) is None

    def test_returns_string_as_bytes_when_no_formatter(self) -> None:
        assert encode_key("foo", None) == b"foo"

    def test_returns_encoded_json_bytes_when_no_formatter(self) -> None:
        assert encode_key({"foo": "bar"}, None) == b'{"foo":"bar"}'

    @mock.patch("karapace.core.key_format.KeyFormatter.format_key")
    def test_returns_formatted_key(self, format_key: mock.MagicMock) -> None:
        argument = {"foo": "bar"}
        format_key.return_value = b"formatted-key"
        key_formatter = KeyFormatter()

        assert encode_key(argument, key_formatter) == format_key.return_value
        format_key.assert_called_once_with(argument)

    @mock.patch("karapace.core.key_format.KeyFormatter.format_key")
    def test_decodes_key_as_json_if_string(self, format_key: mock.MagicMock) -> None:
        argument = '{"foo": "bar"}'
        format_key.return_value = b"formatted-key"
        key_formatter = KeyFormatter()

        assert encode_key(argument, key_formatter) == format_key.return_value
        format_key.assert_called_once_with({"foo": "bar"})


class TestEncodeValue:
    def test_returns_none_for_string_null(self) -> None:
        assert encode_value("null") is None

    def test_returns_string_as_bytes(self) -> None:
        assert encode_value("foo") == b"foo"

    def test_returns_json_encoded_bytes(self) -> None:
        assert encode_value({"foo": "bar"}) == b'{"foo":"bar"}'
