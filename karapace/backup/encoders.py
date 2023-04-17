"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from karapace.key_format import KeyFormatter
from karapace.typing import JsonData, JsonObject
from karapace.utils import json_decode, json_encode


def encode_key(
    key: JsonObject | str,
    key_formatter: KeyFormatter | None,
) -> bytes | None:
    if key == "null":
        return None
    if not key_formatter:
        if isinstance(key, str):
            return key.encode("utf8")
        return json_encode(key, sort_keys=False, binary=True, compact=False)
    if isinstance(key, str):
        key = json_decode(key, JsonObject)
    return key_formatter.format_key(key)


def encode_value(value: JsonData) -> bytes | None:
    if value == "null":
        return None
    if isinstance(value, str):
        return value.encode("utf8")
    return json_encode(value, compact=True, sort_keys=False, binary=True)
