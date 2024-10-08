"""
karapace - anonymize avro

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from typing import Any, Union
from typing_extensions import TypeAlias

import hashlib
import re

ALIASES = "aliases"
DEFAULT = "default"
DOC = "doc"
ENUM = "enum"
FIELDS = "fields"
ITEMS = "items"
LOGICAL_TYPE = "logicalType"
NAME = "name"
NAMESPACE = "namespace"
ORDER = "order"
PRECISION = "precision"
SCALE = "scale"
SIZE = "size"
SYMBOLS = "symbols"
TYPE = "type"
VALUES = "values"


# Doc and order field are special case and not included
KEYWORDS = [
    ALIASES,
    DEFAULT,
    ENUM,
    FIELDS,
    ITEMS,
    LOGICAL_TYPE,
    NAME,
    NAMESPACE,
    PRECISION,
    SCALE,
    SIZE,
    SYMBOLS,
    TYPE,
    VALUES,
]
PRIMITIVE_TYPES = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]
LOGICAL_TYPES = [
    "date",
    "decimal",
    "duration",
    "local-timestamp-micros",
    "local-timestamp-millis",
    "time-micros",
    "time-millis",
    "timestamp-micros",
    "timestamp-millis",
    "uuid",
]
ALL_TYPES = PRIMITIVE_TYPES + LOGICAL_TYPES + ["array", ENUM, "fixed", "map", "record"]

ORDER_VALID_VALUES = ["ascending", "descending", "ignore"]

NAME_ANONYMIZABLE_PATTERN = re.compile("[^.]+")
INVALID_CHARACTER_PATTERN = re.compile("[^.a-zA-Z0-9_]")


def anonymize_name(name: str) -> str:
    """Anonymize the name.

    Name is splitted by dot to logical elements that the whole name consists of.
    The element is sha1 hashed. After hashing element is cleaned to conform to Avro name
    format of:
      * start with [A-Za-z_]
      * subsequently contain only [A-Za-z0-9_]

    Returns anonymized name.
    """

    def anonymize_element(m: re.Match) -> str:
        string = m.group()

        # Preserve possible invalid characters as a suffix.
        invalid_chars = list(set(INVALID_CHARACTER_PATTERN.findall(string)))
        invalid_chars.sort()  # for consistency
        invalid_chars_suffix = "".join(invalid_chars)

        digest = hashlib.sha1(string.encode("utf-8")).hexdigest()

        # AVRO requires field names to start from a-zA-Z. SHA-1 can start with number.
        # Replace the first character with 'a'.
        return "a" + digest[1:] + invalid_chars_suffix

    return NAME_ANONYMIZABLE_PATTERN.sub(anonymize_element, name)


Schema: TypeAlias = Union[str, dict[str, Any], list[Any]]


def anonymize(input_schema: Schema) -> Schema:
    if not input_schema:  # pylint: disable=no-else-return
        return input_schema
    elif isinstance(input_schema, str):
        if input_schema in ALL_TYPES:
            return input_schema
        return anonymize_name(input_schema)
    elif isinstance(input_schema, list):
        return [anonymize(value) for value in input_schema]
    elif isinstance(input_schema, dict):
        output_schema: dict[str, Any] = {}
        for key, value in input_schema.items():
            if key in KEYWORDS:
                output_schema[key] = anonymize(value)
            elif key == DOC:
                pass  # Doc attribute may contain non-public and sensitive data
            elif key == ORDER:
                if value in ORDER_VALID_VALUES:
                    output_schema[key] = value
                else:
                    output_schema[key] = anonymize(value)
            else:
                if isinstance(key, str):
                    key = anonymize_name(key)
                output_schema[key] = anonymize(value)
        return output_schema
    else:
        return input_schema  # type: ignore[unreachable]
