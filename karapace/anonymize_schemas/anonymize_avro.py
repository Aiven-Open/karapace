"""
karapace - anonymize avro

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from typing import Any, Dict, List, Union

import hashlib

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


def anonymize_name(name: str) -> str:
    """Anonymize the name.

    Name is splitted by dot to logical elements that the whole name consists of.
    The element is sha1 hashed. After hashing element is cleaned to conform to Avro name
    format of:
      * start with [A-Za-z_]
      * subsequently contain only [A-Za-z0-9_]

    Returns anonymized name.
    """
    anonymized_elements = []
    for element in name.split("."):
        element = hashlib.sha1(element.encode("utf-8")).hexdigest()
        # SHA-1 can start with number, just add 'a' as first character.
        # This breaks the hash, but is still consistent as required.
        as_list_element = list(element)
        as_list_element[0] = "a"
        anonymized_elements.append("".join(as_list_element))

    return ".".join(anonymized_elements)


Schema = Union[str, Dict[str, Any], List[Any]]


def anonymize(input_schema: Schema) -> Schema:
    if not input_schema:  # pylint: disable=no-else-return
        return input_schema
    elif isinstance(input_schema, str):
        if input_schema in ALL_TYPES:
            return input_schema
        return anonymize_name(input_schema)
    elif isinstance(input_schema, List):
        return [anonymize(value) for value in input_schema]
    elif isinstance(input_schema, Dict):
        output_schema: Dict[str, Any] = {}
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
        return input_schema
