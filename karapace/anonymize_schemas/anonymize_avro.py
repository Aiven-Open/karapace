"""
karapace - anonymize avro

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from typing import Any, Dict, List, Optional, Union

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

PRIMITIVE_TYPES = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]
ALL_TYPES = PRIMITIVE_TYPES + ["array", ENUM, "fixed", "map", "record"]


def anonymize_name(name: Any) -> Any:
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


def anonymize_union_type(schema: List[Any]) -> List[Any]:
    union_types = []
    for union_type in schema:
        if union_type in PRIMITIVE_TYPES:
            union_types.append(union_type)
        elif isinstance(union_type, str):
            union_types.append(anonymize_name(union_type))
        else:
            union_types.append(anonymize(union_type))
    return union_types


def anonymize_complex_type(input_schema: Dict[str, Any]) -> Dict[str, Any]:
    schema: Dict[str, Any] = {}
    for key, value in input_schema.items():
        if key == ALIASES:
            anonymized_aliases = []
            for alias in value:
                anonymized_aliases.append(anonymize_name(alias))
            schema[key] = anonymized_aliases

        elif key == DEFAULT:
            # If the value field for default is not a string do not alter it
            if not isinstance(value, str):
                schema[key] = value
            else:
                schema[key] = anonymize_name(value)

        elif key == DOC:
            continue  # Doc attribute may contain non-public and sensitive data

        elif key == FIELDS:
            fields = []
            for field in value:
                fields.append(anonymize(field))
            schema[key] = fields

        elif key == ITEMS:
            if isinstance(value, List):
                schema[key] = anonymize_union_type(value)
            elif isinstance(value, str) and value not in PRIMITIVE_TYPES:
                schema[key] = anonymize_name(value)
            else:
                schema[key] = value

        elif key == LOGICAL_TYPE:
            schema[key] = value

        elif key in [NAME, NAMESPACE]:
            schema[key] = anonymize_name(value)

        elif key == ORDER:
            schema[key] = value

        elif key == PRECISION:
            schema[key] = value

        elif key == SCALE:
            schema[key] = value

        elif key == SIZE:
            schema[key] = value

        elif key == SYMBOLS:
            anonymized_symbols = []
            for symbol in value:
                anonymized_symbols.append(anonymize_name(symbol))
            schema[key] = anonymized_symbols

        elif key == TYPE:
            if isinstance(value, list):
                # Union type
                schema[key] = anonymize_union_type(value)
            else:
                if value in ALL_TYPES:
                    schema[key] = value
                else:
                    # Nested type
                    schema[key] = anonymize(value)

        else:
            # Handle unknown keys
            if isinstance(key, str):
                # Anonymize the key as it is custom and can contain non-public and sensitive data
                key = anonymize_name(key)
            schema[key] = anonymize(value)

    return schema


def anonymize(input_schema: Union[str, Dict[str, Any], List]) -> Optional[Union[str, Dict[str, Any], List]]:
    if not input_schema:  # pylint: disable=no-else-return
        return input_schema
    elif isinstance(input_schema, str):
        if input_schema in PRIMITIVE_TYPES:
            return input_schema
        return anonymize_name(input_schema)
    elif isinstance(input_schema, List):
        return [anonymize(value) for value in input_schema]
    elif isinstance(input_schema, Dict):
        return anonymize_complex_type(input_schema)
    else:
        return None
