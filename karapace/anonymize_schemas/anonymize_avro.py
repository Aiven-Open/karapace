"""
karapace - anonymize avro

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from typing import Any, Dict, List, Union

import hashlib
import logging

ALIASES = "aliases"
DEFAULT = "default"
DOC = "doc"
ENUM = "enum"
FIELDS = "fields"
ITEMS = "items"
NAME = "name"
NAMESPACE = "namespace"
ORDER = "order"
SIZE = "size"
SYMBOLS = "symbols"
TYPE = "type"

PRIMITIVE_TYPES = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]


def anonymize_name(name: Any) -> Any:
    """Anonymize the name.

    Name is splitted by dot to logical elements that the whole name consists of.
    The element is sha1 hashed. After hashing element is cleaned to conform to Avro name
    format of:
      * start with [A-Za-z_]
      * subsequently contain only [A-Za-z0-9_]

    Returns anonymized name.
    """

    if not isinstance(name, str):
        return name

    anonymized_elements = []
    for element in name.split("."):
        element = hashlib.sha1(element.encode("utf-8")).hexdigest()
        # SHA-1 can start with number, just add 'a' as first character.
        # This breaks the hash, but is still consistent as required.
        as_list_element = list(element)
        as_list_element[0] = "a"
        anonymized_elements.append("".join(as_list_element))

    return ".".join(anonymized_elements)


def anonymize_type(maybe_type_list_or_str: Union[str, List[Any]]) -> Union[str, List[Any]]:
    if isinstance(maybe_type_list_or_str, list):
        union_types = []
        for union_type in maybe_type_list_or_str:
            if union_type in PRIMITIVE_TYPES:
                union_types.append(union_type)
            elif isinstance(union_type, str):
                union_types.append(anonymize_name(union_type))
            else:
                union_types.append(anonymize(union_type))
        return union_types
    return maybe_type_list_or_str


def anonymize_schema(type_element: Union[str, Dict[str, str]], input_schema: Dict[str, Any]) -> Dict[str, Any]:
    schema: Dict[str, Any] = {}
    for key, value in input_schema.items():
        anonymized: Union[str, List]
        if key == ALIASES:
            anonymized = []
            for alias in value:
                anonymized.append(anonymize_name(alias))
            schema[key] = anonymized

        if key == DEFAULT:
            if type_element == ENUM:
                schema[key] = anonymize_name(value)
            else:
                schema[key] = value

        if key == DOC:
            continue  # Doc attribute may contain non-public and sensitive data

        if key == FIELDS:
            fields = []
            for field in value:
                fields.append(anonymize(field))
            schema[key] = fields

        if key == ITEMS:
            schema[key] = anonymize_type(value)

        if key in [NAME, NAMESPACE]:
            anonymized = anonymize_name(value)
            schema[key] = anonymized

        if key == ORDER:
            schema[key] = value

        if key == SIZE:
            schema[key] = value

        if key == SYMBOLS:
            anonymized_symbols = []
            for symbol in value:
                anonymized_symbols.append(anonymize_name(symbol))
            schema[key] = anonymized_symbols

        if key == TYPE:
            if isinstance(value, list):
                # Union type
                schema[key] = anonymize_type(value)
            else:
                # Nested type
                schema[key] = anonymize(value)

    return schema


def anonymize(input_schema: Union[str, Dict[str, Any], List]) -> Union[str, Dict[str, Any], List]:
    if not input_schema:
        return {}

    if isinstance(input_schema, str):
        # In this case the schema is of primitive type, e.g. "int"
        return input_schema

    if isinstance(input_schema, List):
        schema_list = []
        for schema_element in input_schema:
            schema = anonymize(schema_element)
            schema_list.append(schema)
        return schema_list

    type_element = input_schema.get("type", None)
    if not type_element:
        logging.warning("No type for element.")
        return {}

    if isinstance(type_element, Dict):
        input_schema.pop("type")
        schema = anonymize_schema(input_schema, input_schema)
        schema.update({"type": anonymize_schema(type_element, type_element)})
        return schema
    return anonymize_schema(type_element, input_schema)
