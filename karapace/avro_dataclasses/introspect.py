"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from .schema import AvroType, FieldSchema, RecordSchema
from dataclasses import Field, fields, is_dataclass, MISSING
from enum import Enum
from functools import lru_cache
from typing import Final, Sequence, TYPE_CHECKING, TypeVar, Union

# Note: It's important get_args and get_origin are imported from typing_extensions
# until support for Python 3.8 is dropped.
from typing_extensions import get_args, get_origin

import datetime
import uuid

if TYPE_CHECKING:
    from _typeshed import DataclassInstance
else:

    class DataclassInstance:
        ...


class UnsupportedAnnotation(NotImplementedError):
    ...


class UnderspecifiedArray(UnsupportedAnnotation):
    ...


def _field_type_array(field: Field, origin: type, type_: object) -> AvroType:
    if origin is tuple:
        try:
            inner_type, ellipsis = get_args(type_)
        except ValueError as e:
            raise UnsupportedAnnotation("Only homogenous tuples are supported") from e
        if ellipsis is not Ellipsis:
            raise UnsupportedAnnotation("Only homogenous tuples are supported")
    else:
        (inner_type,) = get_args(type_)

    return {
        "name": f"one_of_{field.name}",
        "type": "array",
        "items": (record_schema(inner_type) if is_dataclass(inner_type) else _field_type(field, inner_type)),
    }


sequence_types: Final = frozenset({tuple, list, Sequence})


def _field_type(field: Field, type_: object) -> AvroType:  # pylint: disable=too-many-return-statements
    # Handle primitives.
    if type_ is bool:
        return "boolean"
    if type_ is str:
        return "string"
    if type_ is int:
        type_ = field.metadata.get("type", "int")
        if type_ not in ("int", "long"):
            raise UnsupportedAnnotation(f"Invalid avro type for int: {type_!r}")
        return type_  # type: ignore[return-value]
    if type_ is bytes:
        return "bytes"
    if type_ is type(None) or type_ is None:  # noqa: E721
        return "null"

    # Handle logical types.
    if type_ is datetime.datetime:
        return {
            "logicalType": "timestamp-millis",
            "type": "long",
        }
    if type_ is uuid.UUID:
        return {
            "logicalType": "uuid",
            "type": "string",
        }

    origin = get_origin(type_)

    # Handle union types.
    if origin is Union:
        return [_field_type(field, unit) for unit in get_args(type_)]  # type: ignore[misc]

    # Handle array types.
    if origin in sequence_types:
        return _field_type_array(field, origin, type_)
    if type_ in sequence_types:
        raise UnderspecifiedArray("Inner type must be specified for sequence types")

    # Handle enums.
    if isinstance(type_, type) and issubclass(type_, Enum):
        return FieldSchema(
            {
                # Conditionally set a default.
                **({"default": field.default.value} if field.default is not MISSING else {}),  # type: ignore[misc]
                "name": type_.__name__,
                "type": "enum",
                "symbols": [value.value for value in type_],
            }
        )

    raise NotImplementedError(
        f"Found an unknown type {type_!r} while assembling Avro schema for the field "
        f"{field.name!r}. The Avro dataclasses implementation likely needs to be "
        f"updated to support this."
    )


T = TypeVar("T")


def transform_default(type_: type[T], default: T) -> object:
    if isinstance(type_, type) and issubclass(type_, Enum):
        return default.value  # type: ignore[attr-defined]
    return default


def field_schema(field: Field) -> FieldSchema:
    schema: FieldSchema = {
        "name": field.name,
        "type": _field_type(field, field.type),
    }
    return (
        {
            **schema,  # type: ignore[misc]
            "default": transform_default(field.type, field.default),
        }
        if field.default is not MISSING
        else schema
    )


@lru_cache
def record_schema(record_type: type[DataclassInstance]) -> RecordSchema:
    return {
        "name": record_type.__name__,
        "type": "record",
        "fields": [field_schema(field) for field in fields(record_type)],
    }
