"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from .schema import AvroType, EnumType, FieldSchema, MapType, RecordSchema
from collections.abc import Mapping, Sequence
from dataclasses import Field, fields, is_dataclass, MISSING
from enum import Enum
from functools import lru_cache
from typing import Final, get_args, get_origin, TYPE_CHECKING, TypeVar, Union

import datetime
import uuid

if TYPE_CHECKING:
    from _typeshed import DataclassInstance
else:

    class DataclassInstance:
        ...


class UnsupportedAnnotation(NotImplementedError):
    ...


class UnderspecifiedAnnotation(UnsupportedAnnotation):
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

    items: AvroType
    if is_dataclass(inner_type):
        assert isinstance(inner_type, type)
        items = record_schema(inner_type)
    else:
        items = _field_type(field, inner_type)

    return {
        "name": f"one_of_{field.name}",
        "type": "array",
        "items": items,
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
        raise UnderspecifiedAnnotation("Inner type must be specified for sequence types")

    # Handle enums.
    if isinstance(type_, type) and issubclass(type_, Enum):
        enum_dict: EnumType = {
            "name": type_.__name__,
            "type": "enum",
            "symbols": [value.value for value in type_],
        }
        if field.default is not MISSING:
            enum_dict["default"] = field.default.value
        return enum_dict

    # Handle map types.
    if origin is Mapping:
        args = get_args(type_)
        if len(args) != 2:
            raise UnderspecifiedAnnotation("Key and value types must be specified for map types")
        if args[0] is not str:
            raise UnsupportedAnnotation("Key type must be str")
        map_dict: MapType = {
            "type": "map",
            "values": _field_type(field, args[1]),
        }
        if field.default_factory is not MISSING:
            map_dict["default"] = field.default_factory()
        return map_dict

    raise NotImplementedError(
        f"Found an unknown type {type_!r} while assembling Avro schema for the field "
        f"{field.name!r}. The Avro dataclasses implementation likely needs to be "
        f"updated to support this."
    )


T = TypeVar("T", str, int, bool, Enum, None)


def transform_default(type_: type[T] | str, default: T) -> str | int | bool | None:
    if isinstance(default, Enum):
        assert isinstance(type_, type)
        assert issubclass(type_, Enum)
        assert isinstance(default.value, (str, int, bool)) or default.value is None
        return default.value
    assert not (isinstance(type_, type) and issubclass(type_, Enum))
    return default


def field_schema(field: Field) -> FieldSchema:
    schema: FieldSchema = {
        "name": field.name,
        "type": _field_type(field, field.type),
    }
    return (
        {
            **schema,
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
