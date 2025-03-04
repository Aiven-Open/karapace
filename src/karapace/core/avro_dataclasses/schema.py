"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, TypeAlias
from typing_extensions import NotRequired, TypedDict

Primitive: TypeAlias = Literal["int", "long", "string", "null", "bytes", "boolean"]
LogicalType: TypeAlias = Literal["timestamp-millis", "uuid"]


class TypeObject(TypedDict):
    type: Primitive
    logicalType: LogicalType


class ArrayType(TypedDict):
    name: str
    type: Literal["array"]
    items: AvroType


class EnumType(TypedDict):
    name: str
    type: Literal["enum"]
    symbols: list[str]
    default: NotRequired[str]


class MapType(TypedDict):
    type: Literal["map"]
    values: AvroType
    default: NotRequired[Mapping[str, AvroType]]


TypeUnit: TypeAlias = "Primitive | TypeObject"
UnionType: TypeAlias = "list[TypeUnit]"
AvroType: TypeAlias = "TypeUnit | UnionType | RecordSchema | ArrayType | EnumType | MapType"


class FieldSchema(TypedDict):
    name: str
    type: AvroType
    default: NotRequired[str | int | bytes | None]


class RecordSchema(TypedDict):
    name: str
    type: Literal["record"]
    fields: list[FieldSchema]
