"""
karapace - compile an Avro schema into a tree of decoder closures for single-schema decoding

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from avro.io import BinaryDecoder
from collections.abc import Callable
from typing import Any, cast

import avro.constants
import avro.errors
import avro.schema
import base64

_b64encode = base64.b64encode

_DATE = avro.constants.DATE
_TIME_MILLIS = avro.constants.TIME_MILLIS
_TIME_MICROS = avro.constants.TIME_MICROS
_TIMESTAMP_MILLIS = avro.constants.TIMESTAMP_MILLIS
_TIMESTAMP_MICROS = avro.constants.TIMESTAMP_MICROS

Decoder = Callable[[BinaryDecoder], Any]


class _LazyDecoder:
    __slots__ = ("fn",)

    def __init__(self) -> None:
        self.fn: Decoder | None = None

    def __call__(self, decoder: BinaryDecoder) -> Any:
        return self.fn(decoder)  # type: ignore[misc]


def _decimal_props(schema: avro.schema.Schema) -> tuple[int, int]:
    """Return the (precision, scale) of a decimal schema.

    Only call this when ``schema.logical_type == "decimal"``: avro's schema parsing sets the
    logical type only after validating precision and scale (see avro.schema.DecimalLogicalSchema),
    and warns and leaves the logical type unset for an invalid annotation. So the properties need
    no revalidation here, they are known to be integers.
    """
    return cast(int, schema.get_prop("precision")), cast(int, schema.get_prop("scale"))


def _build_bytes_decoder(schema: avro.schema.Schema) -> Decoder:
    if getattr(schema, "logical_type", None) == "decimal":
        precision, scale = _decimal_props(schema)

        def decode_decimal(decoder: BinaryDecoder, _p: int = precision, _s: int = scale) -> Any:
            return decoder.read_decimal_from_bytes(_p, _s)

        return decode_decimal

    def decode_bytes(decoder: BinaryDecoder) -> str:
        return _b64encode(decoder.read_bytes()).decode("ascii")

    return decode_bytes


def _build_fixed_decoder(schema: avro.schema.FixedSchema) -> Decoder:
    size = schema.size
    if getattr(schema, "logical_type", None) == "decimal":
        precision, scale = _decimal_props(schema)

        def decode_decimal(decoder: BinaryDecoder, _p: int = precision, _s: int = scale, _sz: int = size) -> Any:
            return decoder.read_decimal_from_fixed(_p, _s, _sz)

        return decode_decimal

    def decode_fixed(decoder: BinaryDecoder, _size: int = size) -> str:
        return _b64encode(decoder.read(_size)).decode("ascii")

    return decode_fixed


def _build_enum_decoder(schema: avro.schema.EnumSchema) -> Decoder:
    symbols = schema.symbols
    n = len(symbols)
    default = schema.default

    def decode_enum(decoder: BinaryDecoder) -> str:
        index = decoder.read_int()
        if 0 <= index < n:
            return symbols[index]
        if default is not None:
            return default
        raise avro.errors.SchemaResolutionException(f"Can't access enum index {index} for enum with {n} symbols", schema)

    return decode_enum


def _build_record_decoder(schema: avro.schema.RecordSchema, memo: dict[int, Decoder]) -> Decoder:
    existing = memo.get(id(schema))
    if existing is not None:
        return existing

    lazy = _LazyDecoder()
    memo[id(schema)] = lazy
    field_decoders = tuple((field.name, build_decoder(field.type, memo)) for field in schema.fields)

    def decode_record(decoder: BinaryDecoder, _fields: tuple = field_decoders) -> dict:
        return {name: fn(decoder) for name, fn in _fields}

    lazy.fn = decode_record
    memo[id(schema)] = decode_record
    return decode_record


def _build_union_decoder(schema: avro.schema.UnionSchema, memo: dict[int, Decoder]) -> Decoder:
    branch_decoders = tuple(build_decoder(branch, memo) for branch in schema.schemas)
    n = len(branch_decoders)

    def decode_union(decoder: BinaryDecoder, _branches: tuple = branch_decoders, _n: int = n) -> Any:
        index = decoder.read_long()
        if not (0 <= index < _n):
            raise avro.errors.SchemaResolutionException(
                f"Can't access branch index {index} for union with {_n} branches", schema
            )
        return _branches[index](decoder)

    return decode_union


def _build_array_decoder(schema: avro.schema.ArraySchema, memo: dict[int, Decoder]) -> Decoder:
    item_decoder = build_decoder(schema.items, memo)

    def decode_array(decoder: BinaryDecoder, _item: Decoder = item_decoder) -> list:
        read_items: list = []
        append = read_items.append
        read_long = decoder.read_long
        block_count = read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                read_long()
            for _ in range(block_count):
                append(_item(decoder))
            block_count = read_long()
        return read_items

    return decode_array


def _build_map_decoder(schema: avro.schema.MapSchema, memo: dict[int, Decoder]) -> Decoder:
    value_decoder = build_decoder(schema.values, memo)

    def decode_map(decoder: BinaryDecoder, _value: Decoder = value_decoder) -> dict:
        read_items: dict = {}
        read_long = decoder.read_long
        read_utf8 = decoder.read_utf8
        block_count = read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                read_long()
            for _ in range(block_count):
                key = read_utf8()
                read_items[key] = _value(decoder)
            block_count = read_long()
        return read_items

    return decode_map


def _decode_null(decoder: BinaryDecoder) -> None:
    return None


def _decode_boolean(decoder: BinaryDecoder) -> bool:
    return decoder.read_boolean()


def _decode_string(decoder: BinaryDecoder) -> str:
    return decoder.read_utf8()


def _decode_float(decoder: BinaryDecoder) -> float:
    return decoder.read_float()


def _decode_double(decoder: BinaryDecoder) -> float:
    return decoder.read_double()


def _build_int_decoder(schema: avro.schema.Schema) -> Decoder:
    logical_type = getattr(schema, "logical_type", None)
    if logical_type == _DATE:
        return lambda decoder: decoder.read_date_from_int()
    if logical_type == _TIME_MILLIS:
        return lambda decoder: decoder.read_time_millis_from_int()
    return lambda decoder: decoder.read_int()


def _build_long_decoder(schema: avro.schema.Schema) -> Decoder:
    logical_type = getattr(schema, "logical_type", None)
    if logical_type == _TIME_MICROS:
        return lambda decoder: decoder.read_time_micros_from_long()
    if logical_type == _TIMESTAMP_MILLIS:
        return lambda decoder: decoder.read_timestamp_millis_from_long()
    if logical_type == _TIMESTAMP_MICROS:
        return lambda decoder: decoder.read_timestamp_micros_from_long()
    return lambda decoder: decoder.read_long()


_SIMPLE_BUILDERS: dict[str, Decoder] = {
    "null": _decode_null,
    "boolean": _decode_boolean,
    "string": _decode_string,
    "float": _decode_float,
    "double": _decode_double,
}


def build_decoder(schema: avro.schema.Schema, memo: dict[int, Decoder] | None = None) -> Decoder:
    """Compile ``schema`` into a ``(BinaryDecoder) -> value`` decoder, reusing ``memo`` for cycles."""
    if memo is None:
        memo = {}
    schema_type = schema.type

    simple = _SIMPLE_BUILDERS.get(schema_type)
    if simple is not None:
        return simple
    if schema_type == "int":
        return _build_int_decoder(schema)
    if schema_type == "long":
        return _build_long_decoder(schema)
    if schema_type == "bytes":
        return _build_bytes_decoder(schema)
    if isinstance(schema, avro.schema.RecordSchema):
        return _build_record_decoder(schema, memo)
    if isinstance(schema, avro.schema.UnionSchema):
        return _build_union_decoder(schema, memo)
    if isinstance(schema, avro.schema.ArraySchema):
        return _build_array_decoder(schema, memo)
    if isinstance(schema, avro.schema.MapSchema):
        return _build_map_decoder(schema, memo)
    if isinstance(schema, avro.schema.EnumSchema):
        return _build_enum_decoder(schema)
    if isinstance(schema, avro.schema.FixedSchema):
        return _build_fixed_decoder(schema)
    raise avro.errors.AvroException(f"Cannot build decoder for unknown schema type: {schema_type}")
