"""
karapace - test the compiled Avro decoders

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

import base64
import datetime
import decimal
import io
import json
import warnings

import avro.errors
import avro.io
import avro.schema
import pytest

from karapace.core.avro_fast_decoder import build_decoder


def _parse(schema_definition: dict) -> avro.schema.Schema:
    return avro.schema.parse(json.dumps(schema_definition))


def _encode(schema: avro.schema.Schema, datum: object) -> bytes:
    with io.BytesIO() as bio:
        avro.io.DatumWriter(schema).write(datum, avro.io.BinaryEncoder(bio))
        return bio.getvalue()


def _decode(schema: avro.schema.Schema, payload: bytes) -> object:
    return build_decoder(schema)(avro.io.BinaryDecoder(io.BytesIO(payload)))


def _jsonify(value: object) -> object:
    """Base64 the byte values of a DatumReader result, matching what the compiled decoders emit."""
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonify(item) for key, item in value.items()}
    return value


def _decode_with_datum_reader(schema: avro.schema.Schema, payload: bytes) -> object:
    """Decode the way karapace did before the compiled decoders existed."""
    reader = avro.io.DatumReader(writers_schema=schema)
    return _jsonify(reader.read(avro.io.BinaryDecoder(io.BytesIO(payload))))


SCALARS_SCHEMA = {
    "type": "record",
    "name": "Scalars",
    "fields": [
        {"name": "nothing", "type": "null"},
        {"name": "flag", "type": "boolean"},
        {"name": "text", "type": "string"},
        {"name": "small", "type": "int"},
        {"name": "big", "type": "long"},
        {"name": "single", "type": "float"},
        {"name": "wide", "type": "double"},
        {"name": "raw", "type": "bytes"},
    ],
}

COMPLEX_SCHEMA = {
    "type": "record",
    "name": "Complex",
    "fields": [
        {"name": "id", "type": "string"},
        {
            "name": "attrs",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Attr",
                    "fields": [
                        {"name": "k", "type": "string"},
                        {"name": "v", "type": ["null", "string", "int", "long", "float", "double", "boolean", "bytes"]},
                    ],
                },
            },
        },
        {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
        {"name": "matrix", "type": {"type": "array", "items": {"type": "array", "items": "long"}}},
        {"name": "grouped", "type": {"type": "map", "values": {"type": "map", "values": "string"}}},
        {"name": "level", "type": {"type": "enum", "name": "Level", "symbols": ["LOW", "HIGH"]}},
        {"name": "digest", "type": {"type": "fixed", "name": "Digest", "size": 4}},
    ],
}

LOGICAL_TYPES_SCHEMA = {
    "type": "record",
    "name": "LogicalTypes",
    "fields": [
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "time_ms", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "time_us", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "ts_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "ts_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
        {
            "name": "fixed_amount",
            "type": {"type": "fixed", "name": "Money", "size": 4, "logicalType": "decimal", "precision": 9, "scale": 2},
        },
    ],
}

RECURSIVE_SCHEMA = {
    "type": "record",
    "name": "Node",
    "fields": [
        {"name": "value", "type": "int"},
        {"name": "next", "type": ["null", "Node"]},
    ],
}


@pytest.mark.parametrize(
    "schema_definition,datum",
    [
        pytest.param(
            SCALARS_SCHEMA,
            {
                "nothing": None,
                "flag": True,
                "text": "hyvää päivää",
                "small": -42,
                "big": 2**40,
                "single": 1.5,
                "wide": 3.25,
                "raw": b"\x00\x01\xff",
            },
            id="scalars",
        ),
        pytest.param(
            COMPLEX_SCHEMA,
            {
                "id": "one",
                "attrs": [
                    {"k": "none", "v": None},
                    {"k": "text", "v": "value"},
                    {"k": "count", "v": 5},
                    {"k": "ratio", "v": 1.5},
                    {"k": "flag", "v": True},
                    {"k": "blob", "v": b"\x01\x02\x03"},
                ],
                "props": {"present": "yes", "missing": None},
                "matrix": [[1, 2], [], [3]],
                "grouped": {"a": {"b": "c"}, "empty": {}},
                "level": "HIGH",
                "digest": b"abcd",
            },
            id="unions-collections-enum-fixed",
        ),
        pytest.param(
            LOGICAL_TYPES_SCHEMA,
            {
                "day": datetime.date(2024, 5, 1),
                "time_ms": datetime.time(12, 30, 15, 250000),
                "time_us": datetime.time(12, 30, 15, 250123),
                "ts_ms": datetime.datetime(2024, 5, 1, 12, 30, 15, 250000, tzinfo=datetime.timezone.utc),
                "ts_us": datetime.datetime(2024, 5, 1, 12, 30, 15, 250123, tzinfo=datetime.timezone.utc),
                "amount": decimal.Decimal("12.34"),
                "fixed_amount": decimal.Decimal("-12.34"),
            },
            id="logical-types",
        ),
        pytest.param(
            RECURSIVE_SCHEMA,
            {"value": 1, "next": {"value": 2, "next": {"value": 3, "next": None}}},
            id="recursive",
        ),
    ],
)
def test_compiled_decoder_matches_datum_reader(schema_definition: dict, datum: dict) -> None:
    """Pin the compiled decoders to the stock reader so an avro upgrade cannot silently diverge."""
    schema = _parse(schema_definition)
    payload = _encode(schema, datum)

    assert _decode(schema, payload) == _decode_with_datum_reader(schema, payload)


def test_bytes_and_fixed_are_base64_encoded() -> None:
    """The REST proxy emits JSON, so byte values must come out as base64 strings."""
    schema = _parse(
        {
            "type": "record",
            "name": "Bytes",
            "fields": [
                {"name": "raw", "type": "bytes"},
                {"name": "digest", "type": {"type": "fixed", "name": "Digest", "size": 3}},
                {"name": "maybe", "type": ["null", "bytes"]},
                {"name": "items", "type": {"type": "array", "items": "bytes"}},
            ],
        }
    )
    payload = _encode(schema, {"raw": b"\x01\x02", "digest": b"abc", "maybe": b"\xff", "items": [b"a", b"b"]})

    assert _decode(schema, payload) == {
        "raw": base64.b64encode(b"\x01\x02").decode("ascii"),
        "digest": base64.b64encode(b"abc").decode("ascii"),
        "maybe": base64.b64encode(b"\xff").decode("ascii"),
        "items": [base64.b64encode(b"a").decode("ascii"), base64.b64encode(b"b").decode("ascii")],
    }


def test_blocked_array_and_map_encoding() -> None:
    """Arrays and maps may be written as multiple blocks, negative counts carrying a byte size."""
    schema = _parse(
        {
            "type": "record",
            "name": "Blocked",
            "fields": [
                {"name": "numbers", "type": {"type": "array", "items": "long"}},
                {"name": "props", "type": {"type": "map", "values": "long"}},
            ],
        }
    )
    with io.BytesIO() as bio:
        encoder = avro.io.BinaryEncoder(bio)
        # An array as two blocks, the first one announcing its byte size with a negative count.
        encoder.write_long(-2)
        encoder.write_long(2)  # block byte size, must be read and ignored
        encoder.write_long(1)
        encoder.write_long(2)
        encoder.write_long(1)
        encoder.write_long(3)
        encoder.write_long(0)
        # The same for a map.
        encoder.write_long(-1)
        encoder.write_long(4)
        encoder.write_utf8("a")
        encoder.write_long(1)
        encoder.write_long(1)
        encoder.write_utf8("b")
        encoder.write_long(2)
        encoder.write_long(0)
        payload = bio.getvalue()

    assert _decode(schema, payload) == {"numbers": [1, 2, 3], "props": {"a": 1, "b": 2}}
    assert _decode(schema, payload) == _decode_with_datum_reader(schema, payload)


def test_enum_index_out_of_range_falls_back_to_default() -> None:
    """Behaviour of the patched avro fork we pin (aiven/avro), mirrored by the compiled decoder."""
    schema = _parse({"type": "enum", "name": "E", "symbols": ["A", "B"], "default": "A"})
    payload = _encode(_parse({"type": "int"}), 9)  # index 9, only 2 symbols

    assert _decode(schema, payload) == "A"
    assert _decode_with_datum_reader(schema, payload) == "A"


def test_enum_index_out_of_range_without_default_raises() -> None:
    schema = _parse({"type": "enum", "name": "E", "symbols": ["A", "B"]})
    payload = _encode(_parse({"type": "int"}), 9)

    with pytest.raises(avro.errors.SchemaResolutionException):
        _decode(schema, payload)
    with pytest.raises(avro.errors.SchemaResolutionException):
        _decode_with_datum_reader(schema, payload)


def test_union_index_out_of_range_raises() -> None:
    schema = _parse({"type": "record", "name": "U", "fields": [{"name": "v", "type": ["null", "string"]}]})
    payload = _encode(_parse({"type": "long"}), 7)  # branch 7, only 2 branches

    with pytest.raises(avro.errors.SchemaResolutionException):
        _decode(schema, payload)
    with pytest.raises(avro.errors.SchemaResolutionException):
        _decode_with_datum_reader(schema, payload)


def test_truncated_payload_raises_invalid_binary_encoding() -> None:
    """SchemaRegistrySerializer.deserialize maps this error to InvalidPayload."""
    schema = _parse({"type": "record", "name": "T", "fields": [{"name": "text", "type": "string"}]})
    payload = _encode(schema, {"text": "hello"})[:2]

    with pytest.raises(avro.errors.InvalidAvroBinaryEncoding):
        _decode(schema, payload)


@pytest.mark.parametrize(
    "schema_definition",
    [
        pytest.param({"type": "bytes", "logicalType": "decimal", "precision": -1, "scale": 2}, id="bytes-bad-precision"),
        pytest.param({"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": -2}, id="bytes-bad-scale"),
        pytest.param({"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 9}, id="bytes-scale-gt-precision"),
        pytest.param(
            {"type": "fixed", "name": "F", "size": 4, "logicalType": "decimal", "precision": 20, "scale": 2},
            id="fixed-precision-too-large-for-size",
        ),
    ],
)
def test_invalid_decimal_annotation_is_dropped_by_the_schema_parser(schema_definition: dict) -> None:
    """Why the decoders do not revalidate precision/scale: avro drops the logical type at parse time."""
    with pytest.warns(avro.errors.IgnoredLogicalType):
        schema = _parse(schema_definition)

    assert getattr(schema, "logical_type", None) is None

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # decoding must not warn, the schema parser already did
        if schema.type == "bytes":
            payload = _encode(schema, b"\x04\xd2")
        else:
            payload = _encode(schema, b"\x00\x00\x04\xd2")
        decoded = _decode(schema, payload)

    assert decoded == _decode_with_datum_reader(schema, payload)
    assert isinstance(decoded, str)  # falls back to plain bytes, base64 encoded


def test_unknown_schema_type_raises() -> None:
    class UnknownSchema:
        type = "quantum"

    with pytest.raises(avro.errors.AvroException, match="quantum"):
        build_decoder(UnknownSchema())  # type: ignore[arg-type]
