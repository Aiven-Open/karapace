"""
karapace - test the compiled Avro decoders

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Callable
from hypothesis import assume, given, HealthCheck, settings, strategies as st
from typing import Any, NamedTuple

import base64
import datetime
import decimal
import io
import itertools
import json
import math
import warnings

import avro.constants
import avro.errors
import avro.io
import avro.schema
import pytest

from karapace.core.avro_fast_decoder import build_decoder

SchemaDefinition = Any  # a str, a list (union) or a dict, i.e. anything json can hold


def _parse(schema_definition: SchemaDefinition) -> avro.schema.Schema:
    return avro.schema.parse(json.dumps(schema_definition))


def _encode(schema: avro.schema.Schema, datum: object) -> bytes:
    with io.BytesIO() as bio:
        avro.io.DatumWriter(schema).write(datum, avro.io.BinaryEncoder(bio))
        return bio.getvalue()


def _decode(schema: avro.schema.Schema, payload: bytes) -> object:
    return build_decoder(schema)(avro.io.BinaryDecoder(io.BytesIO(payload)))


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _jsonify(value: object) -> object:
    """Base64 the byte values of a DatumReader result, matching what the compiled decoders emit."""
    if isinstance(value, (bytes, bytearray)):
        return _b64(bytes(value))
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonify(item) for key, item in value.items()}
    return value


def _decode_with_datum_reader(
    schema: avro.schema.Schema, payload: bytes, reader_class: type = avro.io.DatumReader
) -> object:
    """Decode the way karapace did before the compiled decoders existed."""
    reader = reader_class(writers_schema=schema)
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


# ---------------------------------------------------------------------------
# Property based tests.
#
# The compiled decoders only exist to be a faster avro.io.DatumReader, so most
# properties are differential: for a random schema and a random payload the
# compiled decoder must agree with the stock reader on the decoded value, on
# the number of bytes consumed and on the failure mode. The round trip table
# states the expected values independently of avro on top of that.
# ---------------------------------------------------------------------------


class _Node(NamedTuple):
    """A generated schema definition together with a strategy for data valid under it."""

    definition: SchemaDefinition
    datums: st.SearchStrategy


_NAMED_TYPES = frozenset({"record", "enum", "fixed", "error"})
# Logical types whose python representation is a datetime.date, or a subclass of it.
_DATE_LIKE = frozenset({avro.constants.DATE, avro.constants.TIMESTAMP_MILLIS, avro.constants.TIMESTAMP_MICROS})

# Field, enum symbol and named type names have to be valid avro names.
_AVRO_NAMES = st.from_regex(r"[a-z][a-z0-9_]{0,4}", fullmatch=True)
_UTC_DATETIMES = st.datetimes().map(lambda value: value.replace(tzinfo=datetime.timezone.utc))
# Millisecond precision values only, so that writing them is not a lossy operation.
_MILLI_DATETIMES = _UTC_DATETIMES.map(lambda value: value.replace(microsecond=value.microsecond // 1000 * 1000))
_MILLI_TIMES = st.times().map(lambda value: value.replace(microsecond=value.microsecond // 1000 * 1000))


def _decimal_datums(precision: int, scale: int) -> st.SearchStrategy[decimal.Decimal]:
    """Decimals that fit in ``precision`` digits and have exactly ``scale`` fraction digits.

    Anything wider is rejected by the writer (AvroOutOfScaleException) or rounded by the reader,
    neither of which says anything about the decoders.
    """
    bound = 10**precision - 1
    return st.integers(min_value=-bound, max_value=bound).map(lambda unscaled: decimal.Decimal(unscaled).scaleb(-scale))


@st.composite
def _decimal_bytes_nodes(draw: st.DrawFn) -> _Node:
    precision = draw(st.integers(min_value=1, max_value=18))
    scale = draw(st.integers(min_value=0, max_value=precision))
    definition = {"type": "bytes", "logicalType": "decimal", "precision": precision, "scale": scale}
    return _Node(definition, _decimal_datums(precision, scale))


@st.composite
def _decimal_fixed_nodes(draw: st.DrawFn) -> _Node:
    size = draw(st.integers(min_value=1, max_value=8))
    # Mirrors avro.schema.FixedDecimalSchema, a larger precision makes avro drop the logical type.
    max_precision = int(math.floor(math.log10(2) * (8 * size - 1)))
    precision = draw(st.integers(min_value=1, max_value=max_precision))
    scale = draw(st.integers(min_value=0, max_value=precision))
    definition = {
        "type": "fixed",
        "name": "d",
        "size": size,
        "logicalType": "decimal",
        "precision": precision,
        "scale": scale,
    }
    return _Node(definition, _decimal_datums(precision, scale))


@st.composite
def _enum_nodes(draw: st.DrawFn) -> _Node:
    symbols = draw(st.lists(_AVRO_NAMES, min_size=1, max_size=4, unique=True))
    definition: dict = {"type": "enum", "name": "e", "symbols": symbols}
    if draw(st.booleans()):
        definition["default"] = symbols[0]
    return _Node(definition, st.sampled_from(symbols))


@st.composite
def _fixed_nodes(draw: st.DrawFn) -> _Node:
    size = draw(st.integers(min_value=1, max_value=6))
    return _Node({"type": "fixed", "name": "f", "size": size}, st.binary(min_size=size, max_size=size))


_LEAF_NODES = st.one_of(
    st.just(_Node("null", st.none())),
    st.just(_Node("boolean", st.booleans())),
    st.just(_Node("int", st.integers(min_value=-(2**31), max_value=2**31 - 1))),
    st.just(_Node("long", st.integers(min_value=-(2**63), max_value=2**63 - 1))),
    st.just(_Node("float", st.floats(width=32, allow_nan=False))),
    st.just(_Node("double", st.floats(allow_nan=False))),
    st.just(_Node("string", st.text(max_size=8))),
    st.just(_Node("bytes", st.binary(max_size=8))),
    st.just(_Node({"type": "int", "logicalType": avro.constants.DATE}, st.dates())),
    st.just(_Node({"type": "int", "logicalType": avro.constants.TIME_MILLIS}, _MILLI_TIMES)),
    st.just(_Node({"type": "long", "logicalType": avro.constants.TIME_MICROS}, st.times())),
    st.just(_Node({"type": "long", "logicalType": avro.constants.TIMESTAMP_MILLIS}, _MILLI_DATETIMES)),
    st.just(_Node({"type": "long", "logicalType": avro.constants.TIMESTAMP_MICROS}, _UTC_DATETIMES)),
    _decimal_bytes_nodes(),
    _decimal_fixed_nodes(),
    _enum_nodes(),
    _fixed_nodes(),
)


def _avro_type(definition: SchemaDefinition) -> str:
    return definition if isinstance(definition, str) else definition["type"]


def _ambiguous_group(definition: SchemaDefinition) -> str | None:
    """Group union branches that avro itself cannot keep apart, only one per group is generated.

    None of these are decoder limitations, they are limitations of the encoding side that would
    make the generated data impossible to write:

    * ``avro.schema.UnionSchema.validate`` returns the *first* branch that shallowly accepts the
      datum and ``avro.io.validate`` then validates that branch's children, so a record next to a
      map or next to another record makes perfectly valid data look invalid.
    * ``DatumWriter.write_union`` writes the datum with the *last* branch that accepts it. A
      datetime is also a date, so a date branch would be handed a datetime and crash in
      write_date_int, a 32 bit float branch accepts a value that only a double can hold, and any
      decimal branch accepts a decimal whose scale belongs to another branch.
    """
    avro_type = _avro_type(definition)
    if avro_type in ("record", "map"):
        return "mapping"  # both are a plain dict once written
    if avro_type in ("float", "double"):
        return "real"
    if isinstance(definition, dict):
        if definition.get("logicalType") == "decimal":
            return "decimal"  # bytes and fixed decimals are told apart by scale, which is not checked
        if definition.get("logicalType") in _DATE_LIKE:
            return "date-like"
    return None


def _array_node(item: _Node) -> _Node:
    return _Node({"type": "array", "items": item.definition}, st.lists(item.datums, max_size=3))


def _map_node(value: _Node) -> _Node:
    return _Node(
        {"type": "map", "values": value.definition},
        st.dictionaries(st.text(max_size=4), value.datums, max_size=3),
    )


def _record_node(fields: list[tuple[str, _Node]]) -> _Node:
    by_name = dict(fields)  # avro rejects a record that declares the same field name twice
    return _Node(
        {
            "type": "record",
            "name": "r",
            "fields": [{"name": name, "type": node.definition} for name, node in by_name.items()],
        },
        st.fixed_dictionaries({name: node.datums for name, node in by_name.items()}),
    )


def _union_node(candidates: list[_Node]) -> _Node:
    branches: list[_Node] = []
    seen_types: set[str] = set()
    seen_groups: set[str] = set()
    for node in candidates:
        if isinstance(node.definition, list):
            continue  # unions cannot contain unions
        avro_type = _avro_type(node.definition)
        if avro_type not in _NAMED_TYPES and avro_type in seen_types:
            continue  # only named types may repeat inside a union
        group = _ambiguous_group(node.definition)
        if group is not None and group in seen_groups:
            continue
        seen_types.add(avro_type)
        if group is not None:
            seen_groups.add(group)
        branches.append(node)
    if not branches:
        branches = [_Node("null", st.none())]
    return _Node([node.definition for node in branches], st.one_of([node.datums for node in branches]))


def _extend_nodes(children: st.SearchStrategy[_Node]) -> st.SearchStrategy[_Node]:
    return st.one_of(
        children.map(_array_node),
        children.map(_map_node),
        st.lists(st.tuples(_AVRO_NAMES, children), min_size=1, max_size=3).map(_record_node),
        st.lists(children, min_size=1, max_size=3).map(_union_node),
    )


_NODES = st.recursive(_LEAF_NODES, _extend_nodes, max_leaves=4)


def _with_unique_names(definition: SchemaDefinition, counter: itertools.count | None = None) -> SchemaDefinition:
    """Rename every generated record/enum/fixed, a schema may not declare the same name twice.

    The generated trees can hold the very same sub schema more than once, and names are global to
    a schema, so naming is fixed up here instead of while generating.
    """
    counter = itertools.count() if counter is None else counter
    if isinstance(definition, list):
        return [_with_unique_names(branch, counter) for branch in definition]
    if not isinstance(definition, dict):
        return definition
    copied = dict(definition)
    if copied["type"] in _NAMED_TYPES:
        copied["name"] = f"n{next(counter)}"
    if "items" in copied:
        copied["items"] = _with_unique_names(copied["items"], counter)
    if "values" in copied:
        copied["values"] = _with_unique_names(copied["values"], counter)
    if "fields" in copied:
        copied["fields"] = [dict(field, type=_with_unique_names(field["type"], counter)) for field in copied["fields"]]
    return copied


_SCHEMA_DEFINITIONS = _NODES.map(lambda node: _with_unique_names(node.definition))


@st.composite
def _schemas_and_data(draw: st.DrawFn) -> tuple[SchemaDefinition, object]:
    node = draw(_NODES)
    return _with_unique_names(node.definition), draw(node.datums)


def _min_encoded_bytes(definition: SchemaDefinition) -> int:
    """A lower bound on the number of bytes a value of this schema consumes."""
    if isinstance(definition, list):
        return 1  # the branch index
    if _avro_type(definition) == "null":
        return 0
    if isinstance(definition, dict) and definition["type"] == "record":
        return sum(_min_encoded_bytes(field["type"]) for field in definition["fields"])
    return 1


def _terminates_on_garbage(definition: SchemaDefinition) -> bool:
    """Whether decoding arbitrary bytes with this schema is guaranteed to stop.

    An array of zero width items (null, or a record of them) keeps appending until its block count
    is exhausted, and a random block count can be astronomically large. Both the compiled decoder
    and avro's own reader hang on that, so those schemas are kept out of the garbage input test.
    """
    if isinstance(definition, list):
        return all(_terminates_on_garbage(branch) for branch in definition)
    if isinstance(definition, str):
        return True
    avro_type = definition["type"]
    if avro_type == "array":
        return _min_encoded_bytes(definition["items"]) > 0 and _terminates_on_garbage(definition["items"])
    if avro_type == "map":
        return _terminates_on_garbage(definition["values"])
    if avro_type == "record":
        return all(_terminates_on_garbage(field["type"]) for field in definition["fields"])
    return True


def _same(left: object, right: object) -> bool:
    """Equality that also holds for NaN, which random floats and random bytes both produce."""
    if isinstance(left, float) and isinstance(right, float) and math.isnan(left) and math.isnan(right):
        return True
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(_same(a, b) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_same(value, right[key]) for key, value in left.items())
    return type(left) is type(right) and left == right


def _outcome(call: Callable[[], object]) -> tuple[str, object]:
    """Run ``call``, returning either its value or the type of the exception it raised."""
    try:
        return ("value", call())
    except Exception as exc:  # noqa: BLE001 - comparing failure modes is the point of this helper
        return ("error", type(exc))


def _decode_counting_consumed(schema: avro.schema.Schema, payload: bytes) -> tuple[object, int]:
    stream = io.BytesIO(payload)
    value = build_decoder(schema)(avro.io.BinaryDecoder(stream))
    return value, stream.tell()


_PROPERTY_SETTINGS = settings(max_examples=250, deadline=None, suppress_health_check=[HealthCheck.too_slow])

_ROUND_TRIPS = [
    pytest.param("null", st.none(), lambda datum: datum, id="null"),
    pytest.param("boolean", st.booleans(), lambda datum: datum, id="boolean"),
    pytest.param("int", st.integers(min_value=-(2**31), max_value=2**31 - 1), lambda datum: datum, id="int"),
    pytest.param("long", st.integers(min_value=-(2**63), max_value=2**63 - 1), lambda datum: datum, id="long"),
    pytest.param("float", st.floats(width=32, allow_nan=False), lambda datum: datum, id="float"),
    pytest.param("double", st.floats(allow_nan=False), lambda datum: datum, id="double"),
    pytest.param("string", st.text(max_size=32), lambda datum: datum, id="string"),
    pytest.param("bytes", st.binary(max_size=32), _b64, id="bytes"),
    pytest.param({"type": "fixed", "name": "f", "size": 4}, st.binary(min_size=4, max_size=4), _b64, id="fixed"),
    pytest.param(
        {"type": "enum", "name": "e", "symbols": ["low", "mid", "high"]},
        st.sampled_from(["low", "mid", "high"]),
        lambda datum: datum,
        id="enum",
    ),
    pytest.param({"type": "int", "logicalType": avro.constants.DATE}, st.dates(), lambda datum: datum, id="date"),
    pytest.param(
        {"type": "int", "logicalType": avro.constants.TIME_MILLIS}, _MILLI_TIMES, lambda datum: datum, id="time-millis"
    ),
    pytest.param(
        {"type": "long", "logicalType": avro.constants.TIME_MICROS}, st.times(), lambda datum: datum, id="time-micros"
    ),
    pytest.param(
        {"type": "long", "logicalType": avro.constants.TIMESTAMP_MILLIS},
        _MILLI_DATETIMES,
        lambda datum: datum,
        id="timestamp-millis",
    ),
    pytest.param(
        {"type": "long", "logicalType": avro.constants.TIMESTAMP_MICROS},
        _UTC_DATETIMES,
        lambda datum: datum,
        id="timestamp-micros",
    ),
    pytest.param(
        {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 4},
        _decimal_datums(12, 4),
        lambda datum: datum,
        id="decimal-bytes",
    ),
    pytest.param(
        {"type": "fixed", "name": "d", "size": 8, "logicalType": "decimal", "precision": 12, "scale": 4},
        _decimal_datums(12, 4),
        lambda datum: datum,
        id="decimal-fixed",
    ),
    pytest.param(
        {"type": "array", "items": "long"},
        st.lists(st.integers(min_value=-(2**63), max_value=2**63 - 1), max_size=16),
        lambda datum: datum,
        id="array",
    ),
    pytest.param(
        {"type": "map", "values": "bytes"},
        st.dictionaries(st.text(max_size=8), st.binary(max_size=8), max_size=8),
        lambda datum: {key: _b64(value) for key, value in datum.items()},
        id="map",
    ),
    pytest.param(
        {"type": "record", "name": "r", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": ["null", "int"]}]},
        st.fixed_dictionaries({"a": st.text(max_size=8), "b": st.none() | st.integers(min_value=0, max_value=2**31 - 1)}),
        lambda datum: datum,
        id="record",
    ),
]


@pytest.mark.parametrize("schema_definition,datums,expected", _ROUND_TRIPS)
@settings(max_examples=100, deadline=None)
@given(data=st.data())
def test_round_trip_per_type(
    schema_definition: SchemaDefinition,
    datums: st.SearchStrategy,
    expected: Callable[[Any], object],
    data: st.DataObject,
) -> None:
    """Encode with avro, decode with the compiled decoder, get the value back.

    Unlike the differential tests this states the expected result independently of avro's reader,
    the one deliberate difference being that byte values come out base64 encoded.
    """
    schema = _parse(schema_definition)
    datum = data.draw(datums)
    payload = _encode(schema, datum)

    decoded, consumed = _decode_counting_consumed(schema, payload)

    assert _same(decoded, expected(datum))
    assert consumed == len(payload)


@_PROPERTY_SETTINGS
@given(_schemas_and_data())
def test_random_schema_decodes_like_datum_reader(schema_and_datum: tuple[SchemaDefinition, object]) -> None:
    """The core property: same value as avro's own reader, and the whole payload consumed."""
    definition, datum = schema_and_datum
    schema = _parse(definition)
    payload = _encode(schema, datum)

    decoded, consumed = _decode_counting_consumed(schema, payload)

    assert _same(decoded, _decode_with_datum_reader(schema, payload)), definition
    assert consumed == len(payload), "the decoder read too few or too many bytes"


def _has_raw_bytes(value: object) -> bool:
    if isinstance(value, (bytes, bytearray)):
        return True
    if isinstance(value, list):
        return any(_has_raw_bytes(item) for item in value)
    if isinstance(value, dict):
        return any(_has_raw_bytes(item) for item in value.values())
    return False


@_PROPERTY_SETTINGS
@given(_schemas_and_data())
def test_decoded_values_never_contain_raw_bytes(schema_and_datum: tuple[SchemaDefinition, object]) -> None:
    """The REST proxy renders decoded values as json, so bytes and fixed have to be base64 strings."""
    definition, datum = schema_and_datum
    schema = _parse(definition)

    decoded = _decode(schema, _encode(schema, datum))

    assert not _has_raw_bytes(decoded), definition


@_PROPERTY_SETTINGS
@given(_schemas_and_data(), st.integers(min_value=2, max_value=4))
def test_one_compiled_decoder_reads_a_stream_of_values(
    schema_and_datum: tuple[SchemaDefinition, object], repeats: int
) -> None:
    """Serializers reuse a compiled decoder, so it must be stateless and stop at the value boundary."""
    definition, datum = schema_and_datum
    schema = _parse(definition)
    payload = _encode(schema, datum)
    expected = _decode_with_datum_reader(schema, payload)

    stream = io.BytesIO(payload * repeats)
    decoder = avro.io.BinaryDecoder(stream)
    decode = build_decoder(schema)

    for _ in range(repeats):
        assert _same(decode(decoder), expected)
    assert stream.read() == b""


class _StrictDatumReader(avro.io.DatumReader):
    """avro's reader with the out of range index handling of the compiled decoders.

    ``DatumReader.read_enum`` and ``DatumReader.read_union`` only check the upper bound of the
    index they read, so a negative one picks a symbol or a branch from the end of the list, or
    raises IndexError. The compiled decoders reject it instead, which is the single intended
    difference in behaviour, pinned by test_enum_index_outside_the_symbol_range and
    test_union_index_outside_the_branch_range_raises. Normalising it here keeps corrupt payloads
    comparable against the stock reader.

    A non negative index is rewound and handed to avro, so that everything else, including the
    fallback to the enum default, stays the stock implementation.
    """

    def read_enum(
        self,
        writers_schema: avro.schema.EnumSchema,
        readers_schema: avro.schema.EnumSchema,
        decoder: avro.io.BinaryDecoder,
    ) -> str:
        position = decoder.reader.tell()
        index = decoder.read_int()
        if index >= 0:
            decoder.reader.seek(position)
            return super().read_enum(writers_schema, readers_schema, decoder)
        if writers_schema.default is not None:
            return writers_schema.default
        raise avro.errors.SchemaResolutionException(f"Can't access enum index {index}", writers_schema)

    def read_union(
        self,
        writers_schema: avro.schema.UnionSchema,
        readers_schema: avro.schema.UnionSchema,
        decoder: avro.io.BinaryDecoder,
    ) -> object:
        position = decoder.reader.tell()
        index = decoder.read_long()
        if index >= 0:
            decoder.reader.seek(position)
            return super().read_union(writers_schema, readers_schema, decoder)
        raise avro.errors.SchemaResolutionException(f"Can't access branch index {index}", writers_schema)


@_PROPERTY_SETTINGS
@given(_SCHEMA_DEFINITIONS.filter(_terminates_on_garbage), st.binary(max_size=40))
def test_random_bytes_fail_like_datum_reader(definition: SchemaDefinition, payload: bytes) -> None:
    """Corrupt payloads reach the decoders: a stale schema id, a truncated message, a bad producer.

    Whatever the random bytes mean, the compiled decoder must return what avro's reader returns or
    fail the way it fails, never something else.
    """
    schema = _parse(definition)

    compiled = _outcome(lambda: _decode(schema, payload))
    reference = _outcome(lambda: _decode_with_datum_reader(schema, payload, reader_class=_StrictDatumReader))

    assert compiled[0] == reference[0], (definition, payload, compiled, reference)
    assert _same(compiled[1], reference[1]), (definition, payload, compiled, reference)


@_PROPERTY_SETTINGS
@given(_schemas_and_data(), st.data())
def test_truncated_payload_always_raises_invalid_binary_encoding(
    schema_and_datum: tuple[SchemaDefinition, object], data: st.DataObject
) -> None:
    """Every byte of a payload is consumed, so cutting any suffix off has to be detected.

    SchemaRegistrySerializer.deserialize turns this error into an InvalidPayload response.
    """
    definition, datum = schema_and_datum
    schema = _parse(definition)
    payload = _encode(schema, datum)
    assume(payload)  # a schema of only nulls encodes to nothing, there is nothing to cut

    truncated = payload[: data.draw(st.integers(min_value=0, max_value=len(payload) - 1))]

    with pytest.raises(avro.errors.InvalidAvroBinaryEncoding):
        _decode(schema, truncated)


@settings(max_examples=100, deadline=None)
@given(st.lists(st.integers(min_value=1, max_value=32), min_size=1, max_size=6), st.integers(min_value=0, max_value=64))
def test_recursive_schema_round_trip(values: list[int], extra: int) -> None:
    """A record referring to itself is decoded through the lazy decoder that breaks the cycle."""
    schema = _parse(RECURSIVE_SCHEMA)
    datum: dict | None = None
    for value in reversed(values):
        datum = {"value": value, "next": datum}
    payload = _encode(schema, datum)

    # Trailing bytes of a following message must not be touched.
    decoded, consumed = _decode_counting_consumed(schema, payload + bytes(extra))

    assert decoded == datum
    assert consumed == len(payload)


_LONGS = st.integers(min_value=-(2**40), max_value=2**40)
# A block is a list of items and whether the writer announces its size, which makes the item count
# negative and adds a byte size that the reader has to skip.
_ARRAY_BLOCKS = st.lists(st.tuples(st.lists(_LONGS, min_size=1, max_size=3), st.booleans()), max_size=4)
_MAP_BLOCKS = st.lists(
    st.tuples(st.dictionaries(st.text(max_size=3), _LONGS, min_size=1, max_size=3), st.booleans()),
    max_size=4,
)


def _write_blocks(blocks: list[tuple[list, bool]], write_item: Callable[[avro.io.BinaryEncoder, Any], None]) -> bytes:
    with io.BytesIO() as bio:
        encoder = avro.io.BinaryEncoder(bio)
        for items, announce_size in blocks:
            with io.BytesIO() as block_bio:
                block_encoder = avro.io.BinaryEncoder(block_bio)
                for item in items:
                    write_item(block_encoder, item)
                encoded_block = block_bio.getvalue()
            if announce_size:
                encoder.write_long(-len(items))
                encoder.write_long(len(encoded_block))
            else:
                encoder.write_long(len(items))
            encoder.write(encoded_block)
        encoder.write_long(0)
        return bio.getvalue()


@settings(max_examples=100, deadline=None)
@given(_ARRAY_BLOCKS)
def test_arbitrary_array_block_splits_decode_to_one_list(blocks: list[tuple[list[int], bool]]) -> None:
    """Writers may split an array into any number of blocks, and may prefix a block with its size."""
    schema = _parse({"type": "array", "items": "long"})
    payload = _write_blocks(blocks, lambda encoder, item: encoder.write_long(item))

    decoded, consumed = _decode_counting_consumed(schema, payload)

    assert decoded == [value for items, _ in blocks for value in items]
    assert consumed == len(payload)
    assert decoded == _decode_with_datum_reader(schema, payload)


@settings(max_examples=100, deadline=None)
@given(_MAP_BLOCKS)
def test_arbitrary_map_block_splits_decode_to_one_dict(blocks: list[tuple[dict[str, int], bool]]) -> None:
    schema = _parse({"type": "map", "values": "long"})

    def write_entry(encoder: avro.io.BinaryEncoder, entry: tuple[str, int]) -> None:
        key, value = entry
        encoder.write_utf8(key)
        encoder.write_long(value)

    payload = _write_blocks([(list(entries.items()), announce) for entries, announce in blocks], write_entry)

    decoded, consumed = _decode_counting_consumed(schema, payload)

    expected: dict[str, int] = {}
    for entries, _ in blocks:
        expected.update(entries)  # a later block wins, exactly like the decoder's dict assignment
    assert decoded == expected
    assert consumed == len(payload)
    assert decoded == _decode_with_datum_reader(schema, payload)


@settings(max_examples=100, deadline=None)
@given(_enum_nodes(), st.integers(min_value=-(2**31), max_value=2**31 - 1))
def test_enum_index_outside_the_symbol_range(node: _Node, index: int) -> None:
    """An index avro never writes must give the default symbol, or a resolution error.

    A negative index is included: avro's own reader would take a symbol from the end of the list.
    """
    schema = _parse(node.definition)
    payload = _encode(_parse("int"), index)
    symbols = node.definition["symbols"]
    default = node.definition.get("default")

    if 0 <= index < len(symbols):
        assert _decode(schema, payload) == symbols[index]
    elif default is not None:
        assert _decode(schema, payload) == default
    else:
        with pytest.raises(avro.errors.SchemaResolutionException):
            _decode(schema, payload)


@settings(max_examples=100, deadline=None)
@given(st.integers(min_value=-(2**63), max_value=2**63 - 1))
def test_union_index_outside_the_branch_range_raises(index: int) -> None:
    """Unlike avro's reader, a negative branch index is rejected instead of counting from the end."""
    definition = ["null", "string", "long"]
    assume(not 0 <= index < len(definition))
    schema = _parse(definition)
    payload = _encode(_parse("long"), index)

    with pytest.raises(avro.errors.SchemaResolutionException):
        _decode(schema, payload)
