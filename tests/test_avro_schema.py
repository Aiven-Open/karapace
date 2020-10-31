"""
    These are duplicates of other test_schema.py tests, but do not make use of the registry client fixture
    and are here for debugging and speed, and as an initial sanity check
"""
from avro.schema import Parse
from karapace.avro_compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType

import json
import pytest


def test_simple_schema_promotion():
    reader = Parse(json.dumps({"name": "foo", "type": "record", "fields": [{"type": "int", "name": "f1"}]}))
    writer = Parse(
        json.dumps({
            "name": "foo",
            "type": "record",
            "fields": [{
                "type": "int",
                "name": "f1"
            }, {
                "type": "string",
                "name": "f2"
            }]
        })
    )
    res = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
    assert res.compatibility is SchemaCompatibilityType.compatible, res
    res = ReaderWriterCompatibilityChecker().get_compatibility(writer, reader)
    assert res.compatibility is SchemaCompatibilityType.incompatible, res


@pytest.mark.parametrize(
    "field", [
        {
            "type": {
                "type": "array",
                "items": "string",
                "name": "fn"
            },
            "name": "fn"
        },
        {
            "type": {
                "type": "record",
                "name": "fn",
                "fields": [{
                    "type": "string",
                    "name": "inner_rec"
                }]
            },
            "name": "fn"
        },
        {
            "type": {
                "type": "enum",
                "name": "fn",
                "symbols": ["foo", "bar", "baz"]
            },
            "name": "fn"
        },
        {
            "type": {
                "type": "fixed",
                "size": 16,
                "name": "fn",
                "aliases": ["testalias"]
            },
            "name": "fn"
        },
        {
            "type": "string",
            "name": "fn"
        },
        {
            "type": {
                "type": "map",
                "values": "int",
                "name": "fn"
            },
            "name": "fn"
        },
    ]
)
async def test_union_to_simple_comparison(field):
    writer = {"type": "record", "name": "name", "namespace": "namespace", "fields": [field]}
    reader = {
        "type": "record",
        "name": "name",
        "namespace": "namespace",
        "fields": [{
            "type": ["null", field["type"]],
            "name": "fn",
        }]
    }
    reader = Parse(json.dumps(reader))
    writer = Parse(json.dumps(writer))
    assert ReaderWriterCompatibilityChecker(
    ).get_compatibility(reader, writer).compatibility is SchemaCompatibilityType.compatible
