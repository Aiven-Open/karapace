"""
    These are duplicates of other test_schema.py tests, but do not make use of the registry client fixture
    and are here for debugging and speed, and as an initial sanity check
"""
from avro.schema import ArraySchema, Field, MapSchema, parse, RecordSchema, Schema, UnionSchema
from karapace.avro_compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType

import json
import pytest


def test_simple_schema_promotion():
    reader = parse(json.dumps({"name": "foo", "type": "record", "fields": [{"type": "int", "name": "f1"}]}))
    writer = parse(
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

    writer = parse(
        json.dumps({
            "type": "record",
            "name": "CA",
            "namespace": "ns1",
            "fields": [{
                "type": "string",
                "name": "provider"
            }, {
                "type": ["null", "string"],
                "name": "name",
                "default": None
            }, {
                "type": ["null", "string"],
                "name": "phone",
                "default": None
            }, {
                "type": ["null", "string"],
                "name": "email",
                "default": None
            }, {
                "type": ["null", "string"],
                "name": "reference",
                "default": None
            }, {
                "type": ["null", "double"],
                "name": "price",
                "default": None
            }]
        })
    )
    reader = parse(
        json.dumps({
            "type": "record",
            "name": "CA",
            "namespace": "ns1",
            "fields": [{
                "type": "string",
                "name": "provider"
            }, {
                "type": ["null", "string"],
                "name": "name",
                "default": None
            }, {
                "type": ["null", "string"],
                "name": "phone",
                "default": None
            }, {
                "type": ["null", "string"],
                "name": "email",
                "default": None
            }, {
                "type": ["null", "string"],
                "name": "reference",
                "default": None
            }, {
                "type": ["null", "double"],
                "name": "price",
                "default": None
            }, {
                "type": ["null", "string"],
                "name": "status_date",
                "default": None
            }]
        })
    )
    res = ReaderWriterCompatibilityChecker().get_compatibility(writer=writer, reader=reader)
    assert res.compatibility is SchemaCompatibilityType.compatible, res


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
def test_union_to_simple_comparison(field):
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
    reader = parse(json.dumps(reader))
    writer = parse(json.dumps(writer))
    assert are_compatible(reader, writer)


#  ================================================================================================
#            These tests are more or less directly lifted from the java avro codebase
#            There's one test per Java file, so expect the first one to be a mammoth
#  ================================================================================================

BOOLEAN_SCHEMA = parse(json.dumps("boolean"))
NULL_SCHEMA = parse(json.dumps("null"))
INT_SCHEMA = parse(json.dumps("int"))
LONG_SCHEMA = parse(json.dumps("long"))
STRING_SCHEMA = parse(json.dumps("string"))
BYTES_SCHEMA = parse(json.dumps("bytes"))
FLOAT_SCHEMA = parse(json.dumps("float"))
DOUBLE_SCHEMA = parse(json.dumps("double"))
INT_ARRAY_SCHEMA = ArraySchema(INT_SCHEMA)
LONG_ARRAY_SCHEMA = ArraySchema(LONG_SCHEMA)
STRING_ARRAY_SCHEMA = ArraySchema(STRING_SCHEMA)
INT_MAP_SCHEMA = MapSchema(INT_SCHEMA)
LONG_MAP_SCHEMA = MapSchema(LONG_SCHEMA)
STRING_MAP_SCHEMA = MapSchema(STRING_SCHEMA)
ENUM1_AB_SCHEMA = parse(json.dumps({"type": "enum", "name": "Enum1", "symbols": ["A", "B"]}))
ENUM1_ABC_SCHEMA = parse(json.dumps({"type": "enum", "name": "Enum1", "symbols": ["A", "B", "C"]}))
ENUM1_BC_SCHEMA = parse(json.dumps({"type": "enum", "name": "Enum1", "symbols": ["B", "C"]}))
ENUM2_AB_SCHEMA = parse(json.dumps({"type": "enum", "name": "Enum2", "symbols": ["A", "B"]}))
ENUM_ABC_ENUM_DEFAULT_A_SCHEMA = parse(
    json.dumps({
        "type": "enum",
        "name": "Enum",
        "symbols": ["A", "B", "C"],
        "default": "A"
    })
)
ENUM_AB_ENUM_DEFAULT_A_SCHEMA = parse(json.dumps({"type": "enum", "name": "Enum", "symbols": ["A", "B"], "default": "A"}))
ENUM_ABC_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps({
        "type": "record",
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": "enum",
                "name": "Enum",
                "symbols": ["A", "B", "C"],
                "default": "A"
            }
        }]
    })
)
ENUM_AB_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps({
        "type": "record",
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": "enum",
                "name": "Enum",
                "symbols": ["A", "B"],
                "default": "A"
            }
        }]
    })
)
ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps({
        "type": "record",
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": "enum",
                "name": "Enum",
                "symbols": ["A", "B", "C"],
                "default": "A"
            },
            "default": "B"
        }]
    })
)
ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD = parse(
    json.dumps({
        "type": "record",
        "name": "Record",
        "fields": [{
            "name": "Field",
            "type": {
                "type": "enum",
                "name": "Enum",
                "symbols": ["A", "B"],
                "default": "B"
            },
            "default": "A"
        }]
    })
)
EMPTY_UNION_SCHEMA = UnionSchema([])
NULL_UNION_SCHEMA = UnionSchema([NULL_SCHEMA])
INT_UNION_SCHEMA = UnionSchema([INT_SCHEMA])
LONG_UNION_SCHEMA = UnionSchema([LONG_SCHEMA])
FLOAT_UNION_SCHEMA = UnionSchema([FLOAT_SCHEMA])
DOUBLE_UNION_SCHEMA = UnionSchema([DOUBLE_SCHEMA])
STRING_UNION_SCHEMA = UnionSchema([STRING_SCHEMA])
BYTES_UNION_SCHEMA = UnionSchema([BYTES_SCHEMA])
INT_STRING_UNION_SCHEMA = UnionSchema([INT_SCHEMA, STRING_SCHEMA])
STRING_INT_UNION_SCHEMA = UnionSchema([STRING_SCHEMA, INT_SCHEMA])
INT_FLOAT_UNION_SCHEMA = UnionSchema([INT_SCHEMA, FLOAT_SCHEMA])
INT_LONG_UNION_SCHEMA = UnionSchema([INT_SCHEMA, LONG_SCHEMA])
INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA = UnionSchema([INT_SCHEMA, LONG_SCHEMA, FLOAT_SCHEMA, DOUBLE_SCHEMA])
NULL_INT_ARRAY_UNION_SCHEMA = UnionSchema([NULL_SCHEMA, INT_ARRAY_SCHEMA])
NULL_INT_MAP_UNION_SCHEMA = UnionSchema([NULL_SCHEMA, INT_MAP_SCHEMA])
EMPTY_RECORD1 = parse(json.dumps({"type": "record", "name": "Record1", "fields": []}))
EMPTY_RECORD2 = parse(json.dumps({"type": "record", "name": "Record2", "fields": []}))
A_INT_RECORD1 = parse(json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "a", "type": "int"}]}))
A_LONG_RECORD1 = parse(json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "a", "type": "long"}]}))
A_INT_B_INT_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int"
        }, {
            "name": "b",
            "type": "int"
        }]
    })
)
A_DINT_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }]
    })
)
A_INT_B_DINT_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int"
        }, {
            "name": "b",
            "type": "int",
            "default": 0
        }]
    })
)
A_DINT_B_DINT_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }, {
            "name": "b",
            "type": "int",
            "default": 0
        }]
    })
)
A_DINT_B_DFIXED_4_BYTES_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": "fixed",
                "name": "Fixed",
                "size": 4
            }
        }]
    })
)
A_DINT_B_DFIXED_8_BYTES_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": "fixed",
                "name": "Fixed",
                "size": 8
            }
        }]
    })
)
A_DINT_B_DINT_STRING_UNION_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }, {
            "name": "b",
            "type": ["int", "string"],
            "default": 0
        }]
    })
)
A_DINT_B_DINT_UNION_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }, {
            "name": "b",
            "type": ["int"],
            "default": 0
        }]
    })
)
A_DINT_B_DENUM_1_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": "enum",
                "name": "Enum1",
                "symbols": ["A", "B"]
            }
        }]
    })
)
A_DINT_B_DENUM_2_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "a",
            "type": "int",
            "default": 0
        }, {
            "name": "b",
            "type": {
                "type": "enum",
                "name": "Enum2",
                "symbols": ["A", "B"]
            }
        }]
    })
)
FIXED_4_BYTES = parse(json.dumps({"type": "fixed", "name": "Fixed", "size": 4}))
FIXED_8_BYTES = parse(json.dumps({"type": "fixed", "name": "Fixed", "size": 8}))
NS_RECORD1 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "f1",
            "type": [
                "null", {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "InnerRecord1",
                        "namespace": "ns1",
                        "fields": [{
                            "name": "a",
                            "type": "int"
                        }]
                    }
                }
            ]
        }]
    })
)
NS_RECORD2 = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "f1",
            "type": [
                "null", {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "InnerRecord1",
                        "namespace": "ns2",
                        "fields": [{
                            "name": "a",
                            "type": "int"
                        }]
                    }
                }
            ]
        }]
    })
)
INT_LIST_RECORD = parse(json.dumps({"type": "record", "name": "List", "fields": [{"name": "head", "type": "int"}]}))
LONG_LIST_RECORD = parse(json.dumps({"type": "record", "name": "List", "fields": [{"name": "head", "type": "long"}]}))
int_reader_field = Field(name="tail", type=INT_LIST_RECORD, index=1, has_default=False)
long_reader_field = Field(name="tail", type=LONG_LIST_RECORD, index=1, has_default=False)

INT_LIST_RECORD._fields = (INT_LIST_RECORD.fields[0], int_reader_field)
LONG_LIST_RECORD._fields = (LONG_LIST_RECORD.fields[0], long_reader_field)

# pylint: disable=protected-access
INT_LIST_RECORD._field_map = RecordSchema._MakeFieldMap(INT_LIST_RECORD._fields)
LONG_LIST_RECORD._field_map = RecordSchema._MakeFieldMap(LONG_LIST_RECORD._fields)
INT_LIST_RECORD._props["fields"] = INT_LIST_RECORD._fields
LONG_LIST_RECORD._props["fields"] = LONG_LIST_RECORD._fields
# pylint: enable=protected-access
RECORD1_WITH_INT = parse(json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "field1", "type": "int"}]}))
RECORD2_WITH_INT = parse(json.dumps({"type": "record", "name": "Record2", "fields": [{"name": "field1", "type": "int"}]}))
UNION_INT_RECORD1 = UnionSchema([INT_SCHEMA, RECORD1_WITH_INT])
UNION_INT_RECORD2 = UnionSchema([INT_SCHEMA, RECORD2_WITH_INT])
UNION_INT_ENUM1_AB = UnionSchema([INT_SCHEMA, ENUM1_AB_SCHEMA])
UNION_INT_FIXED_4_BYTES = UnionSchema([INT_SCHEMA, FIXED_4_BYTES])
UNION_INT_BOOLEAN = UnionSchema([INT_SCHEMA, BOOLEAN_SCHEMA])
UNION_INT_ARRAY_INT = UnionSchema([INT_SCHEMA, INT_ARRAY_SCHEMA])
UNION_INT_MAP_INT = UnionSchema([INT_SCHEMA, INT_MAP_SCHEMA])
UNION_INT_NULL = UnionSchema([INT_SCHEMA, NULL_SCHEMA])
FIXED_4_ANOTHER_NAME = parse(json.dumps({"type": "fixed", "name": "AnotherName", "size": 4}))
RECORD1_WITH_ENUM_AB = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "field1",
            "type": dict(ENUM1_AB_SCHEMA.to_json())
        }]
    })
)
RECORD1_WITH_ENUM_ABC = parse(
    json.dumps({
        "type": "record",
        "name": "Record1",
        "fields": [{
            "name": "field1",
            "type": dict(ENUM1_ABC_SCHEMA.to_json())
        }]
    })
)


def test_schema_compatibility():
    # testValidateSchemaPairMissingField
    writer = parse(
        json.dumps({
            "type": "record",
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": "int"
            }, {
                "name": "oldField2",
                "type": "string"
            }]
        })
    )
    reader = parse(json.dumps({"type": "record", "name": "Record", "fields": [{"name": "oldField1", "type": "int"}]}))
    assert are_compatible(reader, writer)
    # testValidateSchemaPairMissingSecondField
    reader = parse(json.dumps({"type": "record", "name": "Record", "fields": [{"name": "oldField2", "type": "string"}]}))
    assert are_compatible(reader, writer)
    # testValidateSchemaPairAllFields
    reader = parse(
        json.dumps({
            "type": "record",
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": "int"
            }, {
                "name": "oldField2",
                "type": "string"
            }]
        })
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaNewFieldWithDefault
    reader = parse(
        json.dumps({
            "type": "record",
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": "int"
            }, {
                "name": "newField2",
                "type": "int",
                "default": 42
            }]
        })
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaNewField
    reader = parse(
        json.dumps({
            "type": "record",
            "name": "Record",
            "fields": [{
                "name": "oldField1",
                "type": "int"
            }, {
                "name": "newField2",
                "type": "int"
            }]
        })
    )
    assert not are_compatible(reader, writer)
    # testValidateArrayWriterSchema
    writer = parse(json.dumps({"type": "array", "items": {"type": "string"}}))
    reader = parse(json.dumps({"type": "array", "items": {"type": "string"}}))
    assert are_compatible(reader, writer)
    reader = parse(json.dumps({"type": "map", "values": {"type": "string"}}))
    assert not are_compatible(reader, writer)
    # testValidatePrimitiveWriterSchema
    writer = parse(json.dumps({"type": "string"}))
    reader = parse(json.dumps({"type": "string"}))
    assert are_compatible(reader, writer)
    reader = parse(json.dumps({"type": "int"}))
    assert not are_compatible(reader, writer)
    # testUnionReaderWriterSubsetIncompatibility
    # cannot have a union as a top level data type, so im cheating a bit here
    writer = parse(
        json.dumps({
            "name": "Record",
            "type": "record",
            "fields": [{
                "name": "f1",
                "type": ["int", "string", "long"]
            }]
        })
    )
    reader = parse(json.dumps({"name": "Record", "type": "record", "fields": [{"name": "f1", "type": ["int", "string"]}]}))
    reader = reader.fields[0].type
    writer = writer.fields[0].type
    assert isinstance(reader, UnionSchema)
    assert isinstance(writer, UnionSchema)
    assert not are_compatible(reader, writer)
    # testReaderWriterCompatibility
    compatible_reader_writer_test_cases = [
        (BOOLEAN_SCHEMA, BOOLEAN_SCHEMA),
        (INT_SCHEMA, INT_SCHEMA),
        (LONG_SCHEMA, INT_SCHEMA),
        (LONG_SCHEMA, LONG_SCHEMA),
        (FLOAT_SCHEMA, INT_SCHEMA),
        (FLOAT_SCHEMA, LONG_SCHEMA),
        (DOUBLE_SCHEMA, LONG_SCHEMA),
        (DOUBLE_SCHEMA, INT_SCHEMA),
        (DOUBLE_SCHEMA, FLOAT_SCHEMA),
        (STRING_SCHEMA, STRING_SCHEMA),
        (BYTES_SCHEMA, BYTES_SCHEMA),
        (STRING_SCHEMA, BYTES_SCHEMA),
        (BYTES_SCHEMA, STRING_SCHEMA),
        (INT_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
        (LONG_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
        (INT_MAP_SCHEMA, INT_MAP_SCHEMA),
        (LONG_MAP_SCHEMA, INT_MAP_SCHEMA),
        (ENUM1_AB_SCHEMA, ENUM1_AB_SCHEMA),
        (ENUM1_ABC_SCHEMA, ENUM1_AB_SCHEMA),
        # Union related pairs
        (EMPTY_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, INT_LONG_UNION_SCHEMA),
        (INT_UNION_SCHEMA, INT_UNION_SCHEMA),
        (INT_STRING_UNION_SCHEMA, STRING_INT_UNION_SCHEMA),
        (INT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (LONG_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (LONG_UNION_SCHEMA, INT_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, INT_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, LONG_UNION_SCHEMA),
        (FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, FLOAT_UNION_SCHEMA),
        (STRING_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (STRING_UNION_SCHEMA, BYTES_UNION_SCHEMA),
        (BYTES_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
        (BYTES_UNION_SCHEMA, STRING_UNION_SCHEMA),
        (DOUBLE_UNION_SCHEMA, INT_FLOAT_UNION_SCHEMA),
        # Readers capable of reading all branches of a union are compatible
        (FLOAT_SCHEMA, INT_FLOAT_UNION_SCHEMA),
        (LONG_SCHEMA, INT_LONG_UNION_SCHEMA),
        (DOUBLE_SCHEMA, INT_FLOAT_UNION_SCHEMA),
        (DOUBLE_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA),
        # Special case of singleton unions:
        (FLOAT_SCHEMA, FLOAT_UNION_SCHEMA),
        (INT_UNION_SCHEMA, INT_SCHEMA),
        (INT_SCHEMA, INT_UNION_SCHEMA),
        # Fixed types
        (FIXED_4_BYTES, FIXED_4_BYTES),
        # Tests involving records:
        (EMPTY_RECORD1, EMPTY_RECORD1),
        (EMPTY_RECORD1, A_INT_RECORD1),
        (A_INT_RECORD1, A_INT_RECORD1),
        (A_DINT_RECORD1, A_INT_RECORD1),
        (A_DINT_RECORD1, A_DINT_RECORD1),
        (A_INT_RECORD1, A_DINT_RECORD1),
        (A_LONG_RECORD1, A_INT_RECORD1),
        (A_INT_RECORD1, A_INT_B_INT_RECORD1),
        (A_DINT_RECORD1, A_INT_B_INT_RECORD1),
        (A_INT_B_DINT_RECORD1, A_INT_RECORD1),
        (A_DINT_B_DINT_RECORD1, EMPTY_RECORD1),
        (A_DINT_B_DINT_RECORD1, A_INT_RECORD1),
        (A_INT_B_INT_RECORD1, A_DINT_B_DINT_RECORD1),
        (parse(json.dumps({"type": "null"})), parse(json.dumps({"type": "null"}))),
        (INT_LIST_RECORD, INT_LIST_RECORD),
        (LONG_LIST_RECORD, LONG_LIST_RECORD),
        (LONG_LIST_RECORD, INT_LIST_RECORD),
        (NULL_SCHEMA, NULL_SCHEMA),
        (ENUM_AB_ENUM_DEFAULT_A_RECORD, ENUM_ABC_ENUM_DEFAULT_A_RECORD),
        (ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD, ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD),
        (NS_RECORD1, NS_RECORD2),
    ]

    for (reader, writer) in compatible_reader_writer_test_cases:
        assert are_compatible(reader, writer)


def test_schema_compatibility_fixed_size_mismatch():
    incompatible_fixed_pairs = [
        (FIXED_4_BYTES, FIXED_8_BYTES, "expected: 8, found: 4", "/size"),
        (FIXED_8_BYTES, FIXED_4_BYTES, "expected: 4, found: 8", "/size"),
        (A_DINT_B_DFIXED_8_BYTES_RECORD1, A_DINT_B_DFIXED_4_BYTES_RECORD1, "expected: 4, found: 8", "/fields/1/type/size"),
        (A_DINT_B_DFIXED_4_BYTES_RECORD1, A_DINT_B_DFIXED_8_BYTES_RECORD1, "expected: 8, found: 4", "/fields/1/type/size"),
    ]
    for (reader, writer, message, location) in incompatible_fixed_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert location in result.locations, f"expected {location}, found {result.location}"
        assert message in result.messages, f"expected {message}, found {result.message}"


def test_schema_compatibility_missing_enum_symbols():
    incompatible_pairs = [
        # str(set) representation
        (ENUM1_AB_SCHEMA, ENUM1_ABC_SCHEMA, "{'C'}", "/symbols"),
        (ENUM1_BC_SCHEMA, ENUM1_ABC_SCHEMA, "{'A'}", "/symbols"),
        (RECORD1_WITH_ENUM_AB, RECORD1_WITH_ENUM_ABC, "{'C'}", "/fields/0/type/symbols"),
    ]
    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert message in result.messages
        assert location in result.locations


def test_schema_compatibility_missing_union_branch():
    incompatible_pairs = [
        (INT_UNION_SCHEMA, INT_STRING_UNION_SCHEMA, {"reader union lacking writer type: STRING"}, {"/1"}),
        (STRING_UNION_SCHEMA, INT_STRING_UNION_SCHEMA, {"reader union lacking writer type: INT"}, {"/0"}),
        (INT_UNION_SCHEMA, UNION_INT_RECORD1, {"reader union lacking writer type: RECORD"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_RECORD2, {"reader union lacking writer type: RECORD"}, {"/1"}),
        (UNION_INT_RECORD1, UNION_INT_RECORD2, {"reader union lacking writer type: RECORD"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_ENUM1_AB, {"reader union lacking writer type: ENUM"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_FIXED_4_BYTES, {"reader union lacking writer type: FIXED"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_BOOLEAN, {"reader union lacking writer type: BOOLEAN"}, {"/1"}),
        (INT_UNION_SCHEMA, LONG_UNION_SCHEMA, {"reader union lacking writer type: LONG"}, {"/0"}),
        (INT_UNION_SCHEMA, FLOAT_UNION_SCHEMA, {"reader union lacking writer type: FLOAT"}, {"/0"}),
        (INT_UNION_SCHEMA, DOUBLE_UNION_SCHEMA, {"reader union lacking writer type: DOUBLE"}, {"/0"}),
        (INT_UNION_SCHEMA, BYTES_UNION_SCHEMA, {"reader union lacking writer type: BYTES"}, {"/0"}),
        (INT_UNION_SCHEMA, UNION_INT_ARRAY_INT, {"reader union lacking writer type: ARRAY"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_MAP_INT, {"reader union lacking writer type: MAP"}, {"/1"}),
        (INT_UNION_SCHEMA, UNION_INT_NULL, {"reader union lacking writer type: NULL"}, {"/1"}),
        (
            INT_UNION_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA, {
                "reader union lacking writer type: LONG", "reader union lacking writer type: FLOAT",
                "reader union lacking writer type: DOUBLE"
            }, {"/1", "/2", "/3"}
        ),
        (
            A_DINT_B_DINT_UNION_RECORD1, A_DINT_B_DINT_STRING_UNION_RECORD1, {"reader union lacking writer type: STRING"},
            {"/fields/1/type/1"}
        ),
    ]

    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert result.messages == message
        assert result.locations == location


def test_schema_compatibility_name_mismatch():
    incompatible_pairs = [(ENUM1_AB_SCHEMA, ENUM2_AB_SCHEMA, "expected: Enum2", "/name"),
                          (EMPTY_RECORD2, EMPTY_RECORD1, "expected: Record1", "/name"),
                          (FIXED_4_BYTES, FIXED_4_ANOTHER_NAME, "expected: AnotherName", "/name"),
                          (A_DINT_B_DENUM_1_RECORD1, A_DINT_B_DENUM_2_RECORD1, "expected: Enum2", "/fields/1/type/name")]

    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert message in result.messages
        assert location in result.locations


def test_schema_compatibility_reader_field_missing_default_value():
    incompatible_pairs = [
        (A_INT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"),
        (A_INT_B_DINT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"),
    ]
    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert len(result.messages) == 1 and len(result.locations) == 1
        assert message == "".join(result.messages)
        assert location == "".join(result.locations)


def test_schema_compatibility_type_mismatch():
    incompatible_pairs = [
        (NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/"),
        (NULL_SCHEMA, LONG_SCHEMA, "reader type: NULL not compatible with writer type: LONG", "/"),
        (BOOLEAN_SCHEMA, INT_SCHEMA, "reader type: BOOLEAN not compatible with writer type: INT", "/"),
        (INT_SCHEMA, NULL_SCHEMA, "reader type: INT not compatible with writer type: NULL", "/"),
        (INT_SCHEMA, BOOLEAN_SCHEMA, "reader type: INT not compatible with writer type: BOOLEAN", "/"),
        (INT_SCHEMA, LONG_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/"),
        (INT_SCHEMA, FLOAT_SCHEMA, "reader type: INT not compatible with writer type: FLOAT", "/"),
        (INT_SCHEMA, DOUBLE_SCHEMA, "reader type: INT not compatible with writer type: DOUBLE", "/"),
        (LONG_SCHEMA, FLOAT_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT", "/"),
        (LONG_SCHEMA, DOUBLE_SCHEMA, "reader type: LONG not compatible with writer type: DOUBLE", "/"),
        (FLOAT_SCHEMA, DOUBLE_SCHEMA, "reader type: FLOAT not compatible with writer type: DOUBLE", "/"),
        (DOUBLE_SCHEMA, STRING_SCHEMA, "reader type: DOUBLE not compatible with writer type: STRING", "/"),
        (FIXED_4_BYTES, STRING_SCHEMA, "reader type: FIXED not compatible with writer type: STRING", "/"),
        (STRING_SCHEMA, BOOLEAN_SCHEMA, "reader type: STRING not compatible with writer type: BOOLEAN", "/"),
        (STRING_SCHEMA, INT_SCHEMA, "reader type: STRING not compatible with writer type: INT", "/"),
        (BYTES_SCHEMA, NULL_SCHEMA, "reader type: BYTES not compatible with writer type: NULL", "/"),
        (BYTES_SCHEMA, INT_SCHEMA, "reader type: BYTES not compatible with writer type: INT", "/"),
        (A_INT_RECORD1, INT_SCHEMA, "reader type: RECORD not compatible with writer type: INT", "/"),
        (INT_ARRAY_SCHEMA, LONG_ARRAY_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/items"),
        (INT_MAP_SCHEMA, INT_ARRAY_SCHEMA, "reader type: MAP not compatible with writer type: ARRAY", "/"),
        (INT_ARRAY_SCHEMA, INT_MAP_SCHEMA, "reader type: ARRAY not compatible with writer type: MAP", "/"),
        (INT_MAP_SCHEMA, LONG_MAP_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/values"),
        (INT_SCHEMA, ENUM2_AB_SCHEMA, "reader type: INT not compatible with writer type: ENUM", "/"),
        (ENUM2_AB_SCHEMA, INT_SCHEMA, "reader type: ENUM not compatible with writer type: INT", "/"),
        (
            FLOAT_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA, "reader type: FLOAT not compatible with writer type: DOUBLE",
            "/"
        ),
        (LONG_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT", "/"),
        (INT_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: INT not compatible with writer type: FLOAT", "/"),
        (INT_LIST_RECORD, LONG_LIST_RECORD, "reader type: INT not compatible with writer type: LONG", "/fields/0/type"),
        (NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/"),
    ]
    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert message in result.messages
        assert location in result.locations


def are_compatible(reader: Schema, writer: Schema) -> bool:
    return ReaderWriterCompatibilityChecker(
    ).get_compatibility(reader, writer).compatibility is SchemaCompatibilityType.compatible
