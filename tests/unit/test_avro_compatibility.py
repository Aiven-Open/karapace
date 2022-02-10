"""
    These are duplicates of other test_schema.py tests, but do not make use of the registry client fixture
    and are here for debugging and speed, and as an initial sanity check
"""
from avro.schema import Schema, UnionSchema
from karapace.avro_compatibility import (
    parse_avro_schema_definition,
    ReaderWriterCompatibilityChecker,
    SchemaCompatibilityType,
)
from tests.schemas.avro import (
    A_DINT_B_DENUM_1_RECORD1,
    A_DINT_B_DENUM_2_RECORD1,
    A_DINT_B_DFIXED_4_BYTES_RECORD1,
    A_DINT_B_DFIXED_8_BYTES_RECORD1,
    A_DINT_B_DINT_RECORD1,
    A_DINT_B_DINT_STRING_UNION_RECORD1,
    A_DINT_B_DINT_UNION_RECORD1,
    A_DINT_RECORD1,
    A_INT_B_DINT_RECORD1,
    A_INT_B_INT_RECORD1,
    A_INT_RECORD1,
    A_LONG_RECORD1,
    BOOLEAN_SCHEMA,
    BYTES_SCHEMA,
    BYTES_UNION_SCHEMA,
    DOUBLE_SCHEMA,
    DOUBLE_UNION_SCHEMA,
    EMPTY_RECORD1,
    EMPTY_RECORD2,
    EMPTY_UNION_SCHEMA,
    ENUM1_AB_SCHEMA,
    ENUM1_ABC_SCHEMA,
    ENUM1_BC_SCHEMA,
    ENUM2_AB_SCHEMA,
    ENUM_AB_ENUM_DEFAULT_A_RECORD,
    ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD,
    ENUM_ABC_ENUM_DEFAULT_A_RECORD,
    ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD,
    FIXED_4_ANOTHER_NAME,
    FIXED_4_BYTES,
    FIXED_8_BYTES,
    FLOAT_SCHEMA,
    FLOAT_UNION_SCHEMA,
    INT_ARRAY_SCHEMA,
    INT_FLOAT_UNION_SCHEMA,
    INT_LIST_RECORD,
    INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
    INT_LONG_UNION_SCHEMA,
    INT_MAP_SCHEMA,
    INT_SCHEMA,
    INT_STRING_UNION_SCHEMA,
    INT_UNION_SCHEMA,
    LONG_ARRAY_SCHEMA,
    LONG_LIST_RECORD,
    LONG_MAP_SCHEMA,
    LONG_SCHEMA,
    LONG_UNION_SCHEMA,
    NS_RECORD1,
    NS_RECORD2,
    NULL_SCHEMA,
    RECORD1_WITH_ENUM_AB,
    RECORD1_WITH_ENUM_ABC,
    schema1,
    schema2,
    schema3,
    schema4,
    schema6,
    schema7,
    schema8,
    STRING_ARRAY_SCHEMA,
    STRING_INT_UNION_SCHEMA,
    STRING_MAP_SCHEMA,
    STRING_SCHEMA,
    STRING_UNION_SCHEMA,
    UNION_INT_ARRAY_INT,
    UNION_INT_BOOLEAN,
    UNION_INT_ENUM1_AB,
    UNION_INT_FIXED_4_BYTES,
    UNION_INT_MAP_INT,
    UNION_INT_NULL,
    UNION_INT_RECORD1,
    UNION_INT_RECORD2,
)

import json
import pytest


def are_compatible(reader: Schema, writer: Schema) -> bool:
    return (
        ReaderWriterCompatibilityChecker().get_compatibility(reader, writer).compatibility
        is SchemaCompatibilityType.compatible
    )


def not_compatible(reader: Schema, writer: Schema) -> bool:
    return (
        ReaderWriterCompatibilityChecker().get_compatibility(reader, writer).compatibility
        is not SchemaCompatibilityType.compatible
    )


def test_schemaregistry_basic_backwards_compatibility() -> None:
    """
    Backward compatibility: A new schema is backward compatible if it can be used to read the data
    written in the previous schema.
    """
    msg = "adding a field with default is a backward compatible change"
    assert are_compatible(schema2, schema1), msg

    msg = "adding a field w/o default is NOT a backward compatible change"
    assert not_compatible(schema3, schema1), msg

    msg = "changing field name with alias is a backward compatible change"
    assert are_compatible(schema4, schema1), msg

    msg = "evolving a field type to a union is a backward compatible change"
    assert are_compatible(schema6, schema1), msg

    msg = "removing a type from a union is NOT a backward compatible change"
    assert not_compatible(schema1, schema6), msg

    msg = "adding a new type in union is a backward compatible change"
    assert are_compatible(schema7, schema6), msg

    msg = "removing a type from a union is NOT a backward compatible change"
    assert not_compatible(schema6, schema7), msg


def test_schemaregistry_basic_backwards_transitive_compatibility() -> None:
    """
    Backward transitive compatibility: A new schema is backward compatible if it can be used to read the data
    written in all previous schemas.
    """
    msg = "iteratively adding fields with defaults is a compatible change"
    assert are_compatible(schema8, schema1), msg
    assert are_compatible(schema8, schema2), msg

    msg = "adding a field with default is a backward compatible change"
    assert are_compatible(schema2, schema1), msg

    msg = "removing a default is a compatible change, but not transitively"
    assert are_compatible(schema3, schema2), msg

    msg = "removing a default is not a transitively compatible change"
    assert are_compatible(schema3, schema2), msg
    assert not_compatible(schema3, schema1), msg


def test_schemaregistry_basic_forwards_compatibility() -> None:
    """
    Forward compatibility: A new schema is forward compatible if the previous schema can read data written in this
    schema.
    """
    msg = "adding a field is a forward compatible change"
    assert are_compatible(schema1, schema2), msg

    msg = "adding a field is a forward compatible change"
    assert are_compatible(schema1, schema3), msg

    msg = "adding a field is a forward compatible change"
    assert are_compatible(schema2, schema3), msg

    msg = "adding a field is a forward compatible change"
    assert are_compatible(schema3, schema2), msg

    msg = "removing a default is not a transitively compatible change"
    # Only schema 2 is checked!
    # assert not_compatible(schema3, schema1), msg
    assert are_compatible(schema2, schema1), msg


def test_schemaregistry_basic_forwards_transitive_compatibility() -> None:
    """
    Forward transitive compatibility: A new schema is forward compatible if all previous schemas can read data written
    in this schema.
    """
    msg = "iteratively removing fields with defaults is a compatible change"
    assert are_compatible(schema8, schema1), msg
    assert are_compatible(schema2, schema1), msg

    msg = "adding default to a field is a compatible change"
    assert are_compatible(schema3, schema2), msg

    msg = "removing a field with a default is a compatible change"
    assert are_compatible(schema2, schema1), msg

    msg = "removing a default is not a transitively compatible change"
    assert are_compatible(schema2, schema1), msg
    assert not_compatible(schema3, schema1), msg


def test_basic_full_compatibility() -> None:
    """Full compatibility: A new schema is fully compatible if it’s both backward and forward compatible."""
    msg = "adding a field with default is a backward and a forward compatible change"
    assert are_compatible(schema2, schema1), msg
    assert are_compatible(schema1, schema2), msg

    msg = "transitively adding a field without a default is not a compatible change"
    # Only schema 2 is checked!
    # assert not_compatible(schema3, schema1), msg
    # assert not_compatible(schema1, schema3), msg
    assert are_compatible(schema3, schema2), msg
    assert are_compatible(schema2, schema3), msg

    msg = "transitively removing a field without a default is not a compatible change"
    # Only schema 2 is checked!
    # assert are_compatible(schema1, schema3), msg
    # assert are_compatible(schema3, schema1), msg
    assert are_compatible(schema1, schema2), msg
    assert are_compatible(schema1, schema2), msg


def test_basic_full_transitive_compatibility() -> None:
    """
    Full transitive compatibility: A new schema is fully compatible if it’s both transitively backward
    and transitively forward compatible with the entire schema history.
    """
    msg = "iteratively adding fields with defaults is a compatible change"
    assert are_compatible(schema8, schema1), msg
    assert are_compatible(schema1, schema8), msg
    assert are_compatible(schema8, schema2), msg
    assert are_compatible(schema2, schema8), msg

    msg = "iteratively removing fields with defaults is a compatible change"
    assert are_compatible(schema1, schema8), msg
    assert are_compatible(schema8, schema1), msg
    assert are_compatible(schema1, schema2), msg
    assert are_compatible(schema2, schema1), msg

    msg = "adding default to a field is a compatible change"
    assert are_compatible(schema2, schema3), msg
    assert are_compatible(schema3, schema2), msg

    msg = "removing a field with a default is a compatible change"
    assert are_compatible(schema1, schema2), msg
    assert are_compatible(schema2, schema1), msg

    msg = "adding a field with default is a compatible change"
    assert are_compatible(schema2, schema1), msg
    assert are_compatible(schema1, schema2), msg

    msg = "removing a default from a field compatible change"
    assert are_compatible(schema3, schema2), msg
    assert are_compatible(schema2, schema3), msg

    msg = "transitively adding a field without a default is not a compatible change"
    assert are_compatible(schema3, schema2), msg
    assert not_compatible(schema3, schema1), msg
    assert are_compatible(schema2, schema3), msg
    assert are_compatible(schema1, schema3), msg

    msg = "transitively removing a field without a default is not a compatible change"
    assert are_compatible(schema1, schema2), msg
    assert are_compatible(schema1, schema3), msg
    assert are_compatible(schema2, schema1), msg
    assert not_compatible(schema3, schema1), msg


def test_simple_schema_promotion() -> None:
    reader = parse_avro_schema_definition(
        json.dumps({"name": "foo", "type": "record", "fields": [{"type": "int", "name": "f1"}]})
    )
    field_alias_reader = parse_avro_schema_definition(
        json.dumps({"name": "foo", "type": "record", "fields": [{"type": "int", "name": "bar", "aliases": ["f1"]}]})
    )
    record_alias_reader = parse_avro_schema_definition(
        json.dumps({"name": "other", "type": "record", "fields": [{"type": "int", "name": "f1"}], "aliases": ["foo"]})
    )

    writer = parse_avro_schema_definition(
        json.dumps(
            {
                "name": "foo",
                "type": "record",
                "fields": [
                    {"type": "int", "name": "f1"},
                    {
                        "type": "string",
                        "name": "f2",
                    },
                ],
            }
        )
    )
    # alias testing
    assert are_compatible(field_alias_reader, writer)
    assert are_compatible(record_alias_reader, writer)

    assert are_compatible(reader, writer)
    assert not_compatible(writer, reader)  # pylint: disable=arguments-out-of-order

    writer = parse_avro_schema_definition(
        json.dumps(
            {
                "type": "record",
                "name": "CA",
                "namespace": "ns1",
                "fields": [
                    {"type": "string", "name": "provider"},
                    {"type": ["null", "string"], "name": "name", "default": None},
                    {"type": ["null", "string"], "name": "phone", "default": None},
                    {"type": ["null", "string"], "name": "email", "default": None},
                    {"type": ["null", "string"], "name": "reference", "default": None},
                    {"type": ["null", "double"], "name": "price", "default": None},
                ],
            }
        )
    )
    reader = parse_avro_schema_definition(
        json.dumps(
            {
                "type": "record",
                "name": "CA",
                "namespace": "ns1",
                "fields": [
                    {"type": "string", "name": "provider"},
                    {"type": ["null", "string"], "name": "name", "default": None},
                    {"type": ["null", "string"], "name": "phone", "default": None},
                    {"type": ["null", "string"], "name": "email", "default": None},
                    {"type": ["null", "string"], "name": "reference", "default": None},
                    {"type": ["null", "double"], "name": "price", "default": None},
                    {"type": ["null", "string"], "name": "status_date", "default": None},
                ],
            }
        )
    )
    assert are_compatible(writer=writer, reader=reader)


@pytest.mark.parametrize(
    "field",
    [
        {"type": {"type": "array", "items": "string", "name": "fn"}, "name": "fn"},
        {"type": {"type": "record", "name": "fn", "fields": [{"type": "string", "name": "inner_rec"}]}, "name": "fn"},
        {"type": {"type": "enum", "name": "fn", "symbols": ["foo", "bar", "baz"]}, "name": "fn"},
        {"type": {"type": "fixed", "size": 16, "name": "fn", "aliases": ["testalias"]}, "name": "fn"},
        {"type": "string", "name": "fn"},
        {"type": {"type": "map", "values": "int", "name": "fn"}, "name": "fn"},
    ],
)
def test_union_to_simple_comparison(field: dict) -> None:
    writer = {"type": "record", "name": "name", "namespace": "namespace", "fields": [field]}
    reader = {
        "type": "record",
        "name": "name",
        "namespace": "namespace",
        "fields": [
            {
                "type": ["null", field["type"]],
                "name": "fn",
            }
        ],
    }
    reader = parse_avro_schema_definition(json.dumps(reader))
    writer = parse_avro_schema_definition(json.dumps(writer))
    assert are_compatible(reader, writer)


def test_schema_compatibility() -> None:
    # testValidateSchemaPairMissingField
    writer = parse_avro_schema_definition(
        json.dumps(
            {
                "type": "record",
                "name": "Record",
                "fields": [{"name": "oldField1", "type": "int"}, {"name": "oldField2", "type": "string"}],
            }
        )
    )
    reader = parse_avro_schema_definition(
        json.dumps({"type": "record", "name": "Record", "fields": [{"name": "oldField1", "type": "int"}]})
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaPairMissingSecondField
    reader = parse_avro_schema_definition(
        json.dumps({"type": "record", "name": "Record", "fields": [{"name": "oldField2", "type": "string"}]})
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaPairAllFields
    reader = parse_avro_schema_definition(
        json.dumps(
            {
                "type": "record",
                "name": "Record",
                "fields": [{"name": "oldField1", "type": "int"}, {"name": "oldField2", "type": "string"}],
            }
        )
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaNewFieldWithDefault
    reader = parse_avro_schema_definition(
        json.dumps(
            {
                "type": "record",
                "name": "Record",
                "fields": [{"name": "oldField1", "type": "int"}, {"name": "newField2", "type": "int", "default": 42}],
            }
        )
    )
    assert are_compatible(reader, writer)
    # testValidateSchemaNewField
    reader = parse_avro_schema_definition(
        json.dumps(
            {
                "type": "record",
                "name": "Record",
                "fields": [{"name": "oldField1", "type": "int"}, {"name": "newField2", "type": "int"}],
            }
        )
    )
    assert not are_compatible(reader, writer)
    # testValidateArrayWriterSchema
    writer = STRING_ARRAY_SCHEMA
    assert are_compatible(STRING_ARRAY_SCHEMA, writer)
    assert not are_compatible(STRING_MAP_SCHEMA, writer)
    # testValidatePrimitiveWriterSchema
    writer = STRING_SCHEMA
    reader = STRING_SCHEMA
    assert are_compatible(reader, writer)
    reader = parse_avro_schema_definition(json.dumps({"type": "int"}))
    assert not are_compatible(reader, writer)
    # testUnionReaderWriterSubsetIncompatibility
    # cannot have a union as a top level data type, so im cheating a bit here
    writer = parse_avro_schema_definition(
        json.dumps({"name": "Record", "type": "record", "fields": [{"name": "f1", "type": ["int", "string", "long"]}]})
    )
    reader = parse_avro_schema_definition(
        json.dumps({"name": "Record", "type": "record", "fields": [{"name": "f1", "type": ["int", "string"]}]})
    )
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
        (NULL_SCHEMA, NULL_SCHEMA),
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


def test_schema_compatibility_fixed_size_mismatch() -> None:
    incompatible_fixed_pairs = [
        (FIXED_4_BYTES, FIXED_8_BYTES, "expected: 8, found: 4", "/size"),
        (FIXED_8_BYTES, FIXED_4_BYTES, "expected: 4, found: 8", "/size"),
        (A_DINT_B_DFIXED_8_BYTES_RECORD1, A_DINT_B_DFIXED_4_BYTES_RECORD1, "expected: 4, found: 8", "/fields/1/type/size"),
        (A_DINT_B_DFIXED_4_BYTES_RECORD1, A_DINT_B_DFIXED_8_BYTES_RECORD1, "expected: 8, found: 4", "/fields/1/type/size"),
    ]
    for (reader, writer, message, location) in incompatible_fixed_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert location in result.locations, f"expected {location}, found {result.locations}"
        assert message in result.messages, f"expected {message}, found {result.messages}"


def test_schema_compatibility_missing_enum_symbols() -> None:
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


def test_schema_compatibility_missing_union_branch() -> None:
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
            INT_UNION_SCHEMA,
            INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
            {
                "reader union lacking writer type: LONG",
                "reader union lacking writer type: FLOAT",
                "reader union lacking writer type: DOUBLE",
            },
            {"/1", "/2", "/3"},
        ),
        (
            A_DINT_B_DINT_UNION_RECORD1,
            A_DINT_B_DINT_STRING_UNION_RECORD1,
            {"reader union lacking writer type: STRING"},
            {"/fields/1/type/1"},
        ),
    ]

    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert result.messages == message
        assert result.locations == location


def test_schema_compatibility_name_mismatch() -> None:
    incompatible_pairs = [
        (ENUM1_AB_SCHEMA, ENUM2_AB_SCHEMA, "expected: Enum2", "/name"),
        (EMPTY_RECORD2, EMPTY_RECORD1, "expected: Record1", "/name"),
        (FIXED_4_BYTES, FIXED_4_ANOTHER_NAME, "expected: AnotherName", "/name"),
        (A_DINT_B_DENUM_1_RECORD1, A_DINT_B_DENUM_2_RECORD1, "expected: Enum2", "/fields/1/type/name"),
    ]

    for (reader, writer, message, location) in incompatible_pairs:
        result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        assert message in result.messages
        assert location in result.locations


def test_schema_compatibility_reader_field_missing_default_value() -> None:
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


def test_schema_compatibility_type_mismatch() -> None:
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
            FLOAT_SCHEMA,
            INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
            "reader type: FLOAT not compatible with writer type: DOUBLE",
            "/",
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
