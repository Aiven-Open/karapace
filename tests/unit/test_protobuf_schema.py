from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.schema_reader import SchemaType, TypedSchema
from tests.schemas.protobuf import (
    schema_protobuf_compare_one, schema_protobuf_order_after, schema_protobuf_order_before, schema_protobuf_schema_registry1
)

location: Location = Location.get("file.proto")


def test_protobuf_schema_simple():
    proto = trim_margin(schema_protobuf_schema_registry1)
    protobuf_schema = TypedSchema.parse(SchemaType.PROTOBUF, proto)
    result = str(protobuf_schema)

    assert result == proto


def test_protobuf_schema_sort():
    proto = trim_margin(schema_protobuf_order_before)
    protobuf_schema = TypedSchema.parse(SchemaType.PROTOBUF, proto)
    result = str(protobuf_schema)
    proto2 = trim_margin(schema_protobuf_order_after)
    assert result == proto2


def test_protobuf_schema_compare():
    proto1 = trim_margin(schema_protobuf_order_after)
    protobuf_schema1: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1)
    proto2 = trim_margin(schema_protobuf_compare_one)
    protobuf_schema2: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2)
    result = CompareResult()
    protobuf_schema1.schema.schema.compare(protobuf_schema2.schema.schema, result)
    assert result.is_compatible()


def test_protobuf_schema_compare2():
    proto1 = trim_margin(schema_protobuf_order_after)
    protobuf_schema1: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1)
    proto2 = trim_margin(schema_protobuf_compare_one)
    protobuf_schema2: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2)
    result = CompareResult()
    protobuf_schema2.schema.schema.compare(protobuf_schema1.schema.schema, result)
    assert result.is_compatible()
