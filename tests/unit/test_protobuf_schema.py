from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.proto_file_element import ProtoFileElement
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
    protobuf_schema1: ProtoFileElement = TypedSchema.parse(SchemaType.PROTOBUF, proto1)
    proto2 = trim_margin(schema_protobuf_compare_one)
    protobuf_schema2: ProtoFileElement = TypedSchema.parse(SchemaType.PROTOBUF, proto2)
    result = protobuf_schema1.compatible(protobuf_schema2)
    assert result is True


def test_protobuf_schema_compare():
    proto1 = trim_margin(schema_protobuf_order_after)
    protobuf_schema1: ProtoFileElement = TypedSchema.parse(SchemaType.PROTOBUF, proto1)
    proto2 = trim_margin(schema_protobuf_compare_one)
    protobuf_schema2: ProtoFileElement = TypedSchema.parse(SchemaType.PROTOBUF, proto2)
    result = protobuf_schema2.compatible(protobuf_schema1)
    assert result is False
