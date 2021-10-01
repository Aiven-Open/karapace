from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.schema_reader import SchemaType, TypedSchema
from tests.schemas.protobuf import (
    schema_protobuf_order_after, schema_protobuf_order_before, schema_protobuf_schema_registry1
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
