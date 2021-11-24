from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.io import ProtobufDatumReader
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.schema import ProtobufSchema
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
    protobuf_schema1.schema.compare(protobuf_schema2.schema, result)
    assert result.is_compatible()


def test_protobuf_schema_compare2():
    proto1 = trim_margin(schema_protobuf_order_after)
    protobuf_schema1: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1)
    proto2 = trim_margin(schema_protobuf_compare_one)
    protobuf_schema2: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2)
    result = CompareResult()
    protobuf_schema2.schema.compare(protobuf_schema1.schema, result)
    assert result.is_compatible()


def test_protobuf_schema_compare3():
    proto1 = """
            |syntax = "proto3";
            |package a1;
            |message TestMessage {
            |    message Value {
            |        string str2 = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""

    proto1 = trim_margin(proto1)

    proto2 = """
                |syntax = "proto3";
                |package a1;
                |
                |message TestMessage {
                |  string test = 1;
                |  .a1.TestMessage.Value val = 2;
                |
                |  message Value {
                |    string str2 = 1;
                |    Enu x = 2;
                |  }
                |  enum Enu {
                |    A = 0;
                |    B = 1;
                |  }
                |}
                |"""

    proto2 = trim_margin(proto2)
    protobuf_schema1: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert result.is_compatible()


def test_protobuf_message_compatible_label_alter():
    proto1 = """
    |syntax = "proto3";
    |message Goods {
    |    optional Packet record = 1;
    |    string driver = 2;
    |    message Packet {
    |       bytes order = 1;
    |    }
    |}
    |"""
    proto1 = trim_margin(proto1)

    proto2 = """
    |syntax = "proto3";
    |message Goods {
    |   repeated Packet record = 1;
    |   string driver = 2;
    |   message Packet {
    |       bytes order = 1;
    |   }
    |}
    |"""

    proto2 = trim_margin(proto2)

    protobuf_schema1: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert result.is_compatible()


def test_protobuf_field_type_incompatible_alter():
    proto1 = """
       |syntax = "proto3";
       |message Goods {
       |    string order = 1;
       |    map<string, int32> items_int32 = 2;
       |}
       |"""
    proto1 = trim_margin(proto1)

    proto2 = """
       |syntax = "proto3";
       |message Goods {
       |     string order = 1;
       |     map<string, string> items_string = 2;
       |}
       |"""

    proto2 = trim_margin(proto2)

    protobuf_schema1: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert not result.is_compatible()


def test_protobuf_field_label_compatible_alter():
    proto1 = """
           |syntax = "proto3";
           |message Goods {
           |     optional string driver = 1;
           |     Order order = 2;
           |     message Order {
           |        string item = 1;
           |     }
           |}
           |"""

    proto1 = trim_margin(proto1)
    proto2 = """
           |syntax = "proto3";
           |message Goods {
           |     repeated string driver = 1;
           |     Order order = 2;
           |     message Order {
           |        string item = 1;
           |     }
           |}
           |"""

    proto2 = trim_margin(proto2)

    protobuf_schema1: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert result.is_compatible()


def test_protobuf_field_incompatible_drop_from_oneof():
    proto1 = """
           |syntax = "proto3";
           |message Goods {
           |     oneof item {
           |             string name_a = 1;
           |             string name_b = 2;
           |             int32 id = 3;
           |     }
           |}
           |"""

    proto1 = trim_margin(proto1)
    proto2 = """
           |syntax = "proto3";
           |message Goods {
           |     oneof item {
           |           string name_a = 1;
           |           string name_b = 2;
           |     }
           |}
           |"""

    proto2 = trim_margin(proto2)

    protobuf_schema1: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert not result.is_compatible()


def test_protobuf_field_incompatible_alter_to_oneof():
    proto1 = """
           |syntax = "proto3";
           |message Goods {
           |    string name = 1;
           |    string reg_name = 2;
           |}
           |"""

    proto1 = trim_margin(proto1)
    proto2 = """
           |syntax = "proto3";
           |message Goods {
           |  oneof reg_data {
           |    string name = 1;
           |    string reg_name = 2;
           |    int32 id = 3;
           |  }
           |}
           |"""

    proto2 = trim_margin(proto2)

    protobuf_schema1: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert not result.is_compatible()


def test_protobuf_field_compatible_alter_to_oneof():
    proto1 = """
           |syntax = "proto3";
           |message Goods {
           |    string name = 1;
           |    string foo = 2;
           |}
           |"""

    proto1 = trim_margin(proto1)
    proto2 = """
           |syntax = "proto3";
           |message Goods {
           |    string name = 1;
           |  oneof new_oneof {
           |    string foo = 2;
           |    int32 bar = 3;
           |  }
           |}
           |"""

    proto2 = trim_margin(proto2)

    protobuf_schema1: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = TypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert result.is_compatible()
