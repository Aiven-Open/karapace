"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.schema import ProtobufSchema


def test_nested_field_one_of_dependency():
    """The nested field may contain nested messages that are referred with one of restriction.
    This test is for regression when used types are added and parent name did not include correctly
    the nested `Foo`.
    The full used type is `.test.FooVal.Foo.Bar`.
    """
    proto = """
      |syntax = "proto3";
      |package test;
      |
      |option java_multiple_files = true;
      |option java_string_check_utf8 = true;
      |option java_outer_classname = "FooProto";
      |option java_package = "test";
      |
      |// Comment
      |message FooKey {
      |  // Comment
      |  string field_a = 1;
      |
      |}
      |
      |// Comment
      |message FooVal {
      |  // Comment
      |  repeated Foo field_a = 1;
      |
      |  // Comment
      |  string field_b = 2;
      |
      |  // Comment
      |  message Foo {
      |
      |    // Comment
      |    message Bar {
      |      // Comment
      |      string field_b = 1;
      |    }
      |
      |    // Comment
      |    message Bee {
      |    }
      |
      |    // Comment
      |    oneof packaging {
      |      // Comment
      |      Bar field_h = 2;
      |
      |      // Comment
      |      Bee field_i = 3;
      |    }
      |  }
      |}
    """
    proto = trim_margin(proto)
    pbschema = ProtobufSchema(schema=proto, references=(), dependencies=())
    deps = pbschema.verify_schema_dependencies()
    assert deps.result, "Dependencies verification failed."


def test_one_of_dependency():
    proto = """
      |syntax = "proto3";
      |package test;
      |
      |option java_multiple_files = true;
      |option java_string_check_utf8 = true;
      |option java_outer_classname = "FooProto";
      |option java_package = "test";
      |
      |// Comment
      |message FooKey {
      |  // Comment
      |  string field_a = 1;
      |
      |}
      |
      |// Comment
      |message FooVal {
      |  // Comment
      |  string field_b = 1;
      |
      |  // Comment
      |  message Bar {
      |    // Comment
      |    string field_b = 1;
      |  }
      |
      |  // Comment
      |  message Bee {
      |  }
      |
      |  // Comment
      |  oneof packaging {
      |    // Comment
      |    Bar field_h = 2;
      |
      |    // Comment
      |    Bee field_i = 3;
      |  }
      |}
    """
    proto = trim_margin(proto)
    pbschema = ProtobufSchema(schema=proto, references=(), dependencies=())
    deps = pbschema.verify_schema_dependencies()
    assert deps.result, "Dependencies verification failed."
