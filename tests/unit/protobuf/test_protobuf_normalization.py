"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.location import Location
from karapace.protobuf.proto_normalizations import normalize_options_ordered
from karapace.protobuf.proto_parser import ProtoParser

location: Location = Location("some/folder", "file.proto")


def test_different_options_order_its_correctly_normalized() -> None:
    ordered_schema = """\
syntax = "proto3";

package pkg;

option cc_generic_services = true;
option java_generate_equals_and_hash = true;
option java_generic_services = true;
option java_multiple_files = true;
option java_outer_classname = "FooProto";
option java_package = "com.example.foo";
option java_string_check_utf8 = true;
option optimize_for = SPEED;

message Foo {
  string fieldA = 1;

  string fieldB = 2;

  string fieldC = 3;

  string fieldX = 4;
}
"""

    unordered_schema = """\
syntax = "proto3";

package pkg;

option java_generic_services = true;
option java_generate_equals_and_hash = true;
option java_package = "com.example.foo";
option java_outer_classname = "FooProto";
option optimize_for = SPEED;
option cc_generic_services = true;
option java_multiple_files = true;
option java_string_check_utf8 = true;

message Foo {
  string fieldA = 1;

  string fieldB = 2;

  string fieldC = 3;

  string fieldX = 4;
}
"""

    ordered_proto = ProtoParser.parse(location, ordered_schema)
    unordered_proto = ProtoParser.parse(location, unordered_schema)

    result = CompareResult()
    assert result.is_compatible()
    normalize_options_ordered(ordered_proto).compare(normalize_options_ordered(unordered_proto), result)
    assert result.is_compatible()
    assert normalize_options_ordered(ordered_proto).to_schema() == normalize_options_ordered(unordered_proto).to_schema()
