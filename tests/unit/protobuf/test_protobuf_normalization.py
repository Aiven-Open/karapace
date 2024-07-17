"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.location import Location
from karapace.protobuf.proto_normalizations import normalize
from karapace.protobuf.proto_parser import ProtoParser
from karapace.protobuf.protopace import check_compatibility, format_proto, Proto

import pytest

location: Location = Location("some/folder", "file.proto")


# this would be a good case for using a property based test with a well-formed message generator
# (hypothesis could work if the objects representing the language
# do always correspond to a valid proto schema, that for a correct parser should always be true)
# we should create a set of random valid DSLs and check that the normalization is giving back a sorted options list
#
# nb: we could use the protoc compiler as an oracle to check that the files are always valid protobuf schemas

PROTO_WITH_OPTIONS_ORDERED = """\
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
}
"""

PROTO_WITH_OPTIONS_UNORDERED = """\
syntax = "proto3";

package pkg;

option java_package = "com.example.foo";
option cc_generic_services = true;
option java_generate_equals_and_hash = true;
option java_generic_services = true;
option java_outer_classname = "FooProto";
option optimize_for = SPEED;
option java_string_check_utf8 = true;
option java_multiple_files = true;

message Foo {
    string fieldA = 1;
}
"""


PROTO_WITH_OPTIONS_IN_ENUM_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
    option (my_option3) = "my_value3";

    ACTIVE = 0;
}
"""

PROTO_WITH_OPTIONS_IN_ENUM_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    option (my_option3) = "my_value3";
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";

    ACTIVE = 0;
}
"""

PROTO_WITH_OPTIONS_IN_SERVICE_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.ServiceOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

service MyService {
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
    option (my_option3) = "my_value3";
}
"""

PROTO_WITH_OPTIONS_IN_SERVICE_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.ServiceOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

service MyService {
    option (my_option3) = "my_value3";
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
}
"""

PROTO_WITH_OPTIONS_IN_RPC_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.MethodOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    string res = 1;
}

service MyService {
    rpc MyRpc (Foo) returns (Foo) {
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
        option (my_option3) = "my_value3";
    }
}
"""

PROTO_WITH_OPTIONS_IN_RPC_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.MethodOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    string res = 1;
}

service MyService {
    rpc MyRpc (Foo) returns (Foo) {
        option (my_option3) = "my_value3";
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
    }
}
"""

PROTO_WITH_OPTIONS_IN_EXTEND_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.MessageOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    string fieldA = 1;
}

extend Foo {
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
    option (my_option3) = "my_value3";
}
"""

PROTO_WITH_OPTIONS_IN_EXTEND_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.MessageOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    string fieldA = 1;
}

extend Foo {
    option (my_option3) = "my_value3";
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
}
"""

PROTO_WITH_OPTIONS_IN_ONEOF_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.OneofOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    oneof my_oneof {
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
        option (my_option3) = "my_value3";

        string test = 1;
    }
}
"""

PROTO_WITH_OPTIONS_IN_ONEOF_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.OneofOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    oneof my_oneof {
        option (my_option3) = "my_value3";
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";

        string test = 1;
    }
}
"""

PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumValueOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    MY_ENUM_CONSTANT = 0 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
}
"""

PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumValueOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    MY_ENUM_CONSTANT = 0 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.FieldOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    string fieldA = 1 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.FieldOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    string fieldA = 1 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
}
"""

PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    enum MyEnum {
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
        option (my_option3) = "my_value3";

        ACTIVE = 0;
    }
}
"""

PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    enum MyEnum {
        option (my_option3) = "my_value3";
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";

        ACTIVE = 0;
    }
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.FieldOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    message Bar {
        string fieldA = 1 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
    }
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.FieldOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

message Foo {
    message Bar {
        string fieldA = 1 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
    }
}
"""


PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.FieldOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    MY_ENUM_CONSTANT = 0;
}

message Foo {
    MyEnum fieldA = 1 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.FieldOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    MY_ENUM_CONSTANT = 0;
}

message Foo {
    MyEnum fieldA = 1 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_WITH_OPTIONS_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumValueOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    MY_ENUM_CONSTANT = 0 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
}

message Foo {
    MyEnum fieldA = 1;
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_WITH_OPTIONS_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumValueOptions {
  string my_option = 50002;
  string my_option2 = 50003;
  string my_option3 = 50004;
}

enum MyEnum {
    MY_ENUM_CONSTANT = 0 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
}

message Foo {
    MyEnum fieldA = 1;
}
"""

PROTO_WITH_COMPLEX_SCHEMA_ORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumValueOptions {
  string my_option_EnumValue = 50002;
  string my_option2_EnumValue = 50003;
  string my_option3_EnumValue = 50004;
}

extend google.protobuf.EnumOptions {
  string my_option_Enum = 50002;
  string my_option2_Enum = 50003;
  string my_option3_Enum = 50004;
}

extend google.protobuf.FieldOptions {
  string my_option_Field = 50002;
  string my_option2_Field = 50003;
  string my_option3_Field = 50004;
}

extend google.protobuf.OneofOptions {
  string my_option_Oneof = 50002;
  string my_option2_Oneof = 50003;
  string my_option3_Oneof = 50004;
}

extend google.protobuf.MessageOptions {
  string my_option_Message = 50002;
  string my_option2_Message = 50003;
  string my_option3_Message = 50004;
}

extend google.protobuf.MethodOptions {
  string my_option_Method = 50002;
  string my_option2_Method = 50003;
  string my_option3_Method = 50004;
}

extend google.protobuf.ServiceOptions {
  string my_option_Service = 50002;
  string my_option2_Service = 50003;
  string my_option3_Service = 50004;
}

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

  message NestedFoo {
    string fieldA = 1;
    option (my_option_Message) = "my_value";
    option (my_option2_Message) = "my_value2";
  }


  option (my_option3_Message) = "my_value3";
  option (my_option2_Message) = "my_value2";
  option (my_option_Message) = "my_value";


  oneof my_oneof {
    option (my_option3_Oneof) = "my_value3";
    option (my_option2_Oneof) = "my_value2";
    option (my_option_Oneof) = "my_value";

    string test = 5;
  }


 enum MyEnum {
   option (my_option3_Enum) = "my_value3";
   option (my_option2_Enum) = "my_value2";
   option (my_option_Enum) = "my_value";

   ACTIVE = 0;
 }
}


service MyService {
    option (my_option3_Service) = "my_value3";
    option (my_option2_Service) = "my_value2";
    option (my_option_Service) = "my_value";


    rpc MyRpc (Foo) returns (Foo) {
        option (my_option_Method) = "my_value";
        option (my_option2_Method) = "my_value2";
        option (my_option3_Method) = "my_value3";
    }
}


"""

PROTO_WITH_COMPLEX_SCHEMA_UNORDERED = """\
syntax = "proto3";

package pkg;

import "google/protobuf/descriptor.proto";

extend google.protobuf.EnumValueOptions {
  string my_option_EnumValue = 50002;
  string my_option2_EnumValue = 50003;
  string my_option3_EnumValue = 50004;
}

extend google.protobuf.EnumOptions {
  string my_option_Enum = 50002;
  string my_option2_Enum = 50003;
  string my_option3_Enum = 50004;
}

extend google.protobuf.FieldOptions {
  string my_option_Field = 50002;
  string my_option2_Field = 50003;
  string my_option3_Field = 50004;
}

extend google.protobuf.OneofOptions {
  string my_option_Oneof = 50002;
  string my_option2_Oneof = 50003;
  string my_option3_Oneof = 50004;
}

extend google.protobuf.MessageOptions {
  string my_option_Message = 50002;
  string my_option2_Message = 50003;
  string my_option3_Message = 50004;
}

extend google.protobuf.MethodOptions {
  string my_option_Method = 50002;
  string my_option2_Method = 50003;
  string my_option3_Method = 50004;
}

extend google.protobuf.ServiceOptions {
  string my_option_Service = 50002;
  string my_option2_Service = 50003;
  string my_option3_Service = 50004;
}

option cc_generic_services = true;
option java_outer_classname = "FooProto";
option optimize_for = SPEED;
option java_string_check_utf8 = true;
option java_generate_equals_and_hash = true;
option java_generic_services = true;
option java_multiple_files = true;
option java_package = "com.example.foo";

message Foo {
  string fieldA = 1;

  string fieldB = 2;

  string fieldC = 3;

  string fieldX = 4;

  message NestedFoo {
    string fieldA = 1;
    option (my_option2_Message) = "my_value2";
    option (my_option_Message) = "my_value";
  }

  option (my_option2_Message) = "my_value2";
  option (my_option3_Message) = "my_value3";
  option (my_option_Message) = "my_value";

  oneof my_oneof {
    option (my_option_Oneof) = "my_value";
    option (my_option3_Oneof) = "my_value3";
    option (my_option2_Oneof) = "my_value2";

    string test = 5;
  }


 enum MyEnum {
   option (my_option_Enum) = "my_value";
   option (my_option3_Enum) = "my_value3";
   option (my_option2_Enum) = "my_value2";

   ACTIVE = 0;
 }
}


service MyService {
    option (my_option2_Service) = "my_value2";
    option (my_option_Service) = "my_value";
    option (my_option3_Service) = "my_value3";

    rpc MyRpc (Foo) returns (Foo) {
        option (my_option2_Method) = "my_value2";
        option (my_option3_Method) = "my_value3";
        option (my_option_Method) = "my_value";
    }
}


"""


@pytest.mark.parametrize(
    ("ordered_schema", "unordered_schema"),
    (
        (PROTO_WITH_OPTIONS_ORDERED, PROTO_WITH_OPTIONS_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_ENUM_ORDERED, PROTO_WITH_OPTIONS_IN_ENUM_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_SERVICE_ORDERED, PROTO_WITH_OPTIONS_IN_SERVICE_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_RPC_ORDERED, PROTO_WITH_OPTIONS_IN_RPC_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_EXTEND_ORDERED, PROTO_WITH_OPTIONS_IN_EXTEND_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_ONEOF_ORDERED, PROTO_WITH_OPTIONS_IN_ONEOF_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_ORDERED, PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_ORDERED, PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_UNORDERED),
        (PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_ORDERED, PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_UNORDERED),
        (
            PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_ORDERED,
            PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_UNORDERED,
        ),
        (PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_ORDERED, PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_UNORDERED),
        (
            PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_WITH_OPTIONS_ORDERED,
            PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_WITH_OPTIONS_UNORDERED,
        ),
        (PROTO_WITH_COMPLEX_SCHEMA_ORDERED, PROTO_WITH_COMPLEX_SCHEMA_UNORDERED),
    ),
)
def test_differently_ordered_options_normalizes_equally(ordered_schema: str, unordered_schema: str) -> None:
    ordered_proto = ProtoParser.parse(location, ordered_schema)
    unordered_proto = ProtoParser.parse(location, unordered_schema)

    result = CompareResult()
    normalize(ordered_proto).compare(normalize(unordered_proto), result)
    assert result.is_compatible()
    assert normalize(ordered_proto).to_schema() == normalize(unordered_proto).to_schema()


@pytest.mark.parametrize(
    ("ordered_schema", "unordered_schema"),
    (
        (PROTO_WITH_OPTIONS_ORDERED, PROTO_WITH_OPTIONS_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_ENUM_ORDERED, PROTO_WITH_OPTIONS_IN_ENUM_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_SERVICE_ORDERED, PROTO_WITH_OPTIONS_IN_SERVICE_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_RPC_ORDERED, PROTO_WITH_OPTIONS_IN_RPC_UNORDERED),
        # Note: This does not work. Is it valid proto?
        # (PROTO_WITH_OPTIONS_IN_EXTEND_ORDERED, PROTO_WITH_OPTIONS_IN_EXTEND_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_ONEOF_ORDERED, PROTO_WITH_OPTIONS_IN_ONEOF_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_ORDERED, PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_UNORDERED),
        (PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_ORDERED, PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_UNORDERED),
        (PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_ORDERED, PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_UNORDERED),
        (
            PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_ORDERED,
            PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_UNORDERED,
        ),
        (PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_ORDERED, PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_UNORDERED),
        (
            PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_WITH_OPTIONS_ORDERED,
            PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_WITH_OPTIONS_UNORDERED,
        ),
        (PROTO_WITH_COMPLEX_SCHEMA_ORDERED, PROTO_WITH_COMPLEX_SCHEMA_UNORDERED),
    ),
)
def test_differently_ordered_options_normalizes_equally_with_protopace(ordered_schema: str, unordered_schema: str) -> None:
    ordered_proto = Proto("test.proto", ordered_schema)
    unordered_proto = Proto("test.proto", unordered_schema)

    ordered_str = format_proto(ordered_proto)
    unordered_str = format_proto(unordered_proto)

    assert ordered_str == unordered_str
    check_compatibility(ordered_proto, unordered_proto)
