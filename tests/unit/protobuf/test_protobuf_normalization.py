"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.location import Location
from karapace.protobuf.proto_normalizations import normalize_options_ordered
from karapace.protobuf.proto_parser import ProtoParser

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

enum MyEnum {
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
    option (my_option3) = "my_value3";
}
"""

PROTO_WITH_OPTIONS_IN_ENUM_UNORDERED = """\
syntax = "proto3";

package pkg;

enum MyEnum {
    option (my_option3) = "my_value3";
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
}
"""

PROTO_WITH_OPTIONS_IN_SERVICE_ORDERED = """\
syntax = "proto3";

package pkg;

service MyService {
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
    option (my_option3) = "my_value3";
}
"""

PROTO_WITH_OPTIONS_IN_SERVICE_UNORDERED = """\
syntax = "proto3";

package pkg;

service MyService {
    option (my_option3) = "my_value3";
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
}
"""

PROTO_WITH_OPTIONS_IN_RPC_ORDERED = """\
syntax = "proto3";

package pkg;

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

message Foo {
    oneof my_oneof {
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
        option (my_option3) = "my_value3";
    }
}
"""

PROTO_WITH_OPTIONS_IN_ONEOF_UNORDERED = """\
syntax = "proto3";

package pkg;

message Foo {
    oneof my_oneof {
        option (my_option3) = "my_value3";
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
    }
}
"""

PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_ORDERED = """\
syntax = "proto3";

package pkg;

enum MyEnum {
    MY_ENUM_CONSTANT = 0 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
}
"""

PROTO_WITH_OPTIONS_IN_ENUM_CONSTANTS_UNORDERED = """\
syntax = "proto3";

package pkg;

enum MyEnum {
    MY_ENUM_CONSTANT = 0 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_ORDERED = """\
syntax = "proto3";

package pkg;

message Foo {
    string fieldA = 1 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_UNORDERED = """\
syntax = "proto3";

package pkg;

message Foo {
    string fieldA = 1 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
}
"""

PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_ORDERED = """\
syntax = "proto3";

package pkg;

message Foo {
    enum MyEnum {
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
        option (my_option3) = "my_value3";
    }
}
"""

PROTO_WITH_NEASTED_ENUM_IN_MESSAGE_WITH_OPTIONS_UNORDERED = """\
syntax = "proto3";

package pkg;

message Foo {
    enum MyEnum {
        option (my_option3) = "my_value3";
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
    }
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_ORDERED = """\
syntax = "proto3";

package pkg;

message Foo {
    message Bar {
        string fieldA = 1 [(my_option) = "my_value", (my_option2) = "my_value2", (my_option3) = "my_value3"];
    }
}
"""

PROTO_WITH_OPTIONS_IN_FIELD_OF_MESSAGE_WITH_OPTIONS_UNORDERED = """\
syntax = "proto3";

package pkg;

message Foo {
    message Bar {
        string fieldA = 1 [(my_option3) = "my_value3", (my_option) = "my_value", (my_option2) = "my_value2"];
    }
}
"""


PROTO_WITH_OPTIONS_IN_FIELD_OF_ENUM_ORDERED = """\
syntax = "proto3";

package pkg;

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

  message NeastedFoo {
    string fieldA = 1;
    option (my_option) = "my_value";
    option (my_option2) = "my_value2";
  }


  option (my_option3) = "my_value3";
  option (my_option2) = "my_value2";
  option (my_option) = "my_value";


  oneof my_oneof {
    option (my_option3) = "my_value3";
    option (my_option2) = "my_value2";
    option (my_option) = "my_value";
  }


 enum MyEnum {
   option (my_option3) = "my_value3";
   option (my_option2) = "my_value2";
   option (my_option) = "my_value";
 }
}


extend Foo {
    option (my_option3) = "my_value3";
    option (my_option2) = "my_value2";
    option (my_option) = "my_value";
}

service MyService {
    option (my_option3) = "my_value3";
    option (my_option2) = "my_value2";
    option (my_option) = "my_value";


    rpc MyRpc (Foo) returns (Foo) {
        option (my_option) = "my_value";
        option (my_option2) = "my_value2";
        option (my_option3) = "my_value3";
    }
}


"""

PROTO_WITH_COMPLEX_SCHEMA_UNORDERED = """\
syntax = "proto3";

package pkg;

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

  message NeastedFoo {
    string fieldA = 1;
    option (my_option2) = "my_value2";
    option (my_option) = "my_value";
  }

  option (my_option2) = "my_value2";
  option (my_option3) = "my_value3";
  option (my_option) = "my_value";

  oneof my_oneof {
    option (my_option) = "my_value";
    option (my_option3) = "my_value3";
    option (my_option2) = "my_value2";
  }


 enum MyEnum {
   option (my_option) = "my_value";
   option (my_option3) = "my_value3";
   option (my_option2) = "my_value2";
 }
}


extend Foo {
    option (my_option2) = "my_value2";
    option (my_option3) = "my_value3";
    option (my_option) = "my_value";
}


service MyService {
    option (my_option2) = "my_value2";
    option (my_option) = "my_value";
    option (my_option3) = "my_value3";

    rpc MyRpc (Foo) returns (Foo) {
        option (my_option2) = "my_value2";
        option (my_option3) = "my_value3";
        option (my_option) = "my_value";
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
def test_different_options_order_its_correctly_normalized(ordered_schema: str, unordered_schema: str) -> None:
    ordered_proto = ProtoParser.parse(location, ordered_schema)
    unordered_proto = ProtoParser.parse(location, unordered_schema)

    result = CompareResult()
    normalize_options_ordered(ordered_proto).compare(normalize_options_ordered(unordered_proto), result)
    assert result.is_compatible()
    assert normalize_options_ordered(ordered_proto).to_schema() == normalize_options_ordered(unordered_proto).to_schema()
