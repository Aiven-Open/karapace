"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.dependency import Dependency
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.location import Location
from karapace.protobuf.proto_normalizations import normalize
from karapace.schema_models import parse_protobuf_schema_definition, ValidatedTypedSchema
from karapace.schema_references import Reference
from karapace.schema_type import SchemaType
from karapace.typing import Subject, Version

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

  message NestedFoo {
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

  message NestedFoo {
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
def test_differently_ordered_options_normalizes_equally(ordered_schema: str, unordered_schema: str) -> None:
    ordered_proto = parse_protobuf_schema_definition(
        schema_definition=ordered_schema,
        references=None,
        dependencies=None,
        validate_references=True,
        normalize=True,
    )
    unordered_proto = parse_protobuf_schema_definition(
        schema_definition=unordered_schema,
        references=None,
        dependencies=None,
        validate_references=True,
        normalize=True,
    )

    result = CompareResult()
    ordered_proto.compare(unordered_proto, result)
    assert result.is_compatible()
    assert ordered_proto.to_schema() == unordered_proto.to_schema()


def test_schema_without_references_its_normalized() -> None:
    partial_path_schema = """\
syntax = "proto3";
package foo.bar;

message Baz {
  string name = 1;
  fixed64 uid = 2;
  Settings settings = 3;
  message Settings {
    bool frozen = 1;
    uint32 version = 2;
    repeated string attrs = 3;
  }
}
"""
    partial_path = parse_protobuf_schema_definition(
        schema_definition=partial_path_schema,
        references=None,
        dependencies=None,
        validate_references=True,
        normalize=True,
    )
    # assert on the completeness


def test_partial_path_protobuf_schema_is_equal_if_normalized() -> None:
    dependency = """\
syntax = "proto3";
package my.awesome.customer.request.v1beta1;
message RequestId {
 string request_id = 1;
}\
"""

    full_path_schema = """\
syntax = "proto3";
import "my/awesome/customer/request/v1beta1/request_id.proto";
message MessageRequest {
 my.awesome.customer.request.v1beta1.RequestId request_id = 1;
}\
"""

    partial_path_schema = """\
syntax = "proto3";
import "my/awesome/customer/request/v1beta1/request_id.proto";
message MessageRequest {
 awesome.customer.request.v1beta1.RequestId request_id = 1;
}\
"""

    filename = "Speed.proto"
    subject = Subject("requestid")
    version = Version(1)
    references = [Reference(name="RequestId.proto", subject=subject, version=1)]
    no_ref_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, dependency)
    dependency = {"dependency": Dependency(filename, subject, version, no_ref_schema)}
    full_path = parse_protobuf_schema_definition(
        schema_definition=full_path_schema,
        references=references,
        dependencies=dependency,
        validate_references=True,
        normalize=True,
    )
    partial_path = parse_protobuf_schema_definition(
        schema_definition=partial_path_schema,
        references=references,
        dependencies=dependency,
        validate_references=True,
        normalize=True,
    )

    result = CompareResult()
    full_path.compare(partial_path, result)
    assert result.is_compatible()
    assert (
        normalize(full_path.proto_file_element, full_path.types_tree()).to_schema()
        == normalize(partial_path.proto_file_element, partial_path.types_tree()).to_schema()
    )
