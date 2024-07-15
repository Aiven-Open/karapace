"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.dependency import Dependency
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.location import Location
from karapace.protobuf.proto_normalizations import normalize
from karapace.schema_models import parse_protobuf_schema_definition, ValidatedTypedSchema
from karapace.schema_type import SchemaType
from karapace.typing import Subject, Version
from typing import Final

import pytest

LOCATION: Final[Location] = Location("somefolder", "file.proto")


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
    normalize(ordered_proto).compare(normalize(unordered_proto), result)
    assert result.is_compatible()
    assert normalize(ordered_proto).to_schema() == normalize(unordered_proto).to_schema()


DEPENDENCY = """\
syntax = "proto3";
package my.awesome.customer.v1;

message NestedValue {
    string value = 1;
}
\
"""

PROTO_WITH_FULLY_QUALIFIED_PATHS = """\
syntax = "proto3";
package my.awesome.customer.v1;

import "my/awesome/customer/v1/nested_value.proto";
import "google/protobuf/timestamp.proto";

option csharp_namespace = "my.awesome.customer.V1";
option go_package = "github.com/customer/api/my/awesome/customer/v1;dspv1";
option java_multiple_files = true;
option java_outer_classname = "EventValueProto";
option java_package = "com.my.awesome.customer.v1";
option objc_class_prefix = "TDD";
option php_metadata_namespace = "My\\Awesome\\Customer\\V1";
option php_namespace = "My\\Awesome\\Customer\\V1";
option ruby_package = "My::Awesome::Customer::V1";

message EventValue {
  .my.awesome.customer.v1.NestedValue nested_value = 1;
  .google.protobuf.Timestamp created_at = 2;
}
"""


PROTO_WITH_SIMPLE_NAMES = """\
syntax = "proto3";
package my.awesome.customer.v1;

import "my/awesome/customer/v1/nested_value.proto";
import "google/protobuf/timestamp.proto";

option csharp_namespace = "my.awesome.customer.V1";
option go_package = "github.com/customer/api/my/awesome/customer/v1;dspv1";
option java_multiple_files = true;
option java_outer_classname = "EventValueProto";
option java_package = "com.my.awesome.customer.v1";
option objc_class_prefix = "TDD";
option php_metadata_namespace = "My\\Awesome\\Customer\\V1";
option php_namespace = "My\\Awesome\\Customer\\V1";
option ruby_package = "My::Awesome::Customer::V1";

message EventValue {
  NestedValue nested_value = 1;
  google.protobuf.Timestamp created_at = 2;
}"""


def test_full_path_and_simple_names_are_equal() -> None:
    """
    This test aims to ensure that after the normalization process the schema expressed as SimpleNames will match the same
    schemas expressed with fully qualified references.
    This does not consider the normalization process for the different ways a type can be expressed as a relative reference.

    Long explaination below:

    If we accept the specifications from buf as correct, we can look at how a type
    reference is defined here: https://protobuf.com/docs/language-spec#type-references.

    The main problem with the previous implementation is that the current parser can't tell
    if a fully-qualified type reference in dot notation is the same as one identified by name
    alone aka simple name notation (https://protobuf.com/docs/language-spec#fully-qualified-references).

    The fix do not consider all the different ways users can define a relative reference, schemas
     with different way of expressing a relative reference even if normalized,
     for now will keep being considered different (https://protobuf.com/docs/language-spec#relative-references).

    Right now, our logic removes the `.package_name` (+ the Message scope) part
     before comparing field modifications (in fact it re-writes the schema using the simple name notation).

    Even though the TypeTree (trie data structure) could help resolve
    relative references (https://protobuf.com/docs/language-spec#relative-references),
    we don't want to add this feature in the python implementation now because it might cause new bugs due to the
    non-trivial behaviour of the protoc compiler.

    We plan to properly normalize the protobuf schemas later.

    We'll use protobuf descriptors (after the compilation and linking step)
    to gather type references already resolved, and we will threaten all the protobuf
    using always the fully qualified names.
    Properly handling all path variations means reimplementing the protoc compiler behavior,
     we prefer relying on the already processed proto descriptor.

    So, for now, we'll only implement a normalization for the fully-qualified references
    in dot notation and by simple name alone.

    This is not changing the semantics of the message since the local scope its always the one
    with max priority, so if you get rid of the fully-qualified reference protoc will resolve
    the reference with the one specified in the package scope.
    """
    no_ref_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, DEPENDENCY, normalize=True)
    dep = Dependency("NestedValue.proto", Subject("nested_value"), Version(1), no_ref_schema)
    dependencies = {"NestedValue.proto": dep}
    fully_qualitifed_simple_name_notation = parse_protobuf_schema_definition(
        schema_definition=PROTO_WITH_SIMPLE_NAMES,
        references=None,
        dependencies=dependencies,
        validate_references=True,
        normalize=False,
    )
    fully_qualitifed_dot_notation = parse_protobuf_schema_definition(
        schema_definition=PROTO_WITH_FULLY_QUALIFIED_PATHS,
        references=None,
        dependencies=dependencies,
        validate_references=True,
        normalize=True,
    )
    result = CompareResult()
    fully_qualitifed_simple_name_notation.compare(normalize(fully_qualitifed_dot_notation), result)
    assert result.is_compatible(), "normalized itn't equal to simple name"
    assert (
        normalize(fully_qualitifed_dot_notation).to_schema() == fully_qualitifed_simple_name_notation.to_schema()
    ), "normalization should transform it into an equivalent simple name"

    fully_qualitifed_simple_name_notation.compare(normalize(fully_qualitifed_simple_name_notation), result)
    assert result.is_compatible(), "normalization shouldn't change a simple name notation protofile"
    assert (
        fully_qualitifed_simple_name_notation.to_schema() == normalize(fully_qualitifed_dot_notation).to_schema()
    ), "also the string rendering shouldn't change a simple name notation protofile"


TRICKY_DEPENDENCY = """\
syntax = "proto3";
package org.my.awesome.customer.v1;

message NestedValue {
    string value = 1;
}\
"""

PROTO_WITH_FULLY_QUALIFIED_PATHS_AND_TRICKY_DEPENDENCY = """\
syntax = "proto3";

package my.awesome.customer.v1;

import "org/my/awesome/customer/v1/nested_value.proto";
import "my/awesome/customer/v1/nested_value.proto";
import "google/protobuf/timestamp.proto";

option csharp_namespace = "my.awesome.customer.V1";
option go_package = "github.com/customer/api/my/awesome/customer/v1;dspv1";
option java_multiple_files = true;
option java_outer_classname = "EventValueProto";
option java_package = "com.my.awesome.customer.v1";
option objc_class_prefix = "TDD";
option php_metadata_namespace = "MyAwesomeCustomerV1";
option php_namespace = "MyAwesomeCustomerV1";
option ruby_package = "My::Awesome::Customer::V1";

message EventValue {
  .my.awesome.customer.v1.NestedValue nested_value = 1;

  google.protobuf.Timestamp created_at = 2;
}
"""


def test_full_path_and_simple_names_are_not_equal_if_simple_name_is_not_unique() -> None:
    no_ref_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, DEPENDENCY, normalize=True)
    tricky_no_ref_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, TRICKY_DEPENDENCY, normalize=True)
    dep = Dependency("NestedValue.proto", Subject("nested_value"), Version(1), no_ref_schema)
    tricky_dep = Dependency("TrickyNestedValue.proto", Subject("tricky_nested_value"), Version(1), tricky_no_ref_schema)
    dependencies = {"NestedValue.proto": dep, "TrickyNestedValue.proto": tricky_dep}
    schema = parse_protobuf_schema_definition(
        schema_definition=PROTO_WITH_FULLY_QUALIFIED_PATHS_AND_TRICKY_DEPENDENCY,
        references=None,
        dependencies=dependencies,
        validate_references=True,
        normalize=False,
    ).to_schema()
    normalized_schema = parse_protobuf_schema_definition(
        schema_definition=PROTO_WITH_FULLY_QUALIFIED_PATHS_AND_TRICKY_DEPENDENCY,
        references=None,
        dependencies=dependencies,
        validate_references=True,
        normalize=True,
    ).to_schema()

    assert normalized_schema == schema, "Since the simple name is not unique identifying the type isn't replacing the source"
