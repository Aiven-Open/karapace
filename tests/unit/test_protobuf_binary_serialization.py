"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.protobuf.serialization import deserialize, serialize
from tests.schemas.protobuf import (
    schema_protobuf_complex,
    schema_protobuf_complex_bin,
    schema_protobuf_container2,
    schema_protobuf_container2_bin,
    schema_protobuf_nested_field,
    schema_protobuf_nested_field_bin_protoc,
    schema_protobuf_nested_message4,
    schema_protobuf_nested_message4_bin,
    schema_protobuf_nested_message4_bin_protoc,
    schema_protobuf_oneof,
    schema_protobuf_oneof_bin,
    schema_protobuf_optionals,
    schema_protobuf_optionals_bin,
    schema_protobuf_order_after,
    schema_protobuf_order_after_bin,
    schema_protobuf_plain,
    schema_protobuf_plain_bin,
    schema_protobuf_plain_bin_protoc,
    schema_protobuf_references,
    schema_protobuf_references2,
    schema_protobuf_references2_bin,
    schema_protobuf_references_bin,
)

import pytest

schema_serialized1 = (
    "Cg5tZXNzYWdlcy5wcm90byIRCgNLZXkSCgoCaWQYASABKAUiMQoDRG9nEgwKBG5hbW"
    + "UYASABKAkSDgoGd2VpZ2h0GAIgASgFEgwKBHRveXMYBCADKAliBnByb3RvMw=="
)

schema_plain1 = """\
syntax = "proto3";

message Key {
  int32 id = 1;
}
message Dog {
  string name = 1;
  int32 weight = 2;
  repeated string toys = 4;
}
"""

schema_serialized_normalized = (
    "CgdkZWZhdWx0Ig8KA0tleRIICgJpZBgBKAUiLQoDRG9nEgoKBG5hbWUYASgJEgwKBndlaWdodBgCKAUSDAoEdG95cxgEIAMoCWIGcHJvdG8z"
)

schema_serialized2 = (
    "CiZwcm90by90ZWNoL2Rvam8vZHNwL3YxL2V2ZW50X2tleS5wcm90bxIQdGVjaC5kb2pvLmRzcC52MSIaCghFdmVudEtleRIOCgJpZBgBIAEoCVI"
    + "CaWRCiAEKFGNvbS50ZWNoLmRvam8uZHNwLnYxQg1FdmVudEtleVByb3RvUAGiAgNURESqAhBUZWNoLkRvam8uRHNwLlYxygIQVGVjaFxEb2pvXER"
    + "zcFxWMeICHFRlY2hcRG9qb1xEc3BcVjFcR1BCTWV0YWRhdGHqAhNUZWNoOjpEb2pvOjpEc3A6OlYxYgZwcm90bzM="
)

schema_plain2 = """\
syntax = "proto3";
package tech.dojo.dsp.v1;

option java_package = "com.tech.dojo.dsp.v1";
option java_outer_classname = "EventKeyProto";
option java_multiple_files = true;
option objc_class_prefix = "TDD";
option csharp_namespace = "Tech.Dojo.Dsp.V1";
option php_namespace = "Tech\\\\Dojo\\\\Dsp\\\\V1";
option php_metadata_namespace = "Tech\\\\Dojo\\\\Dsp\\\\V1\\\\GPBMetadata";
option ruby_package = "Tech::Dojo::Dsp::V1";

message EventKey {
  string id = 1;
}
"""


@pytest.mark.parametrize(
    "schema_plain,schema_serialized",
    [
        (schema_plain1, schema_serialized1),
        (schema_plain2, schema_serialized2),
        (schema_protobuf_plain, schema_protobuf_plain_bin),
        (schema_protobuf_order_after, schema_protobuf_order_after_bin),
        (schema_protobuf_nested_message4, schema_protobuf_nested_message4_bin),
        (schema_protobuf_oneof, schema_protobuf_oneof_bin),
        (schema_protobuf_container2, schema_protobuf_container2_bin),
        (schema_protobuf_references, schema_protobuf_references_bin),
        (schema_protobuf_references2, schema_protobuf_references2_bin),
        (schema_protobuf_complex, schema_protobuf_complex_bin),
        (schema_protobuf_optionals, schema_protobuf_optionals_bin),
    ],
)
def test_schema_deserialize(schema_plain, schema_serialized):
    assert (
        schema_plain.strip()
        == ProtobufSchema("", None, None, proto_file_element=deserialize(schema_serialized)).to_schema().strip()
    )


@pytest.mark.parametrize(
    "schema_plain,schema_serialized",
    [
        (schema_protobuf_plain, schema_protobuf_plain_bin_protoc),
        (schema_protobuf_nested_message4, schema_protobuf_nested_message4_bin_protoc),
        (schema_protobuf_nested_field, schema_protobuf_nested_field_bin_protoc),
    ],
)
def test_protoc_serialized_schema_deserialize(schema_plain, schema_serialized):
    assert (
        schema_plain.strip()
        == ProtobufSchema("", None, None, proto_file_element=deserialize(schema_serialized)).to_schema().strip()
    )


@pytest.mark.parametrize(
    "schema",
    [
        schema_plain1,
        schema_protobuf_plain,
        schema_protobuf_order_after,
        schema_protobuf_nested_message4,
        schema_protobuf_oneof,
        schema_protobuf_container2,
        schema_protobuf_references,
        schema_protobuf_references2,
        schema_protobuf_complex,
        schema_protobuf_optionals,
    ],
)
def test_simple_schema_serialize(schema):
    serialized = serialize(ProtobufSchema(schema).proto_file_element)
    assert schema.strip() == ProtobufSchema("", None, None, proto_file_element=deserialize(serialized)).to_schema().strip()
