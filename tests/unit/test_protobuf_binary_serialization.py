"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import pytest

from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.protobuf.serialization import deserialize, serialize
import google.protobuf.descriptor_pb2 as _pb2
import base64
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


# Binary FileDescriptorProto produced by google.protobuf.descriptor_pb2 from:
#   syntax = "proto3";
#   message MapMessage {
#     map<string, string> labels = 1;
#   }
# The binary does NOT contain map<> shorthand — protoc always compiles map<K,V> to the
# expanded entry-message form: a repeated field plus a synthetic LabelsEntry nested
# message with options { map_entry: true } (wire bytes 3a 02 38 01 at offset 98).
_MAP_ENTRY_BIN = "ImQKCk1hcE1lc3NhZ2USJwoGbGFiZWxzGAEgAygLMhcuTWFwTWVzc2FnZS5MYWJlbHNFbnRyeRotCgtMYWJlbHNFbnRyeRILCgNrZXkYASABKAkSDQoFdmFsdWUYAiABKAk6AjgBYgZwcm90bzM="


def test_map_entry_option_preserved_on_deserialize():
    """Binary-registered schema with map<string,string>: map_entry=true must survive deserialize."""
    pfe = deserialize(_MAP_ENTRY_BIN)
    schema_text = ProtobufSchema("", None, None, proto_file_element=pfe).to_schema()
    assert "map_entry = true" in schema_text, f"map_entry not found in:\n{schema_text}"


def test_map_entry_option_preserved_on_serialize_roundtrip():
    """map_entry=true must survive a full deserialize → serialize → deserialize round-trip."""
    pfe = deserialize(_MAP_ENTRY_BIN)
    re_serialized = serialize(pfe)
    # Re-serialized binary must still contain 3a 02 38 01
    raw = base64.b64decode(re_serialized)
    assert bytes([0x3A, 0x02, 0x38, 0x01]) in raw, "3a 02 38 01 (map_entry=true) missing from re-serialized descriptor"
    # And the text form must still declare it
    pfe2 = deserialize(re_serialized)
    schema_text = ProtobufSchema("", None, None, proto_file_element=pfe2).to_schema()
    assert "map_entry = true" in schema_text


def test_map_entry_option_only_on_entry_message():
    """map_entry=true must appear only on LabelsEntry, not on the outer MapMessage."""
    pfe = deserialize(_MAP_ENTRY_BIN)
    from karapace.core.protobuf.message_element import MessageElement
    messages = {t.name: t for t in pfe.types if isinstance(t, MessageElement)}
    outer = messages["MapMessage"]
    assert not any(o.name == "map_entry" for o in outer.options), "map_entry leaked onto outer message"
    entry = next(t for t in outer.nested_types if isinstance(t, MessageElement) and t.name == "LabelsEntry")
    assert any(o.name == "map_entry" and o.value is True for o in entry.options), "map_entry missing from LabelsEntry"
