"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_models import ValidatedTypedSchema
from karapace.schema_type import SchemaType
from tests.utils import schema_protobuf_second

import pytest

MESSAGE_WITH_ENUM = """\
syntax = "proto3";

option java_package = "com.codingharbour.protobuf";
option java_outer_classname = "TestEnumOrder";

message Speed {
  Enum speed = 1;
}

enum Enum {
  HIGH = 0;
  MIDDLE = 1;
  LOW = 2;
}\
"""

MESSAGE_WITH_ENUM_REORDERED = """\
syntax = "proto3";

option java_package = "com.codingharbour.protobuf";
option java_outer_classname = "TestEnumOrder";

enum Enum {
  HIGH = 0;
  MIDDLE = 1;
  LOW = 2;
}

message Speed {
  Enum speed = 1;
}\
"""

COMPLEX_MESSAGE_WITH_NESTING = """\
syntax = "proto3";

package fancy.company.in.party.v1;

message AnotherMessage {
  message WowANestedMessage {
    enum BamFancyEnum {
      // Hei! This is a comment!
      MY_AWESOME_FIELD = 0;
    }
    message DeeplyNestedMsg {
      message AnotherLevelOfNesting {
         BamFancyEnum im_tricky_im_referring_to_the_previous_enum = 1;
      }
    }
  }
}\
"""

MESSAGE_WITH_JUST_ENUMS = """\
syntax = "proto3";

option java_package = "com.codingharbour.protobuf";
option java_outer_classname = "TestEnumOrder";

enum Enum {
  HIGH = 0;
  MIDDLE = 1;
  LOW = 2;
}

enum Enum2 {
  HIGH = 0;
  MIDDLE = 1;
  LOW = 2;
}\
"""


def parse_avro_schema(schema: str) -> ProtobufSchema:
    parsed_schema = ValidatedTypedSchema.parse(
        SchemaType.PROTOBUF,
        schema,
    )
    return parsed_schema.schema


@pytest.mark.parametrize(
    "schema,expected_record_name",
    (
        (parse_avro_schema(MESSAGE_WITH_ENUM), "Speed"),
        (parse_avro_schema(MESSAGE_WITH_ENUM_REORDERED), "Speed"),
        (parse_avro_schema(schema_protobuf_second), "SensorInfo"),
        (parse_avro_schema(COMPLEX_MESSAGE_WITH_NESTING), "fancy.company.in.party.v1.AnotherMessage"),
        (parse_avro_schema(MESSAGE_WITH_JUST_ENUMS), "Enum"),
    ),
)
def test_record_name(schema: ProtobufSchema, expected_record_name: str):
    assert schema.record_name() == expected_record_name
