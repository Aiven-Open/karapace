"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.dependency import Dependency
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.schema import ProtobufSchema, SourceFileReference, TypeTree
from karapace.schema_models import SchemaType, ValidatedTypedSchema
from karapace.typing import Subject
from tests.schemas.protobuf import (
    schema_protobuf_compare_one,
    schema_protobuf_order_after,
    schema_protobuf_order_before,
    schema_protobuf_schema_registry1,
)

location: Location = Location("", "file.proto")


def test_protobuf_schema_simple():
    proto = trim_margin(schema_protobuf_schema_registry1)
    protobuf_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto)
    result = str(protobuf_schema)

    assert result == proto


def test_protobuf_schema_sort():
    proto = trim_margin(schema_protobuf_order_before)
    protobuf_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto)
    result = str(protobuf_schema)
    proto2 = trim_margin(schema_protobuf_order_after)
    assert result == proto2


def test_protobuf_schema_compare():
    proto1 = trim_margin(schema_protobuf_order_after)
    protobuf_schema1: ValidatedTypedSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1)
    proto2 = trim_margin(schema_protobuf_compare_one)
    protobuf_schema2: ValidatedTypedSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2)
    result = CompareResult()
    protobuf_schema1.schema.compare(protobuf_schema2.schema, result)
    assert result.is_compatible()


def test_protobuf_schema_compare2():
    proto1 = trim_margin(schema_protobuf_order_after)
    protobuf_schema1: ValidatedTypedSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1)
    proto2 = trim_margin(schema_protobuf_compare_one)
    protobuf_schema2: ValidatedTypedSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2)
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
    protobuf_schema1: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
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

    protobuf_schema1: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
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

    protobuf_schema1: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
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

    protobuf_schema1: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
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

    protobuf_schema1: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
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

    protobuf_schema1: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
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

    protobuf_schema1: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema
    protobuf_schema2: ProtobufSchema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema
    result = CompareResult()

    protobuf_schema1.compare(protobuf_schema2, result)

    assert result.is_compatible()


def test_protobuf_self_referencing_schema():
    proto1 = """\
    syntax = "proto3";

    package fancy.company.in.party.v1;
    message MyFirstMessage {
      string my_fancy_string = 1;
    }
    message AnotherMessage {
      message WowANestedMessage {
        enum BamFancyEnum {
          // Hei! This is a comment!
          MY_AWESOME_FIELD = 0;
        }
        BamFancyEnum im_tricky_im_referring_to_the_previous_enum = 1;
      }
    }
    """

    assert isinstance(ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto1).schema, ProtobufSchema)

    proto2 = """\
    syntax = "proto3";

    package fancy.company.in.party.v1;
    message AnotherMessage {
      enum BamFancyEnum {
        // Hei! This is a comment!
        MY_AWESOME_FIELD = 0;
      }
      message WowANestedMessage {
        message DeeplyNestedMsg {
          BamFancyEnum im_tricky_im_referring_to_the_previous_enum = 1;
        }
      }
    }
    """

    assert isinstance(ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto2).schema, ProtobufSchema)

    proto3 = """\
        syntax = "proto3";

        package fancy.company.in.party.v1;
        message AnotherMessage {
          enum BamFancyEnum {
            // Hei! This is a comment!
            MY_AWESOME_FIELD = 0;
          }
          message WowANestedMessage {
            message DeeplyNestedMsg {
              message AnotherLevelOfNesting {
                 BamFancyEnum im_tricky_im_referring_to_the_previous_enum = 1;
              }
            }
          }
        }
        """

    assert isinstance(ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto3).schema, ProtobufSchema)

    proto4 = """\
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
        }
        """

    assert isinstance(ValidatedTypedSchema.parse(SchemaType.PROTOBUF, proto4).schema, ProtobufSchema)


def partial_path_protobuf_schema() -> ProtobufSchema:
    plan = """\
    syntax = "proto3";

    package my.awesome.customer.plan.entity.v1beta1;

    message CustomerPlan {
      string plan_name = 1;
    }
    """

    customer_plan_event = """\
    syntax = "proto3";

    package my.awesome.customer.plan.entity.v1beta1;
    import "my/awesome/customer/plan/entity/v1beta1/entity.proto";

    message CustomerPlanEvent {
      message Created {
        //the full path declaration is `my.awesome.customer.plan.entity.v1beta1.CustomerPlan plan = 1;`
        entity.v1beta1.CustomerPlan plan = 1;
      }
    }
    """

    no_ref_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, plan)
    dep = Dependency("this_is_ignored", Subject("this_also"), 1, no_ref_schema)
    ref_schema = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, customer_plan_event, None, {"foobar": dep})
    schema = ref_schema.schema
    assert isinstance(schema, ProtobufSchema)
    return schema


def test_type_tree_rendering() -> None:
    schema = partial_path_protobuf_schema()
    assert (
        str(schema.types_tree())
        == """. ->
   CustomerPlan ->
      v1beta1 ->
         entity ->
            plan ->
               customer ->
                  awesome ->
                     >my
   CustomerPlanEvent ->
      v1beta1 ->
         entity ->
            plan ->
               customer ->
                  awesome ->
                     >my
   Created ->
      CustomerPlanEvent ->
         v1beta1 ->
            entity ->
               plan ->
                  customer ->
                     awesome ->
                        >my"""
    )


def test_type_tree_parsed_structure() -> None:
    schema = partial_path_protobuf_schema()
    assert schema.types_tree() == TypeTree(
        token=".",
        children=[
            TypeTree(
                token="CustomerPlan",
                children=[
                    TypeTree(
                        token="v1beta1",
                        children=[
                            TypeTree(
                                token="entity",
                                children=[
                                    TypeTree(
                                        token="plan",
                                        children=[
                                            TypeTree(
                                                token="customer",
                                                children=[
                                                    TypeTree(
                                                        token="awesome",
                                                        children=[
                                                            TypeTree(
                                                                token="my",
                                                                children=[],
                                                                source_reference=SourceFileReference(
                                                                    reference="foobar", import_order=0
                                                                ),
                                                            )
                                                        ],
                                                        source_reference=None,
                                                    )
                                                ],
                                                source_reference=None,
                                            )
                                        ],
                                        source_reference=None,
                                    )
                                ],
                                source_reference=None,
                            )
                        ],
                        source_reference=None,
                    )
                ],
                source_reference=None,
            ),
            TypeTree(
                token="CustomerPlanEvent",
                children=[
                    TypeTree(
                        token="v1beta1",
                        children=[
                            TypeTree(
                                token="entity",
                                children=[
                                    TypeTree(
                                        token="plan",
                                        children=[
                                            TypeTree(
                                                token="customer",
                                                children=[
                                                    TypeTree(
                                                        token="awesome",
                                                        children=[
                                                            TypeTree(
                                                                token="my",
                                                                children=[],
                                                                source_reference=SourceFileReference(
                                                                    reference="main_schema_file", import_order=1
                                                                ),
                                                            )
                                                        ],
                                                        source_reference=None,
                                                    )
                                                ],
                                                source_reference=None,
                                            )
                                        ],
                                        source_reference=None,
                                    )
                                ],
                                source_reference=None,
                            )
                        ],
                        source_reference=None,
                    )
                ],
                source_reference=None,
            ),
            TypeTree(
                token="Created",
                children=[
                    TypeTree(
                        token="CustomerPlanEvent",
                        children=[
                            TypeTree(
                                token="v1beta1",
                                children=[
                                    TypeTree(
                                        token="entity",
                                        children=[
                                            TypeTree(
                                                token="plan",
                                                children=[
                                                    TypeTree(
                                                        token="customer",
                                                        children=[
                                                            TypeTree(
                                                                token="awesome",
                                                                children=[
                                                                    TypeTree(
                                                                        token="my",
                                                                        children=[],
                                                                        source_reference=SourceFileReference(
                                                                            reference="main_schema_file", import_order=2
                                                                        ),
                                                                    )
                                                                ],
                                                                source_reference=None,
                                                            )
                                                        ],
                                                        source_reference=None,
                                                    )
                                                ],
                                                source_reference=None,
                                            )
                                        ],
                                        source_reference=None,
                                    )
                                ],
                                source_reference=None,
                            )
                        ],
                        source_reference=None,
                    )
                ],
                source_reference=None,
            ),
        ],
        source_reference=None,
    )


def test_type_tree_expand_types() -> None:
    schema = partial_path_protobuf_schema()

    # the count start from 0
    assert schema.types_tree().inserted_elements() == 2

    tokens_to_seek = "entity.v1beta1.CustomerPlan".split(".")
    matched = schema.type_in_tree(schema.types_tree(), tokens_to_seek)
    assert matched is not None
    assert matched.expand_missing_absolute_path() == ["my", "awesome", "customer", "plan"]

    tokens_to_seek = "entity.v1beta1.CustomerPlan".split(".")
    matched = schema.type_in_tree(schema.types_tree(), tokens_to_seek)
    assert matched is not None
    assert matched.expand_missing_absolute_path() == ["my", "awesome", "customer", "plan"]

    matched = schema.type_in_tree(schema.types_tree(), ["Created"])
    assert matched is not None
    assert matched.expand_missing_absolute_path() == [
        "my",
        "awesome",
        "customer",
        "plan",
        "entity",
        "v1beta1",
        "CustomerPlanEvent",
    ]
