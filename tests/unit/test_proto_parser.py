# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/ProtoParserTest.kt

from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.exception import IllegalStateException
from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.extensions_element import ExtensionsElement
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.group_element import GroupElement
from karapace.protobuf.kotlin_wrapper import KotlinRange, trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.proto_parser import ProtoParser
from karapace.protobuf.reserved_element import ReservedElement
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.syntax import Syntax
from karapace.protobuf.utils import MAX_TAG_VALUE

import pytest

location: Location = Location.get("file.proto")


def test_type_parsing():
    proto = """
        |message Types {
        |  required any f1 = 1;
        |  required bool f2 = 2;
        |  required bytes f3 = 3;
        |  required double f4 = 4;
        |  required float f5 = 5;
        |  required fixed32 f6 = 6;
        |  required fixed64 f7 = 7;
        |  required int32 f8 = 8;
        |  required int64 f9 = 9;
        |  required sfixed32 f10 = 10;
        |  required sfixed64 f11 = 11;
        |  required sint32 f12 = 12;
        |  required sint64 f13 = 13;
        |  required string f14 = 14;
        |  required uint32 f15 = 15;
        |  required uint64 f16 = 16;
        |  map<string, bool> f17 = 17;
        |  map<arbitrary, nested.nested> f18 = 18;
        |  required arbitrary f19 = 19;
        |  required nested.nested f20 = 20;
        |}
        """
    proto = trim_margin(proto)

    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="Types",
                fields=[
                    FieldElement(
                        location=location.at(2, 3), label=Field.Label.REQUIRED, element_type="any", name="f1", tag=1
                    ),
                    FieldElement(
                        location=location.at(3, 3), label=Field.Label.REQUIRED, element_type="bool", name="f2", tag=2
                    ),
                    FieldElement(
                        location=location.at(4, 3), label=Field.Label.REQUIRED, element_type="bytes", name="f3", tag=3
                    ),
                    FieldElement(
                        location=location.at(5, 3), label=Field.Label.REQUIRED, element_type="double", name="f4", tag=4
                    ),
                    FieldElement(
                        location=location.at(6, 3), label=Field.Label.REQUIRED, element_type="float", name="f5", tag=5
                    ),
                    FieldElement(
                        location=location.at(7, 3), label=Field.Label.REQUIRED, element_type="fixed32", name="f6", tag=6
                    ),
                    FieldElement(
                        location=location.at(8, 3), label=Field.Label.REQUIRED, element_type="fixed64", name="f7", tag=7
                    ),
                    FieldElement(
                        location=location.at(9, 3), label=Field.Label.REQUIRED, element_type="int32", name="f8", tag=8
                    ),
                    FieldElement(
                        location=location.at(10, 3), label=Field.Label.REQUIRED, element_type="int64", name="f9", tag=9
                    ),
                    FieldElement(
                        location=location.at(11, 3), label=Field.Label.REQUIRED, element_type="sfixed32", name="f10", tag=10
                    ),
                    FieldElement(
                        location=location.at(12, 3), label=Field.Label.REQUIRED, element_type="sfixed64", name="f11", tag=11
                    ),
                    FieldElement(
                        location=location.at(13, 3), label=Field.Label.REQUIRED, element_type="sint32", name="f12", tag=12
                    ),
                    FieldElement(
                        location=location.at(14, 3), label=Field.Label.REQUIRED, element_type="sint64", name="f13", tag=13
                    ),
                    FieldElement(
                        location=location.at(15, 3), label=Field.Label.REQUIRED, element_type="string", name="f14", tag=14
                    ),
                    FieldElement(
                        location=location.at(16, 3), label=Field.Label.REQUIRED, element_type="uint32", name="f15", tag=15
                    ),
                    FieldElement(
                        location=location.at(17, 3), label=Field.Label.REQUIRED, element_type="uint64", name="f16", tag=16
                    ),
                    FieldElement(location=location.at(18, 3), element_type="map<string, bool>", name="f17", tag=17),
                    FieldElement(
                        location=location.at(19, 3), element_type="map<arbitrary, nested.nested>", name="f18", tag=18
                    ),
                    FieldElement(
                        location=location.at(20, 3),
                        label=Field.Label.REQUIRED,
                        element_type="arbitrary",
                        name="f19",
                        tag=19
                    ),
                    FieldElement(
                        location=location.at(21, 3),
                        label=Field.Label.REQUIRED,
                        element_type="nested.nested",
                        name="f20",
                        tag=20
                    )
                ]
            )
        ]
    )
    my = ProtoParser.parse(location, proto)
    assert my == expected


def test_map_with_label_throws():
    with pytest.raises(IllegalStateException, match="Syntax error in file.proto:1:15: 'map' type cannot have label"):
        ProtoParser.parse(location, "message Hey { required map<string, string> a = 1; }")
        pytest.fail("")

    with pytest.raises(IllegalStateException, match="Syntax error in file.proto:1:15: 'map' type cannot have label"):
        ProtoParser.parse(location, "message Hey { optional map<string, string> a = 1; }")
        pytest.fail("")

    with pytest.raises(IllegalStateException, match="Syntax error in file.proto:1:15: 'map' type cannot have label"):
        ProtoParser.parse(location, "message Hey { repeated map<string, string> a = 1; }")
        pytest.fail("")


def test_default_field_option_is_special():
    """ It looks like an option, but 'default' is special. It's not defined as an option.
    """
    proto = """
        |message Message {
        |  required string a = 1 [default = "b", faulted = "c"];
        |}
        |"""

    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="Message",
                fields=[
                    FieldElement(
                        location=location.at(2, 3),
                        label=Field.Label.REQUIRED,
                        element_type="string",
                        name="a",
                        default_value="b",
                        options=[OptionElement("faulted", OptionElement.Kind.STRING, "c")],
                        tag=1
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_json_name_option_is_special():
    """ It looks like an option, but 'json_name' is special. It's not defined as an option.
    """
    proto = """
        |message Message {
        |  required string a = 1 [json_name = "b", faulted = "c"];
        |}
        |"""
    proto = trim_margin(proto)

    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="Message",
                fields=[
                    FieldElement(
                        location=location.at(2, 3),
                        label=Field.Label.REQUIRED,
                        element_type="string",
                        name="a",
                        json_name="b",
                        tag=1,
                        options=[OptionElement("faulted", OptionElement.Kind.STRING, "c")]
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_single_line_comment():
    proto = """
        |// Test all the things!
        |message Test {}
        """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    element_type = parsed.types[0]
    assert element_type.documentation == "Test all the things!"


def test_multiple_single_line_comments():
    proto = """
        |// Test all
        |// the things!
        |message Test {}
        """
    proto = trim_margin(proto)
    expected = """
        |Test all
        |the things!
        """
    expected = trim_margin(expected)

    parsed = ProtoParser.parse(location, proto)
    element_type = parsed.types[0]
    assert element_type.documentation == expected


def test_single_line_javadoc_comment():
    proto = """
        |/** Test */
        |message Test {}
        |"""
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    element_type = parsed.types[0]
    assert element_type.documentation == "Test"


def test_multiline_javadoc_comment():
    proto = """
        |/**
        | * Test
        | *
        | * Foo
        | */
        |message Test {}
        |"""
    proto = trim_margin(proto)
    expected = """
        |Test
        |
        |Foo
        """
    expected = trim_margin(expected)
    parsed = ProtoParser.parse(location, proto)
    element_type = parsed.types[0]
    assert element_type.documentation == expected


def test_multiple_single_line_comments_with_leading_whitespace():
    proto = """
        |// Test
        |//   All
        |//     The
        |//       Things!
        |message Test {}
        """
    proto = trim_margin(proto)
    expected = """
        |Test
        |  All
        |    The
        |      Things!
        """
    expected = trim_margin(expected)
    parsed = ProtoParser.parse(location, proto)
    element_type = parsed.types[0]
    assert element_type.documentation == expected


def test_multiline_javadoc_comment_with_leading_whitespace():
    proto = """
        |/**
        | * Test
        | *   All
        | *     The
        | *       Things!
        | */
        |message Test {}
        """
    proto = trim_margin(proto)
    expected = """
        |Test
        |  All
        |    The
        |      Things!
        """
    expected = trim_margin(expected)
    parsed = ProtoParser.parse(location, proto)
    element_type = parsed.types[0]
    assert element_type.documentation == expected


def test_multiline_javadoc_comment_without_leading_asterisks():
    # We do not honor leading whitespace when the comment lacks leading asterisks.
    proto = """
        |/**
        | Test
        |   All
        |     The
        |       Things!
        | */
        |message Test {}
        """
    proto = trim_margin(proto)
    expected = """
        |Test
        |All
        |The
        |Things!
        """
    expected = trim_margin(expected)
    parsed = ProtoParser.parse(location, proto)
    element_type = parsed.types[0]
    assert element_type.documentation == expected


def test_message_field_trailing_comment():
    # Trailing message field comment.
    proto = """
        |message Test {
        |  optional string name = 1; // Test all the things!
        |}
        """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    message: MessageElement = parsed.types[0]
    field = message.fields[0]
    assert field.documentation == "Test all the things!"


def test_message_field_leading_and_trailing_comment_are_combined():
    proto = """
        |message Test {
        |  // Test all...
        |  optional string name = 1; // ...the things!
        |}
        """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    message: MessageElement = parsed.types[0]
    field = message.fields[0]
    assert field.documentation == "Test all...\n...the things!"


def test_trailing_comment_not_assigned_to_following_field():
    proto = """
        |message Test {
        |  optional string first_name = 1; // Testing!
        |  optional string last_name = 2;
        |}
        """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    message: MessageElement = parsed.types[0]
    field1 = message.fields[0]
    assert field1.documentation == "Testing!"
    field2 = message.fields[1]
    assert field2.documentation == ""


def test_enum_value_trailing_comment():
    proto = """
        |enum Test {
        |  FOO = 1; // Test all the things!
        |}
        """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    enum_element: EnumElement = parsed.types[0]
    value = enum_element.constants[0]
    assert value.documentation == "Test all the things!"


def test_trailing_singleline_comment():
    proto = """
        |enum Test {
        |  FOO = 1; /* Test all the things!  */
        |  BAR = 2;/*Test all the things!*/
        |}
        """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    enum_element: EnumElement = parsed.types[0]
    c_foo = enum_element.constants[0]
    assert c_foo.documentation == "Test all the things!"
    c_bar = enum_element.constants[1]
    assert c_bar.documentation == "Test all the things!"


def test_trailing_multiline_comment():
    proto = """
      |enum Test {
      |  FOO = 1; /* Test all the
      |things! */
      |}
      """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    enum_element: EnumElement = parsed.types[0]
    value = enum_element.constants[0]
    assert value.documentation == "Test all the\nthings!"


def test_trailing_multiline_comment_must_be_last_on_line_throws():
    proto = """
        |enum Test {
        |  FOO = 1; /* Test all the things! */ BAR = 2;
        |}
        """
    proto = trim_margin(proto)
    with pytest.raises(
        IllegalStateException, match="Syntax error in file.proto:2:40: no syntax may follow trailing comment"
    ):
        ProtoParser.parse(location, proto)
        pytest.fail("")


def test_invalid_trailing_comment():
    proto = """
        |enum Test {
        |  FOO = 1; /
        |}
        """
    proto = trim_margin(proto)

    with pytest.raises(IllegalStateException) as re:
        # TODO: this test in Kotlin source contains "2:13:" Need compile square.wire and check how it can be?

        ProtoParser.parse(location, proto)
        pytest.fail("")
    assert re.value.message == "Syntax error in file.proto:2:12: expected '//' or '/*'"


def test_enum_value_leading_and_trailing_comments_are_combined():
    proto = """
      |enum Test {
      |  // Test all...
      |  FOO = 1; // ...the things!
      |}
      """
    proto = trim_margin(proto)
    parsed = ProtoParser.parse(location, proto)
    enum_element: EnumElement = parsed.types[0]
    value = enum_element.constants[0]
    assert value.documentation == "Test all...\n...the things!"


def test_trailing_comment_not_combined_when_empty():
    """ (Kotlin) Can't use raw strings here; otherwise, the formatter removes the trailing whitespace on line 3. """
    proto = "enum Test {\n" \
            "  // Test all...\n" \
            "  FOO = 1; //       \n" \
            "}"
    parsed = ProtoParser.parse(location, proto)
    enum_element: EnumElement = parsed.types[0]
    value = enum_element.constants[0]
    assert value.documentation == "Test all..."


def test_syntax_not_required():
    proto = "message Foo {}"
    parsed = ProtoParser.parse(location, proto)
    assert parsed.syntax is None


def test_syntax_specified():
    proto = """
      |syntax = "proto3";
      |message Foo {}
      """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location, syntax=Syntax.PROTO_3, types=[MessageElement(location=location.at(2, 1), name="Foo")]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_invalid_syntax_value_throws():
    proto = """
      |syntax = "proto4";
      |message Foo {}
      """
    proto = trim_margin(proto)
    with pytest.raises(IllegalStateException, match="Syntax error in file.proto:1:1: unexpected syntax: proto4"):
        ProtoParser.parse(location, proto)
        pytest.fail("")


def test_syntax_not_first_declaration_throws():
    proto = """
      |message Foo {}
      |syntax = "proto3";
      """
    proto = trim_margin(proto)
    with pytest.raises(
        IllegalStateException,
        match="Syntax error in file.proto:2:1: 'syntax' element must be the first declaration "
        "in a file"
    ):
        ProtoParser.parse(location, proto)
        pytest.fail("")


def test_syntax_may_follow_comments_and_empty_lines():
    proto = """
      |/* comment 1 */
      |// comment 2
      |
      |syntax = "proto3";
      |message Foo {}
      """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location, syntax=Syntax.PROTO_3, types=[MessageElement(location=location.at(5, 1), name="Foo")]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto3_message_fields_do_not_require_labels():
    proto = """
      |syntax = "proto3";
      |message Message {
      |  string a = 1;
      |  int32 b = 2;
      |}
      """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        syntax=Syntax.PROTO_3,
        types=[
            MessageElement(
                location=location.at(2, 1),
                name="Message",
                fields=[
                    FieldElement(location=location.at(3, 3), element_type="string", name="a", tag=1),
                    FieldElement(location=location.at(4, 3), element_type="int32", name="b", tag=2)
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto3_extension_fields_do_not_require_labels():
    proto = """
      |syntax = "proto3";
      |message Message {
      |}
      |extend Message {
      |  string a = 1;
      |  int32 b = 2;
      |}
      """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        syntax=Syntax.PROTO_3,
        types=[MessageElement(location=location.at(2, 1), name="Message")],
        extend_declarations=[
            ExtendElement(
                location=location.at(4, 1),
                name="Message",
                fields=[
                    FieldElement(location=location.at(5, 3), element_type="string", name="a", tag=1),
                    FieldElement(location=location.at(6, 3), element_type="int32", name="b", tag=2)
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto3_message_fields_allow_optional():
    proto = """
      |syntax = "proto3";
      |message Message {
      |  optional string a = 1;
      |}
      """
    proto = trim_margin(proto)

    expected = ProtoFileElement(
        location=location,
        syntax=Syntax.PROTO_3,
        types=[
            MessageElement(
                location=location.at(2, 1),
                name="Message",
                fields=[
                    FieldElement(
                        location=location.at(3, 3), element_type="string", name="a", tag=1, label=Field.Label.OPTIONAL
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto3_message_fields_forbid_required():
    proto = """
        |syntax = "proto3";
        |message Message {
        |  required string a = 1;
        |}
        """
    proto = trim_margin(proto)
    with pytest.raises(
        IllegalStateException,
        match="Syntax error in file.proto:3:3: 'required' label forbidden in proto3 field "
        "declarations"
    ):
        ProtoParser.parse(location, proto)
        pytest.fail("")


def test_proto3_extension_fields_allow_optional():
    proto = """
        |syntax = "proto3";
        |message Message {
        |}
        |extend Message {
        |  optional string a = 1;
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        syntax=Syntax.PROTO_3,
        types=[MessageElement(location=location.at(2, 1), name="Message")],
        extend_declarations=[
            ExtendElement(
                location=location.at(4, 1),
                name="Message",
                fields=[
                    FieldElement(
                        location=location.at(5, 3), element_type="string", name="a", tag=1, label=Field.Label.OPTIONAL
                    )
                ],
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto3_extension_fields_forbids_required():
    proto = """
        |syntax = "proto3";
        |message Message {
        |}
        |extend Message {
        |  required string a = 1;
        |}
        """
    proto = trim_margin(proto)
    with pytest.raises(
        IllegalStateException,
        match="Syntax error in file.proto:5:3: 'required' label forbidden in proto3 field "
        "declarations"
    ):
        ProtoParser.parse(location, proto)
        pytest.fail("")


def test_proto3_message_fields_permit_repeated():
    proto = """
        |syntax = "proto3";
        |message Message {
        |  repeated string a = 1;
        |}
        """
    proto = trim_margin(proto)

    expected = ProtoFileElement(
        location=location,
        syntax=Syntax.PROTO_3,
        types=[
            MessageElement(
                location=location.at(2, 1),
                name="Message",
                fields=[
                    FieldElement(
                        location=location.at(3, 3), label=Field.Label.REPEATED, element_type="string", name="a", tag=1
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto3_extension_fields_permit_repeated():
    proto = """
        |syntax = "proto3";
        |message Message {
        |}
        |extend Message {
        |  repeated string a = 1;
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        syntax=Syntax.PROTO_3,
        types=[MessageElement(location=location.at(2, 1), name="Message")],
        extend_declarations=[
            ExtendElement(
                location=location.at(4, 1),
                name="Message",
                fields=[
                    FieldElement(
                        location=location.at(5, 3), label=Field.Label.REPEATED, element_type="string", name="a", tag=1
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_parse_message_and_fields():
    proto = """
        |message SearchRequest {
        |  required string query = 1;
        |  optional int32 page_number = 2;
        |  optional int32 result_per_page = 3;
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="SearchRequest",
                fields=[
                    FieldElement(
                        location=location.at(2, 3), label=Field.Label.REQUIRED, element_type="string", name="query", tag=1
                    ),
                    FieldElement(
                        location=location.at(3, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="int32",
                        name="page_number",
                        tag=2
                    ),
                    FieldElement(
                        location=location.at(4, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="int32",
                        name="result_per_page",
                        tag=3
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_group():
    proto = """
        |message SearchResponse {
        |  repeated group Result = 1 {
        |    required string url = 2;
        |    optional string title = 3;
        |    repeated string snippets = 4;
        |  }
        |}
        """
    proto = trim_margin(proto)
    message = MessageElement(
        location=location.at(1, 1),
        name="SearchResponse",
        groups=[
            GroupElement(
                location=location.at(2, 3),
                label=Field.Label.REPEATED,
                name="Result",
                tag=1,
                fields=[
                    FieldElement(
                        location=location.at(3, 5), label=Field.Label.REQUIRED, element_type="string", name="url", tag=2
                    ),
                    FieldElement(
                        location=location.at(4, 5), label=Field.Label.OPTIONAL, element_type="string", name="title", tag=3
                    ),
                    FieldElement(
                        location=location.at(5, 5),
                        label=Field.Label.REPEATED,
                        element_type="string",
                        name="snippets",
                        tag=4
                    )
                ]
            )
        ]
    )
    expected = ProtoFileElement(location=location, types=[message])
    assert ProtoParser.parse(location, proto) == expected


def test_parse_message_and_one_of():
    proto = """
        |message SearchRequest {
        |  required string query = 1;
        |  oneof page_info {
        |    int32 page_number = 2;
        |    int32 result_per_page = 3;
        |  }
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="SearchRequest",
                fields=[
                    FieldElement(
                        location=location.at(2, 3), label=Field.Label.REQUIRED, element_type="string", name="query", tag=1
                    )
                ],
                one_ofs=[
                    OneOfElement(
                        name="page_info",
                        fields=[
                            FieldElement(location=location.at(4, 5), element_type="int32", name="page_number", tag=2),
                            FieldElement(location=location.at(5, 5), element_type="int32", name="result_per_page", tag=3)
                        ],
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_parse_message_and_one_of_with_group():
    proto = """
        |message SearchRequest {
        |  required string query = 1;
        |  oneof page_info {
        |    int32 page_number = 2;
        |    group Stuff = 3 {
        |      optional int32 result_per_page = 4;
        |      optional int32 page_count = 5;
        |    }
        |  }
        |}
    """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="SearchRequest",
                fields=[
                    FieldElement(
                        location=location.at(2, 3), label=Field.Label.REQUIRED, element_type="string", name="query", tag=1
                    )
                ],
                one_ofs=[
                    OneOfElement(
                        name="page_info",
                        fields=[FieldElement(location=location.at(4, 5), element_type="int32", name="page_number", tag=2)],
                        groups=[
                            GroupElement(
                                label=None,
                                location=location.at(5, 5),
                                name="Stuff",
                                tag=3,
                                fields=[
                                    FieldElement(
                                        location=location.at(6, 7),
                                        label=Field.Label.OPTIONAL,
                                        element_type="int32",
                                        name="result_per_page",
                                        tag=4
                                    ),
                                    FieldElement(
                                        location=location.at(7, 7),
                                        label=Field.Label.OPTIONAL,
                                        element_type="int32",
                                        name="page_count",
                                        tag=5
                                    )
                                ]
                            )
                        ],
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_parse_enum():
    proto = """
        |/**
        | * What's on my waffles.
        | * Also works on pancakes.
        | */
        |enum Topping {
        |  FRUIT = 1;
        |  /** Yummy, yummy cream. */
        |  CREAM = 2;
        |
        |  // Quebec Maple syrup
        |  SYRUP = 3;
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            EnumElement(
                location=location.at(5, 1),
                name="Topping",
                documentation="What's on my waffles.\nAlso works on pancakes.",
                constants=[
                    EnumConstantElement(location=location.at(6, 3), name="FRUIT", tag=1),
                    EnumConstantElement(
                        location=location.at(8, 3),
                        name="CREAM",
                        tag=2,
                        documentation="Yummy, yummy cream.",
                    ),
                    EnumConstantElement(
                        location=location.at(11, 3),
                        name="SYRUP",
                        tag=3,
                        documentation="Quebec Maple syrup",
                    )
                ],
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_parse_enum_with_options():
    proto = """
        |/**
        | * What's on my waffles.
        | * Also works on pancakes.
        | */
        |enum Topping {
        |  option(max_choices) = 2;
        |
        |  FRUIT = 1[(healthy) = true];
        |  /** Yummy, yummy cream. */
        |  CREAM = 2;
        |
        |  // Quebec Maple syrup
        |  SYRUP = 3;
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            EnumElement(
                location=location.at(5, 1),
                name="Topping",
                documentation="What's on my waffles.\nAlso works on pancakes.",
                options=[OptionElement("max_choices", OptionElement.Kind.NUMBER, "2", True)],
                constants=[
                    EnumConstantElement(
                        location=location.at(8, 3),
                        name="FRUIT",
                        tag=1,
                        options=[OptionElement("healthy", OptionElement.Kind.BOOLEAN, "true", True)]
                    ),
                    EnumConstantElement(
                        location=location.at(10, 3),
                        name="CREAM",
                        tag=2,
                        documentation="Yummy, yummy cream.",
                    ),
                    EnumConstantElement(
                        location=location.at(13, 3),
                        name="SYRUP",
                        tag=3,
                        documentation="Quebec Maple syrup",
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_package_declaration():
    proto = """
      |package google.protobuf;
      |option java_package = "com.google.protobuf";
      |
      |// The protocol compiler can output a FileDescriptorSet containing the .proto
      |// files it parses.
      |message FileDescriptorSet {
      |}
      """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        package_name="google.protobuf",
        types=[
            MessageElement(
                location=location.at(6, 1),
                name="FileDescriptorSet",
                documentation="The protocol compiler can output a FileDescriptorSet containing the .proto\nfiles "
                "it parses."
            )
        ],
        options=[OptionElement("java_package", OptionElement.Kind.STRING, "com.google.protobuf")]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_nesting_in_message():
    proto = """
      |message FieldOptions {
      |  optional CType ctype = 1[old_default = STRING, deprecated = true];
      |  enum CType {
      |    STRING = 0[(opt_a) = 1, (opt_b) = 2];
      |  };
      |  // Clients can define custom options in extensions of this message. See above.
      |  extensions 500;
      |  extensions 1000 to max;
      |}
      """
    proto = trim_margin(proto)
    enum_element = EnumElement(
        location=location.at(3, 3),
        name="CType",
        constants=[
            EnumConstantElement(
                location=location.at(4, 5),
                name="STRING",
                tag=0,
                options=[
                    OptionElement("opt_a", OptionElement.Kind.NUMBER, "1", True),
                    OptionElement("opt_b", OptionElement.Kind.NUMBER, "2", True)
                ]
            )
        ],
    )
    field = FieldElement(
        location=location.at(2, 3),
        label=Field.Label.OPTIONAL,
        element_type="CType",
        name="ctype",
        tag=1,
        options=[
            OptionElement("old_default", OptionElement.Kind.ENUM, "STRING"),
            OptionElement("deprecated", OptionElement.Kind.BOOLEAN, "true")
        ]
    )

    assert len(field.options) == 2
    assert OptionElement("old_default", OptionElement.Kind.ENUM, "STRING") in field.options
    assert OptionElement("deprecated", OptionElement.Kind.BOOLEAN, "true") in field.options

    message_element = MessageElement(
        location=location.at(1, 1),
        name="FieldOptions",
        fields=[field],
        nested_types=[enum_element],
        extensions=[
            ExtensionsElement(
                location=location.at(7, 3),
                documentation="Clients can define custom options in extensions of this message. See above.",
                values=[500]
            ),
            ExtensionsElement(location.at(8, 3), "", [KotlinRange(1000, MAX_TAG_VALUE)])
        ]
    )
    expected = ProtoFileElement(location=location, types=[message_element])
    actual = ProtoParser.parse(location, proto)
    assert actual == expected


def test_multi_ranges_extensions():
    proto = """
      |message MeGustaExtensions {
      |  extensions 1, 5 to 200, 500, 1000 to max;
      |}
      """
    proto = trim_margin(proto)
    message_element = MessageElement(
        location=location.at(1, 1),
        name="MeGustaExtensions",
        extensions=[
            ExtensionsElement(
                location=location.at(2, 3), values=[1] + [KotlinRange(5, 200)] + [500] + [KotlinRange(1000, MAX_TAG_VALUE)]
            )
        ]
    )
    expected = ProtoFileElement(location=location, types=[message_element])
    actual = ProtoParser.parse(location, proto)
    assert actual == expected


def test_option_parentheses():
    proto = """
      |message Chickens {
      |  optional bool koka_ko_koka_ko = 1[old_default = true];
      |  optional bool coodle_doodle_do = 2[(delay) = 100, old_default = false];
      |  optional bool coo_coo_ca_cha = 3[old_default = true, (delay) = 200];
      |  optional bool cha_chee_cha = 4;
      |}
      """
    proto = trim_margin(proto)

    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="Chickens",
                fields=[
                    FieldElement(
                        location=location.at(2, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="bool",
                        name="koka_ko_koka_ko",
                        tag=1,
                        options=[OptionElement("old_default", OptionElement.Kind.BOOLEAN, "true")]
                    ),
                    FieldElement(
                        location=location.at(3, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="bool",
                        name="coodle_doodle_do",
                        tag=2,
                        options=[
                            OptionElement("delay", OptionElement.Kind.NUMBER, "100", True),
                            OptionElement("old_default", OptionElement.Kind.BOOLEAN, "false")
                        ]
                    ),
                    FieldElement(
                        location=location.at(4, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="bool",
                        name="coo_coo_ca_cha",
                        tag=3,
                        options=[
                            OptionElement("old_default", OptionElement.Kind.BOOLEAN, "true"),
                            OptionElement("delay", OptionElement.Kind.NUMBER, "200", True)
                        ]
                    ),
                    FieldElement(
                        location=location.at(5, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="bool",
                        name="cha_chee_cha",
                        tag=4
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_imports():
    proto = "import \"src/test/resources/unittest_import.proto\";\n"
    expected = ProtoFileElement(location=location, imports=["src/test/resources/unittest_import.proto"])
    assert ProtoParser.parse(location, proto) == expected


def test_public_imports():
    proto = "import public \"src/test/resources/unittest_import.proto\";\n"
    expected = ProtoFileElement(location=location, public_imports=["src/test/resources/unittest_import.proto"])
    assert ProtoParser.parse(location, proto) == expected


def test_extend():
    proto = """
        |// Extends Foo
        |extend Foo {
        |  optional int32 bar = 126;
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        extend_declarations=[
            ExtendElement(
                location=location.at(2, 1),
                name="Foo",
                documentation="Extends Foo",
                fields=[
                    FieldElement(
                        location=location.at(3, 3), label=Field.Label.OPTIONAL, element_type="int32", name="bar", tag=126
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_extend_in_message():
    proto = """
       |message Bar {
       |  extend Foo {
       |    optional Bar bar = 126;
       |  }
       |}
       """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[MessageElement(location=location.at(1, 1), name="Bar")],
        extend_declarations=[
            ExtendElement(
                location=location.at(2, 3),
                name="Foo",
                fields=[
                    FieldElement(
                        location=location.at(3, 5), label=Field.Label.OPTIONAL, element_type="Bar", name="bar", tag=126
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_extend_in_message_with_package():
    proto = """
        |package kit.kat;
        |
        |message Bar {
        |  extend Foo {
        |    optional Bar bar = 126;
        |  }
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        package_name="kit.kat",
        types=[MessageElement(location=location.at(3, 1), name="Bar")],
        extend_declarations=[
            ExtendElement(
                location=location.at(4, 3),
                name="Foo",
                fields=[
                    FieldElement(
                        location=location.at(5, 5), label=Field.Label.OPTIONAL, element_type="Bar", name="bar", tag=126
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_fqcn_extend_in_message():
    proto = """
        |message Bar {
        |  extend example.Foo {
        |    optional Bar bar = 126;
        |  }
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[MessageElement(location=location.at(1, 1), name="Bar")],
        extend_declarations=[
            ExtendElement(
                location=location.at(2, 3),
                name="example.Foo",
                fields=[
                    FieldElement(
                        location=location.at(3, 5), label=Field.Label.OPTIONAL, element_type="Bar", name="bar", tag=126
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_fqcn_extend_in_message_with_package():
    proto = """
        |package kit.kat;
        |
        |message Bar {
        |  extend example.Foo {
        |    optional Bar bar = 126;
        |  }
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        package_name="kit.kat",
        types=[MessageElement(location=location.at(3, 1), name="Bar")],
        extend_declarations=[
            ExtendElement(
                location=location.at(4, 3),
                name="example.Foo",
                fields=[
                    FieldElement(
                        location=location.at(5, 5), label=Field.Label.OPTIONAL, element_type="Bar", name="bar", tag=126
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_default_field_with_paren():
    proto = """
        |message Foo {
        |  optional string claim_token = 2[(squareup.redacted) = true];
        |}
        """
    proto = trim_margin(proto)
    field = FieldElement(
        location=location.at(2, 3),
        label=Field.Label.OPTIONAL,
        element_type="string",
        name="claim_token",
        tag=2,
        options=[OptionElement("squareup.redacted", OptionElement.Kind.BOOLEAN, "true", True)]
    )
    assert len(field.options) == 1
    assert OptionElement("squareup.redacted", OptionElement.Kind.BOOLEAN, "true", True) in field.options

    message_element = MessageElement(location=location.at(1, 1), name="Foo", fields=[field])
    expected = ProtoFileElement(location=location, types=[message_element])
    assert ProtoParser.parse(location, proto) == expected


# Parse \a, \b, \f, \n, \r, \t, \v, \[0-7]{1-3}, and \[xX]{0-9a-fA-F]{1,2}
def test_default_field_with_string_escapes():
    proto = r"""
        |message Foo {
        |  optional string name = 1 [
        |    x = "\a\b\f\n\r\t\v\1f\01\001\11\011\111\xe\Xe\xE\xE\x41\x41"
        |  ];
        |}
        """
    proto = trim_margin(proto)
    field = FieldElement(
        location=location.at(2, 3),
        label=Field.Label.OPTIONAL,
        element_type="string",
        name="name",
        tag=1,
        options=[
            OptionElement(
                "x", OptionElement.Kind.STRING,
                "\u0007\b\u000C\n\r\t\u000b\u0001f\u0001\u0001\u0009\u0009I\u000e\u000e\u000e\u000eAA"
            )
        ]
    )
    assert len(field.options) == 1
    assert OptionElement(
        "x", OptionElement.Kind.STRING,
        "\u0007\b\u000C\n\r\t\u000b\u0001f\u0001\u0001\u0009\u0009I\u000e\u000e\u000e\u000eAA"
    ) in field.options

    message_element = MessageElement(location=location.at(1, 1), name="Foo", fields=[field])
    expected = ProtoFileElement(location=location, types=[message_element])
    assert ProtoParser.parse(location, proto) == expected


def test_string_with_single_quotes():
    proto = r"""
        |message Foo {
        |  optional string name = 1[default = 'single\"quotes'];
        |}
        """
    proto = trim_margin(proto)

    field = FieldElement(
        location=location.at(2, 3),
        label=Field.Label.OPTIONAL,
        element_type="string",
        name="name",
        tag=1,
        default_value="single\"quotes"
    )
    message_element = MessageElement(location=location.at(1, 1), name="Foo", fields=[field])
    expected = ProtoFileElement(location=location, types=[message_element])
    assert ProtoParser.parse(location, proto) == expected


def test_adjacent_strings_concatenated():
    proto = """
        |message Foo {
        |  optional string name = 1 [
        |    default = "concat "
        |              'these '
        |              "please"
        |  ];
        |}
        """
    proto = trim_margin(proto)

    field = FieldElement(
        location=location.at(2, 3),
        label=Field.Label.OPTIONAL,
        element_type="string",
        name="name",
        tag=1,
        default_value="concat these please"
    )
    message_element = MessageElement(location=location.at(1, 1), name="Foo", fields=[field])
    expected = ProtoFileElement(location=location, types=[message_element])
    assert ProtoParser.parse(location, proto) == expected


def test_invalid_hex_string_escape():
    proto = r"""
        |message Foo {
        |  optional string name = 1 [default = "\xW"];
        |}
        """
    proto = trim_margin(proto)
    with pytest.raises(IllegalStateException) as re:
        ProtoParser.parse(location, proto)
        pytest.fail("")
    assert "expected a digit after \\x or \\X" in re.value.message


def test_service():
    proto = """
        |service SearchService {
        |  option (default_timeout) = 30;
        |
        |  rpc Search (SearchRequest) returns (SearchResponse);
        |  rpc Purchase (PurchaseRequest) returns (PurchaseResponse) {
        |    option (squareup.sake.timeout) = 15;
        |    option (squareup.a.b) = {
        |      value: [
        |        FOO,
        |        BAR
        |      ]
        |    };
        |  }
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        services=[
            ServiceElement(
                location=location.at(1, 1),
                name="SearchService",
                options=[OptionElement("default_timeout", OptionElement.Kind.NUMBER, "30", True)],
                rpcs=[
                    RpcElement(
                        location=location.at(4, 3),
                        name="Search",
                        request_type="SearchRequest",
                        response_type="SearchResponse",
                        response_streaming=False,
                        request_streaming=False
                    ),
                    RpcElement(
                        location=location.at(5, 3),
                        name="Purchase",
                        request_type="PurchaseRequest",
                        response_type="PurchaseResponse",
                        options=[
                            OptionElement("squareup.sake.timeout", OptionElement.Kind.NUMBER, "15", True),
                            OptionElement("squareup.a.b", OptionElement.Kind.MAP, {"value": ["FOO", "BAR"]}, True)
                        ],
                        request_streaming=False,
                        response_streaming=False
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_streaming_service():
    proto = """
        |service RouteGuide {
        |  rpc GetFeature (Point) returns (Feature) {}
        |  rpc ListFeatures (Rectangle) returns (stream Feature) {}
        |  rpc RecordRoute (stream Point) returns (RouteSummary) {}
        |  rpc RouteChat (stream RouteNote) returns (stream RouteNote) {}
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        services=[
            ServiceElement(
                location=location.at(1, 1),
                name="RouteGuide",
                rpcs=[
                    RpcElement(
                        location=location.at(2, 3),
                        name="GetFeature",
                        request_type="Point",
                        response_type="Feature",
                        response_streaming=False,
                        request_streaming=False
                    ),
                    RpcElement(
                        location=location.at(3, 3),
                        name="ListFeatures",
                        request_type="Rectangle",
                        response_type="Feature",
                        response_streaming=True,
                        # TODO: Report Square.Wire there was mistake True instead of False!
                        request_streaming=False,
                    ),
                    RpcElement(
                        location=location.at(4, 3),
                        name="RecordRoute",
                        request_type="Point",
                        response_type="RouteSummary",
                        request_streaming=True,
                        response_streaming=False,
                    ),
                    RpcElement(
                        location=location.at(5, 3),
                        name="RouteChat",
                        request_type="RouteNote",
                        response_type="RouteNote",
                        request_streaming=True,
                        response_streaming=True,
                    )
                ],
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_hex_tag():
    proto = """
        |message HexTag {
        |  required string hex = 0x10;
        |  required string uppercase_x_hex = 0X11;
        |}
        """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="HexTag",
                fields=[
                    FieldElement(
                        location=location.at(2, 3), label=Field.Label.REQUIRED, element_type="string", name="hex", tag=16
                    ),
                    FieldElement(
                        location=location.at(3, 3),
                        label=Field.Label.REQUIRED,
                        element_type="string",
                        name="uppercase_x_hex",
                        tag=17
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_structured_option():
    proto = """
            |message ExoticOptions {
        |  option (squareup.one) = {name: "Name", class_name:"ClassName"};
        |  option (squareup.two.a) = {[squareup.options.type]: EXOTIC};
        |  option (squareup.two.b) = {names: ["Foo", "Bar"]};
        |}
        """
    # TODO: we do not support it yet
    #
    #    |  option (squareup.three) = {x: {y: 1 y: 2 } }; // NOTE: Omitted optional comma
    #    |  option (squareup.four) = {x: {y: {z: 1 }, y: {z: 2 }}};
    #
    #
    #
    proto = trim_margin(proto)

    option_one_map = {"name": "Name", "class_name": "ClassName"}

    option_two_a_map = {"[squareup.options.type]": "EXOTIC"}

    option_two_b_map = {"names": ["Foo", "Bar"]}

    # TODO: we do not support it yet
    #       need create custom dictionary class to support multiple values for one key
    #
    # option_three_map = {"x": {"y": 1, "y": 2}}
    # option_four_map = {"x": ["y": {"z": 1}, "y": {"z": 2}]}

    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="ExoticOptions",
                options=[
                    OptionElement("squareup.one", OptionElement.Kind.MAP, option_one_map, True),
                    OptionElement("squareup.two.a", OptionElement.Kind.MAP, option_two_a_map, True),
                    OptionElement("squareup.two.b", OptionElement.Kind.MAP, option_two_b_map, True),
                    # OptionElement("squareup.three", OptionElement.Kind.MAP, option_three_map, True),
                    # OptionElement("squareup.four", OptionElement.Kind.MAP, option_four_map, True)
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_options_with_nested_maps_and_trailing_commas():
    proto = """
        |message StructuredOption {
        |    optional field.type has_options = 3 [
        |            (option_map) = {
        |                nested_map: {key:"value", key2:["value2a","value2b"]},
        |             },
        |            (option_string) = ["string1","string2"]
        |    ];
        |}
        """
    proto = trim_margin(proto)
    field = FieldElement(
        location=location.at(2, 5),
        label=Field.Label.OPTIONAL,
        element_type="field.type",
        name="has_options",
        tag=3,
        options=[
            OptionElement(
                "option_map", OptionElement.Kind.MAP, {"nested_map": {
                    "key": "value",
                    "key2": ["value2a", "value2b"]
                }}, True
            ),
            OptionElement("option_string", OptionElement.Kind.LIST, ["string1", "string2"], True)
        ]
    )
    assert len(field.options) == 2
    assert OptionElement(
        "option_map", OptionElement.Kind.MAP, {"nested_map": {
            "key": "value",
            "key2": ["value2a", "value2b"]
        }}, True
    ) in field.options
    assert OptionElement("option_string", OptionElement.Kind.LIST, ["string1", "string2"], True) in field.options

    expected = MessageElement(location=location.at(1, 1), name="StructuredOption", fields=[field])
    proto_file = ProtoFileElement(location=location, types=[expected])
    assert ProtoParser.parse(location, proto) == proto_file


def test_option_numerical_bounds():
    proto = r"""
            |message Test {
            |  optional int32 default_int32 = 401 [x = 2147483647];
            |  optional uint32 default_uint32 = 402 [x = 4294967295];
            |  optional sint32 default_sint32 = 403 [x = -2147483648];
            |  optional fixed32 default_fixed32 = 404 [x = 4294967295];
            |  optional sfixed32 default_sfixed32 = 405 [x = -2147483648];
            |  optional int64 default_int64 = 406 [x = 9223372036854775807];
            |  optional uint64 default_uint64 = 407 [x = 18446744073709551615];
            |  optional sint64 default_sint64 = 408 [x = -9223372036854775808];
            |  optional fixed64 default_fixed64 = 409 [x = 18446744073709551615];
            |  optional sfixed64 default_sfixed64 = 410 [x = -9223372036854775808];
            |  optional bool default_bool = 411 [x = true];
            |  optional float default_float = 412 [x = 123.456e7];
            |  optional double default_double = 413 [x = 123.456e78];
            |  optional string default_string = 414 """ + \
            r"""[x = "çok\a\b\f\n\r\t\v\1\01\001\17\017\176\x1\x01\x11\X1\X01\X11güzel" ];
            |  optional bytes default_bytes = 415 """ + \
            r"""[x = "çok\a\b\f\n\r\t\v\1\01\001\17\017\176\x1\x01\x11\X1\X01\X11güzel" ];
            |  optional NestedEnum default_nested_enum = 416 [x = A ];
            |}"""
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="Test",
                fields=[
                    FieldElement(
                        location=location.at(2, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="int32",
                        name="default_int32",
                        tag=401,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "2147483647")]
                    ),
                    FieldElement(
                        location=location.at(3, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="uint32",
                        name="default_uint32",
                        tag=402,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "4294967295")]
                    ),
                    FieldElement(
                        location=location.at(4, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="sint32",
                        name="default_sint32",
                        tag=403,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "-2147483648")]
                    ),
                    FieldElement(
                        location=location.at(5, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="fixed32",
                        name="default_fixed32",
                        tag=404,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "4294967295")]
                    ),
                    FieldElement(
                        location=location.at(6, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="sfixed32",
                        name="default_sfixed32",
                        tag=405,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "-2147483648")]
                    ),
                    FieldElement(
                        location=location.at(7, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="int64",
                        name="default_int64",
                        tag=406,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "9223372036854775807")]
                    ),
                    FieldElement(
                        location=location.at(8, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="uint64",
                        name="default_uint64",
                        tag=407,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "18446744073709551615")]
                    ),
                    FieldElement(
                        location=location.at(9, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="sint64",
                        name="default_sint64",
                        tag=408,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "-9223372036854775808")]
                    ),
                    FieldElement(
                        location=location.at(10, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="fixed64",
                        name="default_fixed64",
                        tag=409,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "18446744073709551615")]
                    ),
                    FieldElement(
                        location=location.at(11, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="sfixed64",
                        name="default_sfixed64",
                        tag=410,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "-9223372036854775808")]
                    ),
                    FieldElement(
                        location=location.at(12, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="bool",
                        name="default_bool",
                        tag=411,
                        options=[OptionElement("x", OptionElement.Kind.BOOLEAN, "true")]
                    ),
                    FieldElement(
                        location=location.at(13, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="float",
                        name="default_float",
                        tag=412,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "123.456e7")]
                    ),
                    FieldElement(
                        location=location.at(14, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="double",
                        name="default_double",
                        tag=413,
                        options=[OptionElement("x", OptionElement.Kind.NUMBER, "123.456e78")]
                    ),
                    FieldElement(
                        location=location.at(15, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="string",
                        name="default_string",
                        tag=414,
                        options=[
                            OptionElement(
                                "x", OptionElement.Kind.STRING,
                                "çok\u0007\b\u000C\n\r\t\u000b\u0001\u0001\u0001\u000f\u000f~\u0001\u0001\u0011"
                                "\u0001\u0001\u0011güzel"
                            )
                        ]
                    ),
                    FieldElement(
                        location=location.at(17, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="bytes",
                        name="default_bytes",
                        tag=415,
                        options=[
                            OptionElement(
                                "x", OptionElement.Kind.STRING,
                                "çok\u0007\b\u000C\n\r\t\u000b\u0001\u0001\u0001\u000f\u000f~\u0001\u0001\u0011"
                                "\u0001\u0001\u0011güzel"
                            )
                        ]
                    ),
                    FieldElement(
                        location=location.at(19, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="NestedEnum",
                        name="default_nested_enum",
                        tag=416,
                        options=[OptionElement("x", OptionElement.Kind.ENUM, "A")]
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_extension_with_nested_message():
    proto = """
        |message Foo {
        |  optional int32 bar = 1[
        |      (validation.range).min = 1,
        |      (validation.range).max = 100,
        |      old_default = 20
        |  ];
        |}
        """
    proto = trim_margin(proto)
    field = FieldElement(
        location=location.at(2, 3),
        label=Field.Label.OPTIONAL,
        element_type="int32",
        name="bar",
        tag=1,
        options=[
            OptionElement(
                "validation.range", OptionElement.Kind.OPTION, OptionElement("min", OptionElement.Kind.NUMBER, "1"), True
            ),
            OptionElement(
                "validation.range", OptionElement.Kind.OPTION, OptionElement("max", OptionElement.Kind.NUMBER, "100"), True
            ),
            OptionElement("old_default", OptionElement.Kind.NUMBER, "20")
        ]
    )
    assert len(field.options) == 3
    assert OptionElement(
        "validation.range", OptionElement.Kind.OPTION, OptionElement("min", OptionElement.Kind.NUMBER, "1"), True
    ) in field.options

    assert OptionElement(
        "validation.range", OptionElement.Kind.OPTION, OptionElement("max", OptionElement.Kind.NUMBER, "100"), True
    ) in field.options

    assert OptionElement("old_default", OptionElement.Kind.NUMBER, "20") in field.options

    expected = MessageElement(location=location.at(1, 1), name="Foo", fields=[field])
    proto_file = ProtoFileElement(location=location, types=[expected])
    assert ProtoParser.parse(location, proto) == proto_file


def test_reserved():
    proto = """
        |message Foo {
        |  reserved 10, 12 to 14, 'foo';
        |}
        """
    proto = trim_margin(proto)
    message = MessageElement(
        location=location.at(1, 1),
        name="Foo",
        reserveds=[ReservedElement(location=location.at(2, 3), values=[10, KotlinRange(12, 14), "foo"])]
    )
    expected = ProtoFileElement(location=location, types=[message])
    assert ProtoParser.parse(location, proto) == expected


def test_reserved_with_comments():
    proto = """
        |message Foo {
        |  optional string a = 1; // This is A.
        |  reserved 2; // This is reserved.
        |  optional string c = 3; // This is C.
        |}
        """
    proto = trim_margin(proto)
    message = MessageElement(
        location=location.at(1, 1),
        name="Foo",
        fields=[
            FieldElement(
                location=location.at(2, 3),
                label=Field.Label.OPTIONAL,
                element_type="string",
                name="a",
                tag=1,
                documentation="This is A."
            ),
            FieldElement(
                location=location.at(4, 3),
                label=Field.Label.OPTIONAL,
                element_type="string",
                name="c",
                tag=3,
                documentation="This is C."
            )
        ],
        reserveds=[ReservedElement(location=location.at(3, 3), values=[2], documentation="This is reserved.")]
    )
    expected = ProtoFileElement(location=location, types=[message])
    assert ProtoParser.parse(location, proto) == expected


def test_no_whitespace():
    proto = "message C {optional A.B ab = 1;}"
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="C",
                fields=[
                    FieldElement(
                        location=location.at(1, 12), label=Field.Label.OPTIONAL, element_type="A.B", name="ab", tag=1
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_deep_option_assignments():
    proto = """
      |message Foo {
      |  optional string a = 1 [(wire.my_field_option).baz.value = "a"];
      |}
      |"""
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="Foo",
                fields=[
                    FieldElement(
                        location=location.at(2, 3),
                        label=Field.Label.OPTIONAL,
                        element_type="string",
                        name="a",
                        tag=1,
                        options=[
                            OptionElement(
                                name="wire.my_field_option",
                                kind=OptionElement.Kind.OPTION,
                                is_parenthesized=True,
                                value=OptionElement(
                                    name="baz",
                                    kind=OptionElement.Kind.OPTION,
                                    is_parenthesized=False,
                                    value=OptionElement(
                                        name="value", kind=OptionElement.Kind.STRING, is_parenthesized=False, value="a"
                                    )
                                )
                            )
                        ]
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto_keyword_as_enum_constants():
    # Note: this is consistent with protoc.
    proto = """
      |enum Foo {
      |  syntax = 0;
      |  import = 1;
      |  package = 2;
      |  // option = 3;
      |  // reserved = 4;
      |  message = 5;
      |  enum = 6;
      |  service = 7;
      |  extend = 8;
      |  rpc = 9;
      |  oneof = 10;
      |  extensions = 11;
      |}
      |"""
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            EnumElement(
                location=location.at(1, 1),
                name="Foo",
                constants=[
                    EnumConstantElement(location.at(2, 3), "syntax", 0),
                    EnumConstantElement(location.at(3, 3), "import", 1),
                    EnumConstantElement(location.at(4, 3), "package", 2),
                    EnumConstantElement(location.at(7, 3), "message", 5, documentation="option = 3;\nreserved = 4;"),
                    EnumConstantElement(location.at(8, 3), "enum", 6),
                    EnumConstantElement(location.at(9, 3), "service", 7),
                    EnumConstantElement(location.at(10, 3), "extend", 8),
                    EnumConstantElement(location.at(11, 3), "rpc", 9),
                    EnumConstantElement(location.at(12, 3), "oneof", 10),
                    EnumConstantElement(location.at(13, 3), "extensions", 11),
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto_keyword_as_message_name_and_field_proto2():
    # Note: this is consistent with protoc.
    proto = """
      |message syntax {
      |  optional syntax syntax = 1;
      |}
      |message import {
      |  optional import import = 1;
      |}
      |message package {
      |  optional package package = 1;
      |}
      |message option {
      |  optional option option = 1;
      |}
      |message reserved {
      |  optional reserved reserved = 1;
      |}
      |message message {
      |  optional message message = 1;
      |}
      |message enum {
      |  optional enum enum = 1;
      |}
      |message service {
      |  optional service service = 1;
      |}
      |message extend {
      |  optional extend extend = 1;
      |}
      |message rpc {
      |  optional rpc rpc = 1;
      |}
      |message oneof {
      |  optional oneof oneof = 1;
      |}
      |message extensions {
      |  optional extensions extensions = 1;
      |}
      |"""
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="syntax",
                fields=[
                    FieldElement(location.at(2, 3), label=Field.Label.OPTIONAL, element_type="syntax", name="syntax", tag=1)
                ]
            ),
            MessageElement(
                location=location.at(4, 1),
                name="import",
                fields=[
                    FieldElement(location.at(5, 3), label=Field.Label.OPTIONAL, element_type="import", name="import", tag=1)
                ]
            ),
            MessageElement(
                location=location.at(7, 1),
                name="package",
                fields=[
                    FieldElement(
                        location.at(8, 3), label=Field.Label.OPTIONAL, element_type="package", name="package", tag=1
                    )
                ]
            ),
            MessageElement(
                location=location.at(10, 1),
                name="option",
                fields=[
                    FieldElement(
                        location.at(11, 3), label=Field.Label.OPTIONAL, element_type="option", name="option", tag=1
                    )
                ]
            ),
            MessageElement(
                location=location.at(13, 1),
                name="reserved",
                fields=[
                    FieldElement(
                        location.at(14, 3), label=Field.Label.OPTIONAL, element_type="reserved", name="reserved", tag=1
                    )
                ]
            ),
            MessageElement(
                location=location.at(16, 1),
                name="message",
                fields=[
                    FieldElement(
                        location.at(17, 3), label=Field.Label.OPTIONAL, element_type="message", name="message", tag=1
                    )
                ]
            ),
            MessageElement(
                location=location.at(19, 1),
                name="enum",
                fields=[
                    FieldElement(location.at(20, 3), label=Field.Label.OPTIONAL, element_type="enum", name="enum", tag=1)
                ]
            ),
            MessageElement(
                location=location.at(22, 1),
                name="service",
                fields=[
                    FieldElement(
                        location.at(23, 3), label=Field.Label.OPTIONAL, element_type="service", name="service", tag=1
                    )
                ]
            ),
            MessageElement(
                location=location.at(25, 1),
                name="extend",
                fields=[
                    FieldElement(
                        location.at(26, 3), label=Field.Label.OPTIONAL, element_type="extend", name="extend", tag=1
                    )
                ]
            ),
            MessageElement(
                location=location.at(28, 1),
                name="rpc",
                fields=[FieldElement(location.at(29, 3), label=Field.Label.OPTIONAL, element_type="rpc", name="rpc", tag=1)]
            ),
            MessageElement(
                location=location.at(31, 1),
                name="oneof",
                fields=[
                    FieldElement(location.at(32, 3), label=Field.Label.OPTIONAL, element_type="oneof", name="oneof", tag=1)
                ]
            ),
            MessageElement(
                location=location.at(34, 1),
                name="extensions",
                fields=[
                    FieldElement(
                        location.at(35, 3), label=Field.Label.OPTIONAL, element_type="extensions", name="extensions", tag=1
                    )
                ]
            ),
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto_keyword_as_message_name_and_field_proto3():
    # Note: this is consistent with protoc.
    proto = """
      |syntax = "proto3";
      |message syntax {
      |  syntax syntax = 1;
      |}
      |message import {
      |  import import = 1;
      |}
      |message package {
      |  package package = 1;
      |}
      |message option {
      |  option option = 1;
      |}
      |message reserved {
      |  // reserved reserved = 1;
      |}
      |message message {
      |  // message message = 1;
      |}
      |message enum {
      |  // enum enum = 1;
      |}
      |message service {
      |  service service = 1;
      |}
      |message extend {
      |  // extend extend = 1;
      |}
      |message rpc {
      |  rpc rpc = 1;
      |}
      |message oneof {
      |  // oneof oneof = 1;
      |}
      |message extensions {
      |  // extensions extensions = 1;
      |}
      |"""

    proto = trim_margin(proto)
    expected = ProtoFileElement(
        syntax=Syntax.PROTO_3,
        location=location,
        types=[
            MessageElement(
                location=location.at(2, 1),
                name="syntax",
                fields=[FieldElement(location.at(3, 3), element_type="syntax", name="syntax", tag=1)]
            ),
            MessageElement(
                location=location.at(5, 1),
                name="import",
                fields=[FieldElement(location.at(6, 3), element_type="import", name="import", tag=1)]
            ),
            MessageElement(
                location=location.at(8, 1),
                name="package",
                fields=[FieldElement(location.at(9, 3), element_type="package", name="package", tag=1)]
            ),
            MessageElement(
                location=location.at(11, 1),
                name="option",
                options=[OptionElement(name="option", kind=OptionElement.Kind.NUMBER, value="1", is_parenthesized=False)],
            ),
            MessageElement(
                location=location.at(14, 1),
                name="reserved",
            ),
            MessageElement(
                location=location.at(17, 1),
                name="message",
            ),
            MessageElement(
                location=location.at(20, 1),
                name="enum",
            ),
            MessageElement(
                location=location.at(23, 1),
                name="service",
                fields=[FieldElement(location.at(24, 3), element_type="service", name="service", tag=1)]
            ),
            MessageElement(
                location=location.at(26, 1),
                name="extend",
            ),
            MessageElement(
                location=location.at(29, 1),
                name="rpc",
                fields=[FieldElement(location.at(30, 3), element_type="rpc", name="rpc", tag=1)]
            ),
            MessageElement(
                location=location.at(32, 1),
                name="oneof",
            ),
            MessageElement(
                location=location.at(35, 1),
                name="extensions",
            ),
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_proto_keyword_as_service_name_and_rpc():
    # Note: this is consistent with protoc.
    proto = """
      |service syntax {
      |  rpc syntax (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service import {
      |  rpc import (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service package {
      |  rpc package (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service option {
      |  rpc option (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service reserved {
      |  rpc reserved (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service message {
      |  rpc message (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service enum {
      |  rpc enum (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service service {
      |  rpc service (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service extend {
      |  rpc extend (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service rpc {
      |  rpc rpc (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service oneof {
      |  rpc oneof (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |service extensions {
      |  rpc extensions (google.protobuf.StringValue) returns (google.protobuf.StringValue);
      |}
      |"""
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        services=[
            ServiceElement(
                location=location.at(1, 1),
                name="syntax",
                rpcs=[
                    RpcElement(
                        location.at(2, 3),
                        name="syntax",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(4, 1),
                name="import",
                rpcs=[
                    RpcElement(
                        location.at(5, 3),
                        name="import",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(7, 1),
                name="package",
                rpcs=[
                    RpcElement(
                        location.at(8, 3),
                        name="package",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(10, 1),
                name="option",
                rpcs=[
                    RpcElement(
                        location.at(11, 3),
                        name="option",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(13, 1),
                name="reserved",
                rpcs=[
                    RpcElement(
                        location.at(14, 3),
                        name="reserved",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(16, 1),
                name="message",
                rpcs=[
                    RpcElement(
                        location.at(17, 3),
                        name="message",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(19, 1),
                name="enum",
                rpcs=[
                    RpcElement(
                        location.at(20, 3),
                        name="enum",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(22, 1),
                name="service",
                rpcs=[
                    RpcElement(
                        location.at(23, 3),
                        name="service",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(25, 1),
                name="extend",
                rpcs=[
                    RpcElement(
                        location.at(26, 3),
                        name="extend",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(28, 1),
                name="rpc",
                rpcs=[
                    RpcElement(
                        location.at(29, 3),
                        name="rpc",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue",
                    )
                ]
            ),
            ServiceElement(
                location=location.at(31, 1),
                name="oneof",
                rpcs=[
                    RpcElement(
                        location.at(32, 3),
                        name="oneof",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue"
                    )
                ]
            ),
            ServiceElement(
                location=location.at(34, 1),
                name="extensions",
                rpcs=[
                    RpcElement(
                        location.at(35, 3),
                        name="extensions",
                        request_type="google.protobuf.StringValue",
                        response_type="google.protobuf.StringValue"
                    )
                ]
            ),
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_forbid_multiple_syntax_definitions():
    proto = """
          |  syntax = "proto2";
          |  syntax = "proto2";
          """
    proto = trim_margin(proto)
    with pytest.raises(IllegalStateException, match="Syntax error in file.proto:2:3: too many syntax definitions"):
        # TODO: this test in Kotlin source contains "2:13:" Need compile square.wire and check how it can be?
        ProtoParser.parse(location, proto)
        pytest.fail("")


def test_one_of_options():
    proto = """
        |message SearchRequest {
        |  required string query = 1;
        |  oneof page_info {
        |    option (my_option) = true;
        |    int32 page_number = 2;
        |    int32 result_per_page = 3;
        |  }
        |}
    """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        types=[
            MessageElement(
                location=location.at(1, 1),
                name="SearchRequest",
                fields=[
                    FieldElement(
                        location=location.at(2, 3), label=Field.Label.REQUIRED, element_type="string", name="query", tag=1
                    )
                ],
                one_ofs=[
                    OneOfElement(
                        name="page_info",
                        fields=[
                            FieldElement(location=location.at(5, 5), element_type="int32", name="page_number", tag=2),
                            FieldElement(location=location.at(6, 5), element_type="int32", name="result_per_page", tag=3)
                        ],
                        options=[
                            OptionElement("my_option", OptionElement.Kind.BOOLEAN, value="true", is_parenthesized=True)
                        ]
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected


def test_semi_colon_as_options_delimiters():
    proto = """
      |service MyService {
      |    option (custom_rule) = {
      |        my_string: "abc"; my_int: 3;
      |        my_list: ["a", "b", "c"];
      |    };
      |}
    """
    proto = trim_margin(proto)
    expected = ProtoFileElement(
        location=location,
        services=[
            ServiceElement(
                location=location.at(1, 1),
                name="MyService",
                options=[
                    OptionElement(
                        "custom_rule",
                        OptionElement.Kind.MAP, {
                            "my_string": "abc",
                            "my_int": "3",
                            "my_list": ["a", "b", "c"]
                        },
                        is_parenthesized=True
                    )
                ]
            )
        ]
    )
    assert ProtoParser.parse(location, proto) == expected
