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
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.syntax import Syntax
from karapace.protobuf.utils import MAX_TAG_VALUE

import pytest

location: Location = Location.get("file.proto")


def test_type_parsing():
    proto: str = """
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
    proto: str = trim_margin(proto)

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
    # try :
    #   ProtoParser.parse(location, proto)
    # except IllegalStateException as e :
    #    if e.message != "Syntax error in file.proto:2:12: expected '//' or '/*'" :
    #        pytest.fail("")

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
                documentation="",
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
                documentation="",
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
                documentation="",
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
                documentation="",
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
                        documentation="",
                        fields=[
                            FieldElement(location=location.at(4, 5), element_type="int32", name="page_number", tag=2),
                            FieldElement(location=location.at(5, 5), element_type="int32", name="result_per_page", tag=3)
                        ],
                        groups=[],
                        options=[]
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
                        documentation="",
                        fields=[FieldElement(location=location.at(4, 5), element_type="int32", name="page_number", tag=2)],
                        groups=[
                            GroupElement(
                                label=None,
                                location=location.at(5, 5),
                                name="Stuff",
                                tag=3,
                                documentation="",
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
                        options=[]
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
                    EnumConstantElement(location=location.at(6, 3), name="FRUIT", tag=1, documentation="", options=[]),
                    EnumConstantElement(
                        location=location.at(8, 3), name="CREAM", tag=2, documentation="Yummy, yummy cream.", options=[]
                    ),
                    EnumConstantElement(
                        location=location.at(11, 3), name="SYRUP", tag=3, documentation="Quebec Maple syrup", options=[]
                    )
                ],
                options=[]
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
                        documentation="",
                        options=[OptionElement("healthy", OptionElement.Kind.BOOLEAN, "true", True)]
                    ),
                    EnumConstantElement(
                        location=location.at(10, 3), name="CREAM", tag=2, documentation="Yummy, yummy cream.", options=[]
                    ),
                    EnumConstantElement(
                        location=location.at(13, 3), name="SYRUP", tag=3, documentation="Quebec Maple syrup", options=[]
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
        documentation="",
        constants=[
            EnumConstantElement(
                location=location.at(4, 5),
                name="STRING",
                tag=0,
                documentation="",
                options=[
                    OptionElement("opt_a", OptionElement.Kind.NUMBER, "1", True),
                    OptionElement("opt_b", OptionElement.Kind.NUMBER, "2", True)
                ]
            )
        ],
        options=[]
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
        documentation="",
        fields=[],
        nested_types=[],
        extensions=[
            ExtensionsElement(
                location=location.at(2, 3),
                documentation="",
                values=[1] + [KotlinRange(5, 200)] + [500] + [KotlinRange(1000, MAX_TAG_VALUE)]
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
                documentation="",
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
                documentation="",
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
                documentation="",
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
                documentation="",
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
                documentation="",
                options=[OptionElement("default_timeout", OptionElement.Kind.NUMBER, "30", True)],
                rpcs=[
                    RpcElement(
                        location=location.at(4, 3),
                        name="Search",
                        documentation="",
                        request_type="SearchRequest",
                        response_type="SearchResponse",
                        options=[],
                        response_streaming=False,
                        request_streaming=False
                    ),
                    RpcElement(
                        location=location.at(5, 3),
                        name="Purchase",
                        documentation="",
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
                documentation="",
                rpcs=[
                    RpcElement(
                        location=location.at(2, 3),
                        name="GetFeature",
                        documentation="",
                        request_type="Point",
                        response_type="Feature",
                        options=[],
                        response_streaming=False,
                        request_streaming=False
                    ),
                    RpcElement(
                        location=location.at(3, 3),
                        name="ListFeatures",
                        documentation="",
                        request_type="Rectangle",
                        response_type="Feature",
                        response_streaming=True,
                        # TODO: Report Square.Wire there was mistake True instead of False!
                        request_streaming=False,
                        options=[]
                    ),
                    RpcElement(
                        location=location.at(4, 3),
                        name="RecordRoute",
                        documentation="",
                        request_type="Point",
                        response_type="RouteSummary",
                        request_streaming=True,
                        response_streaming=False,
                        options=[]
                    ),
                    RpcElement(
                        location=location.at(5, 3),
                        name="RouteChat",
                        documentation="",
                        request_type="RouteNote",
                        response_type="RouteNote",
                        request_streaming=True,
                        response_streaming=True,
                        options=[]
                    )
                ],
                options=[]
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
