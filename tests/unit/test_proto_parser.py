from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.exception import IllegalStateException
from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.proto_parser import ProtoParser
from karapace.protobuf.syntax import Syntax

import unittest


class ProtoParserTest(unittest.TestCase):
    location: Location = Location.get("file.proto")

    def test_type_parsing(self, ):
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
            location=self.location,
            types=[
                MessageElement(
                    location=self.location.at(1, 1),
                    name="Types",
                    fields=[
                        FieldElement(
                            location=self.location.at(2, 3),
                            label=Field.Label.REQUIRED,
                            element_type="any",
                            name="f1",
                            tag=1
                        ),
                        FieldElement(
                            location=self.location.at(3, 3),
                            label=Field.Label.REQUIRED,
                            element_type="bool",
                            name="f2",
                            tag=2
                        ),
                        FieldElement(
                            location=self.location.at(4, 3),
                            label=Field.Label.REQUIRED,
                            element_type="bytes",
                            name="f3",
                            tag=3
                        ),
                        FieldElement(
                            location=self.location.at(5, 3),
                            label=Field.Label.REQUIRED,
                            element_type="double",
                            name="f4",
                            tag=4
                        ),
                        FieldElement(
                            location=self.location.at(6, 3),
                            label=Field.Label.REQUIRED,
                            element_type="float",
                            name="f5",
                            tag=5
                        ),
                        FieldElement(
                            location=self.location.at(7, 3),
                            label=Field.Label.REQUIRED,
                            element_type="fixed32",
                            name="f6",
                            tag=6
                        ),
                        FieldElement(
                            location=self.location.at(8, 3),
                            label=Field.Label.REQUIRED,
                            element_type="fixed64",
                            name="f7",
                            tag=7
                        ),
                        FieldElement(
                            location=self.location.at(9, 3),
                            label=Field.Label.REQUIRED,
                            element_type="int32",
                            name="f8",
                            tag=8
                        ),
                        FieldElement(
                            location=self.location.at(10, 3),
                            label=Field.Label.REQUIRED,
                            element_type="int64",
                            name="f9",
                            tag=9
                        ),
                        FieldElement(
                            location=self.location.at(11, 3),
                            label=Field.Label.REQUIRED,
                            element_type="sfixed32",
                            name="f10",
                            tag=10
                        ),
                        FieldElement(
                            location=self.location.at(12, 3),
                            label=Field.Label.REQUIRED,
                            element_type="sfixed64",
                            name="f11",
                            tag=11
                        ),
                        FieldElement(
                            location=self.location.at(13, 3),
                            label=Field.Label.REQUIRED,
                            element_type="sint32",
                            name="f12",
                            tag=12
                        ),
                        FieldElement(
                            location=self.location.at(14, 3),
                            label=Field.Label.REQUIRED,
                            element_type="sint64",
                            name="f13",
                            tag=13
                        ),
                        FieldElement(
                            location=self.location.at(15, 3),
                            label=Field.Label.REQUIRED,
                            element_type="string",
                            name="f14",
                            tag=14
                        ),
                        FieldElement(
                            location=self.location.at(16, 3),
                            label=Field.Label.REQUIRED,
                            element_type="uint32",
                            name="f15",
                            tag=15
                        ),
                        FieldElement(
                            location=self.location.at(17, 3),
                            label=Field.Label.REQUIRED,
                            element_type="uint64",
                            name="f16",
                            tag=16
                        ),
                        FieldElement(location=self.location.at(18, 3), element_type="map<string, bool>", name="f17", tag=17),
                        FieldElement(
                            location=self.location.at(19, 3),
                            element_type="map<arbitrary, nested.nested>",
                            name="f18",
                            tag=18
                        ),
                        FieldElement(
                            location=self.location.at(20, 3),
                            label=Field.Label.REQUIRED,
                            element_type="arbitrary",
                            name="f19",
                            tag=19
                        ),
                        FieldElement(
                            location=self.location.at(21, 3),
                            label=Field.Label.REQUIRED,
                            element_type="nested.nested",
                            name="f20",
                            tag=20
                        )
                    ]
                )
            ]
        )
        my = ProtoParser.parse(self.location, proto)
        self.assertEqual(my, expected)

    def test_map_with_label_throws(self):
        with self.assertRaisesRegex(IllegalStateException, "Syntax error in file.proto:1:15: 'map' type cannot have label"):
            ProtoParser.parse(self.location, "message Hey { required map<string, string> a = 1; }")
            self.fail()

        with self.assertRaisesRegex(IllegalStateException, "Syntax error in file.proto:1:15: 'map' type cannot have label"):
            ProtoParser.parse(self.location, "message Hey { optional map<string, string> a = 1; }")
            self.fail()

        with self.assertRaisesRegex(IllegalStateException, "Syntax error in file.proto:1:15: 'map' type cannot have label"):
            ProtoParser.parse(self.location, "message Hey { repeated map<string, string> a = 1; }")
            self.fail()

    def test_default_field_option_is_special(self):
        """ It looks like an option, but 'default' is special. It's not defined as an option.
        """
        proto = """
            |message Message {
            |  required string a = 1 [default = "b", faulted = "c"];
            |}
            |"""

        proto = trim_margin(proto)
        expected = ProtoFileElement(
            location=self.location,
            types=[
                MessageElement(
                    location=self.location.at(1, 1),
                    name="Message",
                    fields=[
                        FieldElement(
                            location=self.location.at(2, 3),
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
        self.assertEqual(ProtoParser.parse(self.location, proto), expected)

    def test_json_name_option_is_special(self):
        """ It looks like an option, but 'json_name' is special. It's not defined as an option.
        """
        proto = """
            |message Message {
            |  required string a = 1 [json_name = "b", faulted = "c"];
            |}
            |"""
        proto = trim_margin(proto)

        expected = ProtoFileElement(
            location=self.location,
            types=[
                MessageElement(
                    location=self.location.at(1, 1),
                    name="Message",
                    fields=[
                        FieldElement(
                            location=self.location.at(2, 3),
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
        self.assertEqual(ProtoParser.parse(self.location, proto), expected)

    def test_single_line_comment(self):
        proto = """
            |// Test all the things!
            |message Test {}
            """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        element_type = parsed.types[0]
        self.assertEqual(element_type.documentation, "Test all the things!")

    def test_multiple_single_line_comments(self):
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

        parsed = ProtoParser.parse(self.location, proto)
        element_type = parsed.types[0]
        self.assertEqual(element_type.documentation, expected)

    def test_single_line_javadoc_comment(self):
        proto = """
            |/** Test */
            |message Test {}
            |"""
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        element_type = parsed.types[0]
        self.assertEqual(element_type.documentation, "Test")

    def test_multiline_javadoc_comment(self):
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
        parsed = ProtoParser.parse(self.location, proto)
        element_type = parsed.types[0]
        self.assertEqual(element_type.documentation, expected)

    def test_multiple_single_line_comments_with_leading_whitespace(self):
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
        parsed = ProtoParser.parse(self.location, proto)
        element_type = parsed.types[0]
        self.assertEqual(element_type.documentation, expected)

    def test_multiline_javadoc_comment_with_leading_whitespace(self):
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
        parsed = ProtoParser.parse(self.location, proto)
        element_type = parsed.types[0]
        self.assertEqual(element_type.documentation, expected)

    def test_multiline_javadoc_comment_without_leading_asterisks(self):
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
        parsed = ProtoParser.parse(self.location, proto)
        element_type = parsed.types[0]
        self.assertEqual(element_type.documentation, expected)

    def test_message_field_trailing_comment(self):
        # Trailing message field comment.
        proto = """
            |message Test {
            |  optional string name = 1; // Test all the things!
            |}
            """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        message: MessageElement = parsed.types[0]
        field = message.fields[0]
        self.assertEqual(field.documentation, "Test all the things!")

    def test_message_field_leading_and_trailing_comment_are_combined(self):
        proto = """
            |message Test {
            |  // Test all...
            |  optional string name = 1; // ...the things!
            |}
            """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        message: MessageElement = parsed.types[0]
        field = message.fields[0]
        self.assertEqual(field.documentation, "Test all...\n...the things!")

    def test_trailing_comment_not_assigned_to_following_field(self):
        proto = """
            |message Test {
            |  optional string first_name = 1; // Testing!
            |  optional string last_name = 2;
            |}
            """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        message: MessageElement = parsed.types[0]
        field1 = message.fields[0]
        self.assertEqual(field1.documentation, "Testing!")
        field2 = message.fields[1]
        self.assertEqual(field2.documentation, "")

    def test_enum_value_trailing_comment(self):
        proto = """
            |enum Test {
            |  FOO = 1; // Test all the things!
            |}
            """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        enum_element: EnumElement = parsed.types[0]
        value = enum_element.constants[0]
        self.assertEqual(value.documentation, "Test all the things!")

    def test_trailing_singleline_comment(self):
        proto = """
            |enum Test {
            |  FOO = 1; /* Test all the things!  */
            |  BAR = 2;/*Test all the things!*/
            |}
            """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        enum_element: EnumElement = parsed.types[0]
        c_foo = enum_element.constants[0]
        self.assertEqual(c_foo.documentation, "Test all the things!")
        c_bar = enum_element.constants[1]
        self.assertEqual(c_bar.documentation, "Test all the things!")

    def test_trailing_multiline_comment(self):
        proto = """
          |enum Test {
          |  FOO = 1; /* Test all the
          |things! */
          |}
          """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        enum_element: EnumElement = parsed.types[0]
        value = enum_element.constants[0]
        self.assertEqual(value.documentation, "Test all the\nthings!")

    def test_trailing_multiline_comment_must_be_last_on_line_throws(self):
        proto = """
            |enum Test {
            |  FOO = 1; /* Test all the things! */ BAR = 2;
            |}
            """
        proto = trim_margin(proto)
        with self.assertRaisesRegex(
            IllegalStateException, "Syntax error in file.proto:2:40: no syntax may follow trailing comment"
        ):
            ProtoParser.parse(self.location, proto)
            self.fail()

    def test_invalid_trailing_comment(self):
        proto = """
            |enum Test {
            |  FOO = 1; /
            |}
            """
        proto = trim_margin(proto)
        # try :
        #   ProtoParser.parse(self.location, proto)
        # except IllegalStateException as e :
        #    if e.message != "Syntax error in file.proto:2:12: expected '//' or '/*'" :
        #        self.fail()

        with self.assertRaises(IllegalStateException) as re:
            # TODO: this test in Kotlin source contains "2:13:" Need compile square.wire and check how it can be?

            ProtoParser.parse(self.location, proto)
            self.fail()
        self.assertEqual(re.exception.message, "Syntax error in file.proto:2:12: expected '//' or '/*'")

    def test_enum_value_leading_and_trailing_comments_are_combined(self):
        proto = """
          |enum Test {
          |  // Test all...
          |  FOO = 1; // ...the things!
          |}
          """
        proto = trim_margin(proto)
        parsed = ProtoParser.parse(self.location, proto)
        enum_element: EnumElement = parsed.types[0]
        value = enum_element.constants[0]
        self.assertEqual(value.documentation, "Test all...\n...the things!")

    def test_trailing_comment_not_combined_when_empty(self):
        """ (Kotlin) Can't use raw strings here; otherwise, the formatter removes the trailing whitespace on line 3. """
        proto = "enum Test {\n" \
                "  // Test all...\n" \
                "  FOO = 1; //       \n" \
                "}"
        parsed = ProtoParser.parse(self.location, proto)
        enum_element: EnumElement = parsed.types[0]
        value = enum_element.constants[0]
        self.assertEqual(value.documentation, "Test all...")

    def test_syntax_not_required(self):
        proto = "message Foo {}"
        parsed = ProtoParser.parse(self.location, proto)
        self.assertIsNone(parsed.syntax)

    def test_syntax_specified(self):
        proto = """
          |syntax = "proto3";
          |message Foo {}
          """
        proto = trim_margin(proto)
        expected = ProtoFileElement(
            location=self.location,
            syntax=Syntax.PROTO_3,
            types=[MessageElement(location=self.location.at(2, 1), name="Foo")]
        )
        self.assertEqual(ProtoParser.parse(self.location, proto), expected)

    def test_invalid_syntax_value_throws(self):
        proto = """
          |syntax = "proto4";
          |message Foo {}
          """
        proto = trim_margin(proto)
        with self.assertRaisesRegex(IllegalStateException, "Syntax error in file.proto:1:1: unexpected syntax: proto4"):
            ProtoParser.parse(self.location, proto)
            self.fail()

    def test_syntax_not_first_declaration_throws(self):
        proto = """
          |message Foo {}
          |syntax = "proto3";
          """
        proto = trim_margin(proto)
        with self.assertRaisesRegex(
            IllegalStateException, "Syntax error in file.proto:2:1: 'syntax' element must be the first declaration "
            "in a file"
        ):
            ProtoParser.parse(self.location, proto)
            self.fail()

    def test_syntax_may_follow_comments_and_empty_lines(self):
        proto = """
          |/* comment 1 */
          |// comment 2
          |
          |syntax = "proto3";
          |message Foo {}
          """
        proto = trim_margin(proto)
        expected = ProtoFileElement(
            location=self.location,
            syntax=Syntax.PROTO_3,
            types=[MessageElement(location=self.location.at(5, 1), name="Foo")]
        )
        self.assertEqual(ProtoParser.parse(self.location, proto), expected)

    def test_proto3_message_fields_do_not_require_labels(self):
        proto = """
          |syntax = "proto3";
          |message Message {
          |  string a = 1;
          |  int32 b = 2;
          |}
          """
        proto = trim_margin(proto)
        expected = ProtoFileElement(
            location=self.location,
            syntax=Syntax.PROTO_3,
            types=[
                MessageElement(
                    location=self.location.at(2, 1),
                    name="Message",
                    fields=[
                        FieldElement(location=self.location.at(3, 3), element_type="string", name="a", tag=1),
                        FieldElement(location=self.location.at(4, 3), element_type="int32", name="b", tag=2)
                    ]
                )
            ]
        )
        self.assertEqual(ProtoParser.parse(self.location, proto), expected)

    def test_proto3_extension_fields_do_not_require_labels(self):
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
            location=self.location,
            syntax=Syntax.PROTO_3,
            types=[MessageElement(location=self.location.at(2, 1), name="Message")],
            extend_declarations=[
                ExtendElement(
                    location=self.location.at(4, 1),
                    name="Message",
                    documentation="",
                    fields=[
                        FieldElement(location=self.location.at(5, 3), element_type="string", name="a", tag=1),
                        FieldElement(location=self.location.at(6, 3), element_type="int32", name="b", tag=2)
                    ]
                )
            ]
        )
        self.assertEqual(ProtoParser.parse(self.location, proto), expected)

    def test_proto3_message_fields_allow_optional(self):
        proto = """
          |syntax = "proto3";
          |message Message {
          |  optional string a = 1;
          |}
          """
        proto = trim_margin(proto)

        expected = ProtoFileElement(
            location=self.location,
            syntax=Syntax.PROTO_3,
            types=[
                MessageElement(
                    location=self.location.at(2, 1),
                    name="Message",
                    fields=[
                        FieldElement(
                            location=self.location.at(3, 3),
                            element_type="string",
                            name="a",
                            tag=1,
                            label=Field.Label.OPTIONAL
                        )
                    ]
                )
            ]
        )
        self.assertEqual(ProtoParser.parse(self.location, proto), expected)
