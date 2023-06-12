"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/MessageElementTest.kt

from karapace.protobuf.extensions_element import ExtensionsElement
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.group_element import GroupElement
from karapace.protobuf.kotlin_wrapper import KotlinRange, trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.reserved_element import ReservedElement

location: Location = Location("", "file.proto")


def test_empty_to_schema():
    element = MessageElement(location=location, name="Message")
    expected = "message Message {}\n"
    assert element.to_schema() == expected


def test_simple_to_schema():
    element = MessageElement(
        location=location,
        name="Message",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)],
    )
    expected = """
        |message Message {
        |  required string name = 1;
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_add_multiple_fields():
    first_name = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="first_name", tag=1)
    last_name = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="last_name", tag=2)
    element = MessageElement(location=location, name="Message", fields=[first_name, last_name])
    assert len(element.fields) == 2


def test_simple_with_documentation_to_schema():
    element = MessageElement(
        location=location,
        name="Message",
        documentation="Hello",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)],
    )
    expected = """
        |// Hello
        |message Message {
        |  required string name = 1;
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_simple_with_options_to_schema():
    field = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)
    element = MessageElement(
        location=location, name="Message", fields=[field], options=[OptionElement("kit", OptionElement.Kind.STRING, "kat")]
    )
    expected = """message Message {
        |  option kit = "kat";
        |
        |  required string name = 1;
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_add_multiple_options():
    field = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)
    kit_kat = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    foo_bar = OptionElement("foo", OptionElement.Kind.STRING, "bar")
    element = MessageElement(location=location, name="Message", fields=[field], options=[kit_kat, foo_bar])
    assert len(element.options) == 2


def test_simple_with_nested_elements_to_schema():
    element = MessageElement(
        location=location,
        name="Message",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)],
        nested_types=[
            MessageElement(
                location=location,
                name="Nested",
                fields=[
                    FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)
                ],
            )
        ],
    )
    expected = """
        |message Message {
        |  required string name = 1;
        |
        |  message Nested {
        |    required string name = 1;
        |  }
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_add_multiple_types():
    nested1 = MessageElement(location=location, name="Nested1")
    nested2 = MessageElement(location=location, name="Nested2")
    element = MessageElement(
        location=location,
        name="Message",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)],
        nested_types=[nested1, nested2],
    )
    assert len(element.nested_types) == 2


def test_simple_with_extensions_to_schema():
    element = MessageElement(
        location=location,
        name="Message",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)],
        extensions=[ExtensionsElement(location=location, values=[KotlinRange(500, 501)])],
    )
    expected = """
        |message Message {
        |  required string name = 1;
        |
        |  extensions 500 to 501;
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_add_multiple_extensions():
    fives = ExtensionsElement(location=location, values=[KotlinRange(500, 501)])
    sixes = ExtensionsElement(location=location, values=[KotlinRange(600, 601)])
    element = MessageElement(
        location=location,
        name="Message",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)],
        extensions=[fives, sixes],
    )
    assert len(element.extensions) == 2


def test_one_of_to_schema():
    element = MessageElement(
        location=location,
        name="Message",
        one_ofs=[
            OneOfElement(name="hi", fields=[FieldElement(location=location, element_type="string", name="name", tag=1)])
        ],
    )
    expected = """
        |message Message {
        |  oneof hi {
        |    string name = 1;
        |  }
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_one_of_with_group_to_schema():
    element = MessageElement(
        location=location,
        name="Message",
        one_ofs=[
            OneOfElement(
                name="hi",
                fields=[FieldElement(location=location, element_type="string", name="name", tag=1)],
                groups=[
                    GroupElement(
                        location=location.at(5, 5),
                        name="Stuff",
                        tag=3,
                        label=None,
                        fields=[
                            FieldElement(
                                location=location.at(6, 7),
                                label=Field.Label.OPTIONAL,
                                element_type="int32",
                                name="result_per_page",
                                tag=4,
                            ),
                            FieldElement(
                                location=location.at(7, 7),
                                label=Field.Label.OPTIONAL,
                                element_type="int32",
                                name="page_count",
                                tag=5,
                            ),
                        ],
                    )
                ],
            )
        ],
    )

    expected = (
        """
        |message Message {
        |  oneof hi {
        |    string name = 1;
        |  """
        + """
        |    group Stuff = 3 {
        |      optional int32 result_per_page = 4;
        |      optional int32 page_count = 5;
        |    }
        |  }
        |}
        |"""
    )
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_add_multiple_one_ofs():
    hi = OneOfElement(name="hi", fields=[FieldElement(location=location, element_type="string", name="name", tag=1)])
    hey = OneOfElement(name="hey", fields=[FieldElement(location=location, element_type="string", name="city", tag=2)])
    element = MessageElement(location=location, name="Message", one_ofs=[hi, hey])
    assert len(element.one_ofs) == 2


def test_reserved_to_schema():
    element = MessageElement(
        location=location,
        name="Message",
        reserveds=[
            ReservedElement(location=location, values=[10, KotlinRange(12, 14), "foo"]),
            ReservedElement(location=location, values=[10]),
            ReservedElement(location=location, values=[KotlinRange(12, 14)]),
            ReservedElement(location=location, values=["foo"]),
        ],
    )
    expected = """
        |message Message {
        |  reserved 10, 12 to 14, "foo";
        |  reserved 10;
        |  reserved 12 to 14;
        |  reserved "foo";
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_group_to_schema():
    element = MessageElement(
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
                        location=location.at(5, 5), label=Field.Label.REPEATED, element_type="string", name="snippets", tag=4
                    ),
                ],
            )
        ],
    )
    expected = """
        |message SearchResponse {
        |  repeated group Result = 1 {
        |    required string url = 2;
        |    optional string title = 3;
        |    repeated string snippets = 4;
        |  }
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_multiple_everything_to_schema():
    field1 = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)
    field2 = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="bool", name="other_name", tag=2)
    one_off_1_field = FieldElement(location=location, element_type="string", name="namey", tag=3)
    one_of_1 = OneOfElement(name="thingy", fields=[one_off_1_field])
    one_off_2_field = FieldElement(location=location, element_type="string", name="namer", tag=4)
    one_of_2 = OneOfElement(name="thinger", fields=[one_off_2_field])
    extensions1 = ExtensionsElement(location=location, values=[KotlinRange(500, 501)])
    extensions2 = ExtensionsElement(location=location, values=[503])
    nested = MessageElement(location=location, name="Nested", fields=[field1])
    option = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    element = MessageElement(
        location=location,
        name="Message",
        fields=[field1, field2],
        one_ofs=[one_of_1, one_of_2],
        nested_types=[nested],
        extensions=[extensions1, extensions2],
        options=[option],
    )
    expected = """
        |message Message {
        |  option kit = "kat";
        |
        |  required string name = 1;
        |
        |  required bool other_name = 2;
        |
        |  oneof thingy {
        |    string namey = 3;
        |  }
        |
        |  oneof thinger {
        |    string namer = 4;
        |  }
        |
        |  extensions 500 to 501;
        |  extensions 503;
        |
        |  message Nested {
        |    required string name = 1;
        |  }
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_field_to_schema():
    field = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)
    expected = "required string name = 1;\n"
    assert field.to_schema() == expected


def test_field_with_default_string_to_schema_in_proto2():
    field = FieldElement(
        location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1, default_value="benoît"
    )
    expected = 'required string name = 1 [default = "benoît"];\n'
    assert field.to_schema() == expected


def test_field_with_default_number_to_schema():
    field = FieldElement(
        location=location, label=Field.Label.REQUIRED, element_type="int32", name="age", tag=1, default_value="34"
    )
    expected = "required int32 age = 1 [default = 34];\n"
    assert field.to_schema() == expected


def test_field_with_default_bool_to_schema():
    field = FieldElement(
        location=location, label=Field.Label.REQUIRED, element_type="bool", name="human", tag=1, default_value="true"
    )
    expected = "required bool human = 1 [default = true];\n"
    assert field.to_schema() == expected


def test_one_of_field_to_schema():
    field = FieldElement(location=location, element_type="string", name="name", tag=1)
    expected = "string name = 1;\n"
    assert field.to_schema() == expected


def test_field_with_documentation_to_schema():
    field = FieldElement(
        location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1, documentation="Hello"
    )
    expected = """// Hello
        |required string name = 1;
        |"""
    expected = trim_margin(expected)
    assert field.to_schema() == expected


def test_field_with_one_option_to_schema():
    field = FieldElement(
        location=location,
        label=Field.Label.REQUIRED,
        element_type="string",
        name="name",
        tag=1,
        options=[OptionElement("kit", OptionElement.Kind.STRING, "kat")],
    )
    expected = """required string name = 1 [kit = "kat"];
        |"""
    expected = trim_margin(expected)
    assert field.to_schema() == expected


def test_field_with_more_than_one_option_to_schema():
    field = FieldElement(
        location=location,
        label=Field.Label.REQUIRED,
        element_type="string",
        name="name",
        tag=1,
        options=[
            OptionElement("kit", OptionElement.Kind.STRING, "kat"),
            OptionElement("dup", OptionElement.Kind.STRING, "lo"),
        ],
    )
    expected = """required string name = 1 [
        |  kit = "kat",
        |  dup = "lo"
        |];
        |"""
    expected = trim_margin(expected)
    assert field.to_schema() == expected


def test_one_of_with_options():
    expected = """
        |oneof page_info {
        |  option (my_option) = true;
        |
        |  int32 page_number = 2;
        |  int32 result_per_page = 3;
        |}
        |"""
    expected = trim_margin(expected)
    one_of = OneOfElement(
        name="page_info",
        fields=[
            FieldElement(location=location.at(4, 5), element_type="int32", name="page_number", tag=2),
            FieldElement(location=location.at(5, 5), element_type="int32", name="result_per_page", tag=3),
        ],
        options=[OptionElement("my_option", OptionElement.Kind.BOOLEAN, "true", True)],
    )
    assert one_of.to_schema() == expected
