# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/EnumElementTest.kt

from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement

location: Location = Location.get("file.proto")


def test_empty_to_schema():
    element = EnumElement(location=location, name="Enum")
    expected = "enum Enum {}\n"
    assert element.to_schema() == expected


def test_simple_to_schema():
    element = EnumElement(
        location=location,
        name="Enum",
        constants=[
            EnumConstantElement(location=location, name="ONE", tag=1),
            EnumConstantElement(location=location, name="TWO", tag=2),
            EnumConstantElement(location=location, name="SIX", tag=6)
        ]
    )
    expected = """
        |enum Enum {
        |  ONE = 1;
        |  TWO = 2;
        |  SIX = 6;
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_add_multiple_constants():
    one = EnumConstantElement(location=location, name="ONE", tag=1)
    two = EnumConstantElement(location=location, name="TWO", tag=2)
    six = EnumConstantElement(location=location, name="SIX", tag=6)
    element = EnumElement(location=location, name="Enum", constants=[one, two, six])
    assert len(element.constants) == 3


def test_simple_with_options_to_schema():
    element = EnumElement(
        location=location,
        name="Enum",
        options=[OptionElement("kit", OptionElement.Kind.STRING, "kat")],
        constants=[
            EnumConstantElement(location=location, name="ONE", tag=1),
            EnumConstantElement(location=location, name="TWO", tag=2),
            EnumConstantElement(location=location, name="SIX", tag=6)
        ]
    )
    expected = """
        |enum Enum {
        |  option kit = "kat";
        |  ONE = 1;
        |  TWO = 2;
        |  SIX = 6;
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_add_multiple_options():
    kit_kat = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    foo_bar = OptionElement("foo", OptionElement.Kind.STRING, "bar")
    element = EnumElement(
        location=location,
        name="Enum",
        options=[kit_kat, foo_bar],
        constants=[EnumConstantElement(location=location, name="ONE", tag=1)]
    )
    assert len(element.options) == 2


def test_simple_with_documentation_to_schema():
    element = EnumElement(
        location=location,
        name="Enum",
        documentation="Hello",
        constants=[
            EnumConstantElement(location=location, name="ONE", tag=1),
            EnumConstantElement(location=location, name="TWO", tag=2),
            EnumConstantElement(location=location, name="SIX", tag=6)
        ]
    )
    expected = """
        |// Hello
        |enum Enum {
        |  ONE = 1;
        |  TWO = 2;
        |  SIX = 6;
        |}
        |"""
    expected = trim_margin(expected)
    assert element.to_schema() == expected


def test_field_to_schema():
    value = EnumConstantElement(location=location, name="NAME", tag=1)
    expected = "NAME = 1;\n"
    assert value.to_schema() == expected


def test_field_with_documentation_to_schema():
    value = EnumConstantElement(location=location, name="NAME", tag=1, documentation="Hello")
    expected = """
        |// Hello
        |NAME = 1;
        |"""
    expected = trim_margin(expected)
    assert value.to_schema() == expected


def test_field_with_options_to_schema():
    value = EnumConstantElement(
        location=location,
        name="NAME",
        tag=1,
        options=[
            OptionElement("kit", OptionElement.Kind.STRING, "kat", True),
            OptionElement("tit", OptionElement.Kind.STRING, "tat")
        ]
    )
    expected = """
        |NAME = 1 [
        |  (kit) = "kat",
        |  tit = "tat"
        |];
        |"""
    expected = trim_margin(expected)
    assert value.to_schema() == expected
