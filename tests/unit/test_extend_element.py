# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/ExtendElementTest.kt

from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location

location = Location.get("file.proto")


def test_empty_to_schema():
    extend = ExtendElement(location=location, name="Name")
    expected = "extend Name {}\n"
    assert extend.to_schema() == expected


def test_simple_to_schema():
    extend = ExtendElement(
        location=location,
        name="Name",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)]
    )
    expected = """
        |extend Name {
        |  required string name = 1;
        |}
        |"""
    expected = trim_margin(expected)
    assert extend.to_schema() == expected


def test_add_multiple_fields():
    first_name = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="first_name", tag=1)
    last_name = FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="last_name", tag=2)
    extend = ExtendElement(location=location, name="Name", fields=[first_name, last_name])
    assert len(extend.fields) == 2


def test_simple_with_documentation_to_schema():
    extend = ExtendElement(
        location=location,
        name="Name",
        documentation="Hello",
        fields=[FieldElement(location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1)]
    )
    expected = """
        |// Hello
        |extend Name {
        |  required string name = 1;
        |}
        |"""
    expected = trim_margin(expected)
    assert extend.to_schema() == expected


def test_json_name_to_schema():
    extend = ExtendElement(
        location=location,
        name="Name",
        fields=[
            FieldElement(
                location=location,
                label=Field.Label.REQUIRED,
                element_type="string",
                name="name",
                json_name="my_json",
                tag=1
            )
        ]
    )
    expected = """
        |extend Name {
        |  required string name = 1 [json_name = "my_json"];
        |}
        |"""
    expected = trim_margin(expected)
    assert extend.to_schema() == expected


def test_default_is_set_in_proto2_file():
    extend = ExtendElement(
        location=location,
        name="Name",
        documentation="Hello",
        fields=[
            FieldElement(
                location=location,
                label=Field.Label.REQUIRED,
                element_type="string",
                name="name",
                tag=1,
                default_value="defaultValue"
            )
        ]
    )
    expected = """
        |// Hello
        |extend Name {
        |  required string name = 1 [default = "defaultValue"];
        |}
        |"""
    expected = trim_margin(expected)
    assert extend.to_schema() == expected
