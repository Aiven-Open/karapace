"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/FieldElementTest.kt

from karapace.core.protobuf.field import Field
from karapace.core.protobuf.field_element import FieldElement
from karapace.core.protobuf.kotlin_wrapper import trim_margin
from karapace.core.protobuf.location import Location
from karapace.core.protobuf.option_element import OptionElement

location = Location("", "file.proto")


def test_field():
    field = FieldElement(
        location=location,
        label=Field.Label.OPTIONAL,
        element_type="CType",
        name="ctype",
        tag=1,
        options=[
            OptionElement("default", OptionElement.Kind.ENUM, "TEST"),
            OptionElement("deprecated", OptionElement.Kind.BOOLEAN, "true"),
        ],
    )

    assert len(field.options) == 2
    assert OptionElement("default", OptionElement.Kind.ENUM, "TEST") in field.options
    assert OptionElement("deprecated", OptionElement.Kind.BOOLEAN, "true") in field.options


def test_add_multiple_options():
    kit_kat = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    foo_bar = OptionElement("foo", OptionElement.Kind.STRING, "bar")
    field = FieldElement(
        location=location, label=Field.Label.REQUIRED, element_type="string", name="name", tag=1, options=[kit_kat, foo_bar]
    )

    assert len(field.options) == 2


def test_default_is_set():
    field = FieldElement(
        location=location,
        label=Field.Label.REQUIRED,
        element_type="string",
        name="name",
        tag=1,
        default_value="defaultValue",
    )

    assert field.to_schema() == trim_margin(
        """
            |required string name = 1 [default = "defaultValue"];
            |"""
    )


def test_json_name_and_default_value():
    field = FieldElement(
        location=location,
        label=Field.Label.REQUIRED,
        element_type="string",
        name="name",
        default_value="defaultValue",
        json_name="my_json",
        tag=1,
    )

    assert field.to_schema() == trim_margin(
        """
            |required string name = 1 [
            |  default = "defaultValue",
            |  json_name = "my_json"
            |];
            |"""
    )


def test_json_name():
    field = FieldElement(
        location=location, label=Field.Label.REQUIRED, element_type="string", name="name", json_name="my_json", tag=1
    )

    assert field.to_schema() == trim_margin(
        """
            |required string name = 1 [json_name = "my_json"];
            |"""
    )
