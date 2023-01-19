"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/ExtensionsElementTest.kt

from karapace.protobuf.extensions_element import ExtensionsElement
from karapace.protobuf.kotlin_wrapper import KotlinRange, trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.utils import MAX_TAG_VALUE

location = Location.get("file.proto")


def test_single_value_to_schema():
    actual = ExtensionsElement(location=location, values=[500])
    expected = "extensions 500;\n"
    assert actual.to_schema() == expected


def test_range_to_schema():
    actual = ExtensionsElement(location=location, values=[KotlinRange(500, 505)])
    expected = "extensions 500 to 505;\n"
    assert actual.to_schema() == expected


def test_max_range_to_schema():
    actual = ExtensionsElement(location=location, values=[KotlinRange(500, MAX_TAG_VALUE)])
    expected = "extensions 500 to max;\n"
    assert actual.to_schema() == expected


def test_with_documentation_to_schema():
    actual = ExtensionsElement(location=location, documentation="Hello", values=[500])
    expected = """
        |// Hello
        |extensions 500;
        |"""
    expected = trim_margin(expected)
    assert actual.to_schema() == expected
