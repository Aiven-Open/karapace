"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/OptionElementTest.kt

from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.option_element import OptionElement


def test_simple_to_schema():
    option = OptionElement("foo", OptionElement.Kind.STRING, "bar")
    expected = """foo = \"bar\""""
    assert option.to_schema() == expected


def test_nested_to_schema():
    option = OptionElement(
        "foo.boo", OptionElement.Kind.OPTION, OptionElement("bar", OptionElement.Kind.STRING, "baz"), True
    )
    expected = """(foo.boo).bar = \"baz\""""
    assert option.to_schema() == expected


def test_list_to_schema():
    option = OptionElement(
        "foo",
        OptionElement.Kind.LIST,
        [
            OptionElement("ping", OptionElement.Kind.STRING, "pong", True),
            OptionElement("kit", OptionElement.Kind.STRING, "kat"),
        ],
        True,
    )
    expected = """
        |(foo) = [
        |  (ping) = "pong",
        |  kit = "kat"
        |]
        """
    expected = trim_margin(expected)
    assert option.to_schema() == expected


def test_map_to_schema():
    option = OptionElement("foo", OptionElement.Kind.MAP, {"ping": "pong", "kit": ["kat", "kot"]})
    expected = """
        |foo = {
        |  ping: "pong",
        |  kit: [
        |    "kat",
        |    "kot"
        |  ]
        |}
        """
    expected = trim_margin(expected)
    assert option.to_schema() == expected


def test_boolean_to_schema():
    option = OptionElement("foo", OptionElement.Kind.BOOLEAN, "false")
    expected = "foo = false"
    assert option.to_schema() == expected
