"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/ProtoFileElementTest.kt
from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.option_element import OptionElement, PACKED_OPTION_ELEMENT
from karapace.protobuf.proto_file_element import PackageName, ProtoFileElement, TypeName
from karapace.protobuf.proto_parser import ProtoParser
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.syntax import Syntax

import copy

location: Location = Location("some/folder", "file.proto")


def test_empty_to_schema():
    file = ProtoFileElement(location=location)
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_empty_with_package_to_schema():
    file = ProtoFileElement(location=location, package_name="example.simple")
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |package example.simple;
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_simple_to_schema():
    element = MessageElement(location=location, name="Message")
    file = ProtoFileElement(location=location, types=[element])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |message Message {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_simple_with_imports_to_schema():
    element = MessageElement(location=location, name="Message")
    file = ProtoFileElement(location=location, imports=[TypeName("example.other")], types=[element])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |import "example.other";
        |
        |message Message {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_add_multiple_dependencies():
    element = MessageElement(location=location, name="Message")
    file = ProtoFileElement(
        location=location, imports=[TypeName("example.other"), TypeName("example.another")], types=[element]
    )
    assert len(file.imports) == 2


def test_simple_with_public_imports_to_schema():
    element = MessageElement(location=location, name="Message")
    file = ProtoFileElement(location=location, public_imports=[TypeName("example.other")], types=[element])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |import public "example.other";
        |
        |message Message {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_add_multiple_public_dependencies():
    element = MessageElement(location=location, name="Message")
    file = ProtoFileElement(
        location=location, public_imports=[TypeName("example.other"), TypeName("example.another")], types=[element]
    )

    assert len(file.public_imports) == 2


def test_simple_with_both_imports_to_schema():
    element = MessageElement(location=location, name="Message")
    file = ProtoFileElement(
        location=location, imports=[TypeName("example.thing")], public_imports=[TypeName("example.other")], types=[element]
    )
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |import "example.thing";
        |import public "example.other";
        |
        |message Message {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_simple_with_services_to_schema():
    element = MessageElement(location=location, name="Message")
    service = ServiceElement(location=location, name="Service")
    file = ProtoFileElement(location=location, types=[element], services=[service])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |message Message {}
        |
        |service Service {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_add_multiple_services():
    service1 = ServiceElement(location=location, name="Service1")
    service2 = ServiceElement(location=location, name="Service2")
    file = ProtoFileElement(location=location, services=[service1, service2])
    assert len(file.services) == 2


def test_simple_with_options_to_schema():
    element = MessageElement(location=location, name="Message")
    option = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    file = ProtoFileElement(location=location, options=[option], types=[element])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |option kit = "kat";
        |
        |message Message {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_add_multiple_options():
    element = MessageElement(location=location, name="Message")
    kit_kat = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    foo_bar = OptionElement("foo", OptionElement.Kind.STRING, "bar")
    file = ProtoFileElement(location=location, options=[kit_kat, foo_bar], types=[element])
    assert len(file.options) == 2


def test_simple_with_extends_to_schema():
    file = ProtoFileElement(
        location=location,
        extend_declarations=[ExtendElement(location=location.at(5, 1), name="Extend")],
        types=[MessageElement(location=location, name="Message")],
    )
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |message Message {}
        |
        |extend Extend {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_add_multiple_extends():
    extend1 = ExtendElement(location=location, name="Extend1")
    extend2 = ExtendElement(location=location, name="Extend2")
    file = ProtoFileElement(location=location, extend_declarations=[extend1, extend2])
    assert len(file.extend_declarations) == 2


def test_multiple_everything_to_schema():
    element1 = MessageElement(location=location.at(12, 1), name="Message1")
    element2 = MessageElement(location=location.at(14, 1), name="Message2")
    extend1 = ExtendElement(location=location.at(16, 1), name="Extend1")
    extend2 = ExtendElement(location=location.at(18, 1), name="Extend2")
    option1 = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    option2 = OptionElement("foo", OptionElement.Kind.STRING, "bar")
    service1 = ServiceElement(location=location.at(20, 1), name="Service1")
    service2 = ServiceElement(location=location.at(22, 1), name="Service2")
    file = ProtoFileElement(
        location=location,
        package_name=PackageName("example.simple"),
        imports=[TypeName("example.thing")],
        public_imports=[TypeName("example.other")],
        types=[element1, element2],
        services=[service1, service2],
        extend_declarations=[extend1, extend2],
        options=[option1, option2],
    )
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |package example.simple;
        |
        |import "example.thing";
        |import public "example.other";
        |
        |option kit = "kat";
        |option foo = "bar";
        |
        |message Message1 {}
        |
        |message Message2 {}
        |
        |extend Extend1 {}
        |
        |extend Extend2 {}
        |
        |service Service1 {}
        |
        |service Service2 {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected

    # Re-parse the expected string into a ProtoFile and ensure they're equal.
    parsed = ProtoParser.parse(location, expected)
    assert parsed == file


def test_syntax_to_schema():
    element = MessageElement(location=location, name="Message")
    file = ProtoFileElement(location=location, syntax=Syntax.PROTO_2, types=[element])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |syntax = "proto2";
        |
        |message Message {}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected


def test_default_is_set_in_proto2():
    field = FieldElement(
        location=location.at(12, 3),
        label=Field.Label.REQUIRED,
        element_type="string",
        name="name",
        tag=1,
        default_value="defaultValue",
    )
    message = MessageElement(location=location.at(11, 1), name="Message", fields=[field])
    file = ProtoFileElement(
        syntax=Syntax.PROTO_2,
        location=location,
        package_name=PackageName("example.simple"),
        imports=[TypeName("example.thing")],
        public_imports=[TypeName("example.other")],
        types=[message],
    )
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |syntax = "proto2";
        |
        |package example.simple;
        |
        |import "example.thing";
        |import public "example.other";
        |
        |message Message {
        |  required string name = 1 [default = "defaultValue"];
        |}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected

    # Re-parse the expected string into a ProtoFile and ensure they're equal.
    parsed = ProtoParser.parse(location, expected)
    assert parsed == file


def test_convert_packed_option_from_wire_schema_in_proto2():
    field_numeric = FieldElement(
        location=location.at(9, 3),
        label=Field.Label.REPEATED,
        element_type="int32",
        name="numeric_without_packed_option",
        tag=1,
    )
    field_numeric_packed_true = FieldElement(
        location=location.at(11, 3),
        label=Field.Label.REPEATED,
        element_type="int32",
        name="numeric_packed_true",
        tag=2,
        options=[PACKED_OPTION_ELEMENT],
    )
    el = copy.copy(PACKED_OPTION_ELEMENT)
    el.value = "false"
    field_numeric_packed_false = FieldElement(
        location=location.at(13, 3),
        label=Field.Label.REPEATED,
        element_type="int32",
        name="numeric_packed_false",
        tag=3,
        options=[el],
    )
    field_string = FieldElement(
        location=location.at(15, 3),
        label=Field.Label.REPEATED,
        element_type="string",
        name="string_without_packed_option",
        tag=4,
    )
    field_string_packed_true = FieldElement(
        location=location.at(17, 3),
        label=Field.Label.REPEATED,
        element_type="string",
        name="string_packed_true",
        tag=5,
        options=[PACKED_OPTION_ELEMENT],
    )
    field_string_packed_false = FieldElement(
        location=location.at(19, 3),
        label=Field.Label.REPEATED,
        element_type="string",
        name="string_packed_false",
        tag=6,
        options=[el],
    )

    message = MessageElement(
        location=location.at(8, 1),
        name="Message",
        fields=[
            field_numeric,
            field_numeric_packed_true,
            field_numeric_packed_false,
            field_string,
            field_string_packed_true,
            field_string_packed_false,
        ],
    )
    file = ProtoFileElement(syntax=Syntax.PROTO_2, location=location, package_name="example.simple", types=[message])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |syntax = "proto2";
        |
        |package example.simple;
        |
        |message Message {
        |  repeated int32 numeric_without_packed_option = 1;
        |
        |  repeated int32 numeric_packed_true = 2 [packed = true];
        |
        |  repeated int32 numeric_packed_false = 3 [packed = false];
        |
        |  repeated string string_without_packed_option = 4;
        |
        |  repeated string string_packed_true = 5 [packed = true];
        |
        |  repeated string string_packed_false = 6 [packed = false];
        |}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected

    # Re-parse the expected string into a ProtoFile and ensure they're equal.
    parsed = ProtoParser.parse(location, expected)
    assert parsed == file


def test_convert_packed_option_from_wire_schema_in_proto3():
    field_numeric = FieldElement(
        location=location.at(9, 3),
        label=Field.Label.REPEATED,
        element_type="int32",
        name="numeric_without_packed_option",
        tag=1,
    )
    field_numeric_packed_true = FieldElement(
        location=location.at(11, 3),
        label=Field.Label.REPEATED,
        element_type="int32",
        name="numeric_packed_true",
        tag=2,
        options=[PACKED_OPTION_ELEMENT],
    )
    el = copy.copy(PACKED_OPTION_ELEMENT)
    el.value = "false"
    field_numeric_packed_false = FieldElement(
        location=location.at(13, 3),
        label=Field.Label.REPEATED,
        element_type="int32",
        name="numeric_packed_false",
        tag=3,
        options=[el],
    )
    field_string = FieldElement(
        location=location.at(15, 3),
        label=Field.Label.REPEATED,
        element_type="string",
        name="string_without_packed_option",
        tag=4,
    )
    field_string_packed_true = FieldElement(
        location=location.at(17, 3),
        label=Field.Label.REPEATED,
        element_type="string",
        name="string_packed_true",
        tag=5,
        options=[PACKED_OPTION_ELEMENT],
    )

    field_string_packed_false = FieldElement(
        location=location.at(19, 3),
        label=Field.Label.REPEATED,
        element_type="string",
        name="string_packed_false",
        tag=6,
        options=[el],
    )

    message = MessageElement(
        location=location.at(8, 1),
        name="Message",
        fields=[
            field_numeric,
            field_numeric_packed_true,
            field_numeric_packed_false,
            field_string,
            field_string_packed_true,
            field_string_packed_false,
        ],
    )
    file = ProtoFileElement(syntax=Syntax.PROTO_3, location=location, package_name="example.simple", types=[message])
    expected = """
        |// Proto schema formatted by Wire, do not edit.
        |// Source: file.proto
        |
        |syntax = "proto3";
        |
        |package example.simple;
        |
        |message Message {
        |  repeated int32 numeric_without_packed_option = 1;
        |
        |  repeated int32 numeric_packed_true = 2 [packed = true];
        |
        |  repeated int32 numeric_packed_false = 3 [packed = false];
        |
        |  repeated string string_without_packed_option = 4;
        |
        |  repeated string string_packed_true = 5 [packed = true];
        |
        |  repeated string string_packed_false = 6 [packed = false];
        |}
        |"""
    expected = trim_margin(expected)
    assert file.to_schema() == expected

    # Re-parse the expected string into a ProtoFile and ensure they're equal.
    parsed = ProtoParser.parse(location, expected)
    assert parsed == file
