"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/jvmTest/kotlin/com/squareup/wire/schema/internal/parser/ServiceElementTest.kt

from karapace.core.protobuf.kotlin_wrapper import trim_margin
from karapace.core.protobuf.location import Location
from karapace.core.protobuf.option_element import OptionElement
from karapace.core.protobuf.rpc_element import RpcElement
from karapace.core.protobuf.service_element import ServiceElement

location: Location = Location("", "file.proto")


def test_empty_to_schema():
    service = ServiceElement(location=location, name="Service")
    expected = "service Service {}\n"
    assert service.to_schema() == expected


def test_single_to_schema():
    service = ServiceElement(
        location=location,
        name="Service",
        rpcs=[RpcElement(location=location, name="Name", request_type="RequestType", response_type="ResponseType")],
    )
    expected = """
        |service Service {
        |  rpc Name (RequestType) returns (ResponseType);
        |}
        |"""
    expected = trim_margin(expected)
    assert service.to_schema() == expected


def test_add_multiple_rpcs():
    first_name = RpcElement(location=location, name="FirstName", request_type="RequestType", response_type="ResponseType")
    last_name = RpcElement(location=location, name="LastName", request_type="RequestType", response_type="ResponseType")
    service = ServiceElement(location=location, name="Service", rpcs=[first_name, last_name])
    assert len(service.rpcs) == 2


def test_single_with_options_to_schema():
    service = ServiceElement(
        location=location,
        name="Service",
        options=[OptionElement("foo", OptionElement.Kind.STRING, "bar")],
        rpcs=[RpcElement(location=location, name="Name", request_type="RequestType", response_type="ResponseType")],
    )
    expected = """
        |service Service {
        |  option foo = "bar";
        |
        |  rpc Name (RequestType) returns (ResponseType);
        |}
        |"""
    expected = trim_margin(expected)
    assert service.to_schema() == expected


def test_add_multiple_options():
    kit_kat = OptionElement("kit", OptionElement.Kind.STRING, "kat")
    foo_bar = OptionElement("foo", OptionElement.Kind.STRING, "bar")
    service = ServiceElement(
        location=location,
        name="Service",
        options=[kit_kat, foo_bar],
        rpcs=[RpcElement(location=location, name="Name", request_type="RequestType", response_type="ResponseType")],
    )
    assert len(service.options) == 2


def test_single_with_documentation_to_schema():
    service = ServiceElement(
        location=location,
        name="Service",
        documentation="Hello",
        rpcs=[RpcElement(location=location, name="Name", request_type="RequestType", response_type="ResponseType")],
    )
    expected = """
        |// Hello
        |service Service {
        |  rpc Name (RequestType) returns (ResponseType);
        |}
        |"""
    expected = trim_margin(expected)
    assert service.to_schema() == expected


def test_multiple_to_schema():
    rpc = RpcElement(location=location, name="Name", request_type="RequestType", response_type="ResponseType")
    service = ServiceElement(location=location, name="Service", rpcs=[rpc, rpc])
    expected = """
        |service Service {
        |  rpc Name (RequestType) returns (ResponseType);
        |  rpc Name (RequestType) returns (ResponseType);
        |}
        |"""
    expected = trim_margin(expected)

    assert service.to_schema() == expected


def test_rpc_to_schema():
    rpc = RpcElement(location=location, name="Name", request_type="RequestType", response_type="ResponseType")
    expected = "rpc Name (RequestType) returns (ResponseType);\n"
    assert rpc.to_schema() == expected


def test_rpc_with_documentation_to_schema():
    rpc = RpcElement(
        location=location, name="Name", documentation="Hello", request_type="RequestType", response_type="ResponseType"
    )
    expected = """
        |// Hello
        |rpc Name (RequestType) returns (ResponseType);
        |"""
    expected = trim_margin(expected)
    assert rpc.to_schema() == expected


def test_rpc_with_options_to_schema():
    rpc = RpcElement(
        location=location,
        name="Name",
        request_type="RequestType",
        response_type="ResponseType",
        options=[OptionElement("foo", OptionElement.Kind.STRING, "bar")],
    )

    expected = """
        |rpc Name (RequestType) returns (ResponseType) {
        |  option foo = "bar";
        |};
        |"""
    expected = trim_margin(expected)
    assert rpc.to_schema() == expected


def test_rpc_with_request_streaming_to_schema():
    rpc = RpcElement(
        location=location, name="Name", request_type="RequestType", response_type="ResponseType", request_streaming=True
    )
    expected = "rpc Name (stream RequestType) returns (ResponseType);\n"
    assert rpc.to_schema() == expected


def test_rpc_with_response_streaming_to_schema():
    rpc = RpcElement(
        location=location, name="Name", request_type="RequestType", response_type="ResponseType", response_streaming=True
    )
    expected = "rpc Name (RequestType) returns (stream ResponseType);\n"
    assert rpc.to_schema() == expected
