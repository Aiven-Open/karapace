"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.config import read_config
from karapace.dependency import Dependency
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.schema_models import ParsedTypedSchema, SchemaType
from karapace.schema_references import Reference
from karapace.serialization import (
    InvalidMessageHeader,
    InvalidMessageSchema,
    InvalidPayload,
    SchemaRegistrySerializer,
    START_BYTE,
)
from karapace.typing import ResolvedVersion, Subject
from tests.utils import schema_protobuf, test_fail_objects_protobuf, test_objects_protobuf
from unittest.mock import call, Mock

import asyncio
import logging
import pytest
import struct

log = logging.getLogger(__name__)


async def make_ser_deser(config_path: str, mock_client) -> SchemaRegistrySerializer:
    with open(config_path, encoding="utf8") as handler:
        config = read_config(handler)
    serializer = SchemaRegistrySerializer(config=config)
    await serializer.registry_client.close()
    serializer.registry_client = mock_client
    return serializer


async def test_happy_flow(default_config_path):
    mock_protobuf_registry_client = Mock()
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result(
        (ParsedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf)), [Subject("stub")])
    )
    mock_protobuf_registry_client.get_schema_for_id.return_value = schema_for_id_one_future
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result(
        (1, ParsedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf)), ResolvedVersion(1))
    )
    mock_protobuf_registry_client.get_schema.return_value = get_latest_schema_future

    serializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    assert len(serializer.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject("top")
    for o in test_objects_protobuf:
        a = await serializer.serialize(schema, o)
        u = await serializer.deserialize(a)
        assert o == u
    assert len(serializer.ids_to_schemas) == 1
    assert 1 in serializer.ids_to_schemas

    assert mock_protobuf_registry_client.method_calls == [call.get_schema("top"), call.get_schema_for_id(1)]


async def test_happy_flow_references(default_config_path):
    no_ref_schema_str = """
    |syntax = "proto3";
    |
    |option java_package = "com.codingharbour.protobuf";
    |option java_outer_classname = "TestEnumOrder";
    |
    |message Speed {
    |  Enum speed = 1;
    |}
    |
    |enum Enum {
    |  HIGH = 0;
    |  MIDDLE = 1;
    |  LOW = 2;
    |}
    |
    """

    ref_schema_str = """
    |syntax = "proto3";
    |
    |option java_package = "com.codingharbour.protobuf";
    |option java_outer_classname = "TestEnumOrder";
    |import "Speed.proto";
    |
    |message Message {
    |  int32 query = 1;
    |  Speed speed = 2;
    |}
    |
    |
    """
    no_ref_schema_str = trim_margin(no_ref_schema_str)
    ref_schema_str = trim_margin(ref_schema_str)

    test_objects = [
        {"query": 5, "speed": {"speed": "HIGH"}},
        {"query": 10, "speed": {"speed": "MIDDLE"}},
    ]

    references = [Reference(name="Speed.proto", subject="speed", version=1)]

    no_ref_schema = ParsedTypedSchema.parse(SchemaType.PROTOBUF, no_ref_schema_str)
    dep = Dependency("Speed.proto", "speed", 1, no_ref_schema)
    ref_schema = ParsedTypedSchema.parse(SchemaType.PROTOBUF, ref_schema_str, references, {"Speed.proto": dep})

    mock_protobuf_registry_client = Mock()
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ref_schema, [Subject("stub")]))
    mock_protobuf_registry_client.get_schema_for_id.return_value = schema_for_id_one_future
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, ref_schema, ResolvedVersion(1)))
    mock_protobuf_registry_client.get_schema.return_value = get_latest_schema_future

    serializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    assert len(serializer.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject("top")
    for o in test_objects:
        a = await serializer.serialize(schema, o)
        u = await serializer.deserialize(a)
        assert o == u
    assert len(serializer.ids_to_schemas) == 1
    assert 1 in serializer.ids_to_schemas

    assert mock_protobuf_registry_client.method_calls == [call.get_schema("top"), call.get_schema_for_id(1)]


async def test_happy_flow_references_two(default_config_path):
    no_ref_schema_str = """
    |syntax = "proto3";
    |
    |option java_package = "com.serge.protobuf";
    |option java_outer_classname = "TestSpeed";
    |
    |message Speed {
    |  Enum speed = 1;
    |}
    |
    |enum Enum {
    |  HIGH = 0;
    |  MIDDLE = 1;
    |  LOW = 2;
    |}
    |
    """

    ref_schema_str = """
    |syntax = "proto3";
    |
    |option java_package = "com.serge.protobuf";
    |option java_outer_classname = "TestQuery";
    |import "Speed.proto";
    |
    |message Query {
    |  int32 query = 1;
    |  Speed speed = 2;
    |}
    |
    """

    ref_schema_str_two = """
    |syntax = "proto3";
    |
    |option java_package = "com.serge.protobuf";
    |option java_outer_classname = "TestMessage";
    |import "Query.proto";
    |
    |message Message {
    |  int32 index = 1;
    |  Query qry = 2;
    |}
    |
    """

    no_ref_schema_str = trim_margin(no_ref_schema_str)
    ref_schema_str = trim_margin(ref_schema_str)
    ref_schema_str_two = trim_margin(ref_schema_str_two)
    test_objects = [
        {"index": 1, "qry": {"query": 5, "speed": {"speed": "HIGH"}}},
        {"index": 2, "qry": {"query": 10, "speed": {"speed": "HIGH"}}},
    ]

    references = [Reference(name="Speed.proto", subject="speed", version=1)]
    references_two = [Reference(name="Query.proto", subject="msg", version=1)]

    no_ref_schema = ParsedTypedSchema.parse(SchemaType.PROTOBUF, no_ref_schema_str)
    dep = Dependency("Speed.proto", "speed", 1, no_ref_schema)
    ref_schema = ParsedTypedSchema.parse(SchemaType.PROTOBUF, ref_schema_str, references, {"Speed.proto": dep})
    dep_two = Dependency("Query.proto", "qry", 1, ref_schema)
    ref_schema_two = ParsedTypedSchema.parse(
        SchemaType.PROTOBUF, ref_schema_str_two, references_two, {"Query.proto": dep_two}
    )

    mock_protobuf_registry_client = Mock()
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ref_schema_two, [Subject("mock")]))
    mock_protobuf_registry_client.get_schema_for_id.return_value = schema_for_id_one_future
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, ref_schema_two, ResolvedVersion(1)))
    mock_protobuf_registry_client.get_schema.return_value = get_latest_schema_future

    serializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    assert len(serializer.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject("top")
    for o in test_objects:
        a = await serializer.serialize(schema, o)
        u = await serializer.deserialize(a)
        assert o == u
    assert len(serializer.ids_to_schemas) == 1
    assert 1 in serializer.ids_to_schemas

    assert mock_protobuf_registry_client.method_calls == [call.get_schema("top"), call.get_schema_for_id(1)]


async def test_serialization_fails(default_config_path):
    mock_protobuf_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result(
        (1, ParsedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf)), ResolvedVersion(1))
    )
    mock_protobuf_registry_client.get_schema.return_value = get_latest_schema_future

    serializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject("top")
        await serializer.serialize(schema, test_fail_objects_protobuf[0])

    assert mock_protobuf_registry_client.method_calls == [call.get_schema("top")]
    mock_protobuf_registry_client.reset_mock()

    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject("top")
        await serializer.serialize(schema, test_fail_objects_protobuf[1])

    assert mock_protobuf_registry_client.method_calls == [call.get_schema("top")]


async def test_deserialization_fails(default_config_path):
    mock_protobuf_registry_client = Mock()

    deserializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(InvalidMessageHeader):
        await deserializer.deserialize(invalid_header_payload)

    assert mock_protobuf_registry_client.method_calls == []
    mock_protobuf_registry_client.reset_mock()

    # wrong schema id (500)
    invalid_data_payload = struct.pack(">bII", START_BYTE, 500, 500)
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(invalid_data_payload)

    assert mock_protobuf_registry_client.method_calls == [call.get_schema_for_id(500)]


async def test_deserialization_fails2(default_config_path):
    mock_protobuf_registry_client = Mock()

    deserializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(InvalidMessageHeader):
        await deserializer.deserialize(invalid_header_payload)

    assert mock_protobuf_registry_client.method_calls == []
    mock_protobuf_registry_client.reset_mock()

    enc_bytes = b"\x00\x00\x00\x00\x01\x00\x02\x05\0x12"  # wrong schema data (2)
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(enc_bytes)

    assert mock_protobuf_registry_client.method_calls == [call.get_schema_for_id(1)]
