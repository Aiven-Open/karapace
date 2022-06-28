from karapace.config import read_config
from karapace.serialization import (
    InvalidMessageHeader,
    InvalidMessageSchema,
    InvalidPayload,
    SchemaRegistrySerializer,
    START_BYTE,
)
from tests.utils import test_fail_objects_protobuf, test_objects_protobuf

import logging
import pytest
import struct

log = logging.getLogger(__name__)


async def make_ser_deser(config_path: str, mock_client) -> SchemaRegistrySerializer:
    with open(config_path, encoding="utf8") as handler:
        config = read_config(handler)
    serializer = SchemaRegistrySerializer(config_path=config_path, config=config)
    await serializer.registry_client.close()
    serializer.registry_client = mock_client
    return serializer


async def test_happy_flow(default_config_path: str, mock_protobuf_registry_client) -> None:
    serializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    assert len(serializer.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject("top")
    for o in test_objects_protobuf:
        a = await serializer.serialize(schema, o)
        u = await serializer.deserialize(a)
        assert o == u
    assert len(serializer.ids_to_schemas) == 1
    assert 1 in serializer.ids_to_schemas


async def test_serialization_fails(default_config_path: str, mock_protobuf_registry_client) -> None:
    serializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject("top")
        await serializer.serialize(schema, test_fail_objects_protobuf[0])

    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject("top")
        await serializer.serialize(schema, test_fail_objects_protobuf[1])


async def test_deserialization_fails(default_config_path: str, mock_protobuf_registry_client) -> None:
    deserializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(InvalidMessageHeader):
        await deserializer.deserialize(invalid_header_payload)

    # wrong schema id (500)
    invalid_data_payload = struct.pack(">bII", START_BYTE, 500, 500)
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(invalid_data_payload)


async def test_deserialization_fails2(default_config_path: str, mock_protobuf_registry_client) -> None:
    deserializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(InvalidMessageHeader):
        await deserializer.deserialize(invalid_header_payload)

    enc_bytes = b"\x00\x00\x00\x00\x01\x00\x02\x05\0x12"  # wrong schema data (2)
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(enc_bytes)
