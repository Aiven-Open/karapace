from karapace.schema_reader import SchemaType, TypedSchema
from karapace.serialization import (
    HEADER_FORMAT, InvalidMessageHeader, InvalidMessageSchema, InvalidPayload, SchemaRegistryClient,
    SchemaRegistryDeserializer, SchemaRegistrySerializer, START_BYTE
)
from tests.utils import schema_avro_json, test_objects_avro

import avro
import copy
import io
import json
import pytest
import struct

pytest_plugins = "aiohttp.pytest_plugin"


async def make_ser_deser(config_path, mock_client):
    serializer = SchemaRegistrySerializer(config_path=config_path)
    deserializer = SchemaRegistryDeserializer(config_path=config_path)
    await serializer.registry_client.close()
    await deserializer.registry_client.close()
    serializer.registry_client = mock_client
    deserializer.registry_client = mock_client
    return serializer, deserializer


async def test_happy_flow(default_config_path, mock_registry_client):
    serializer, deserializer = await make_ser_deser(default_config_path, mock_registry_client)
    for o in serializer, deserializer:
        assert len(o.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject("top")
    for o in test_objects_avro:
        assert o == await deserializer.deserialize(await serializer.serialize(schema, o))
    for o in serializer, deserializer:
        assert len(o.ids_to_schemas) == 1
        assert 1 in o.ids_to_schemas


async def test_serialization_fails(default_config_path, mock_registry_client):
    serializer, _ = await make_ser_deser(default_config_path, mock_registry_client)
    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject("topic")
        await serializer.serialize(schema, {"foo": "bar"})


async def test_deserialization_fails(default_config_path, mock_registry_client):
    _, deserializer = await make_ser_deser(default_config_path, mock_registry_client)
    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(InvalidMessageHeader):
        await deserializer.deserialize(invalid_header_payload)

    # for now we ignore the packed in schema id
    invalid_data_payload = struct.pack(">bII", START_BYTE, 1, 500)
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(invalid_data_payload)

    # but we can pass in a perfectly fine doc belonging to a diff schema
    schema = await mock_registry_client.get_schema_for_id(1)
    schema = copy.deepcopy(schema.to_json())
    schema["name"] = "BadUser"
    schema["fields"][0]["type"] = "int"
    obj = {"name": 100, "favorite_number": 2, "favorite_color": "bar"}
    writer = avro.io.DatumWriter(avro.io.schema.parse(json.dumps(schema)))
    with io.BytesIO() as bio:
        enc = avro.io.BinaryEncoder(bio)
        bio.write(struct.pack(HEADER_FORMAT, START_BYTE, 1))
        writer.write(obj, enc)
        enc_bytes = bio.getvalue()
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(enc_bytes)


async def test_remote_client(registry_async_client):
    schema_avro = TypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client
    sc_id = await reg_cli.post_new_schema("foo", schema_avro)
    assert sc_id >= 0
    stored_schema = await reg_cli.get_schema_for_id(sc_id)
    assert stored_schema == schema_avro, f"stored schema {stored_schema.to_json()} is not {schema_avro.to_json()}"
    stored_id, stored_schema = await reg_cli.get_latest_schema("foo")
    assert stored_id == sc_id
    assert stored_schema == schema_avro
