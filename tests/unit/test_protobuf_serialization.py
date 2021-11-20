from karapace.config import read_config
from karapace.serialization import SchemaRegistryDeserializer, SchemaRegistrySerializer
from tests.utils import test_objects_protobuf

import logging

log = logging.getLogger(__name__)


async def make_ser_deser(config_path, mock_client):
    with open(config_path) as handler:
        config = read_config(handler)
    serializer = SchemaRegistrySerializer(config_path=config_path, config=config)
    deserializer = SchemaRegistryDeserializer(config_path=config_path, config=config)
    await serializer.registry_client.close()
    await deserializer.registry_client.close()
    serializer.registry_client = mock_client
    deserializer.registry_client = mock_client
    return serializer, deserializer


async def test_happy_flow(default_config_path, mock_protobuf_registry_client):
    serializer, deserializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
    for o in serializer, deserializer:
        assert len(o.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject("top")
    for o in test_objects_protobuf:
        a = await serializer.serialize(schema, o)
        u = await deserializer.deserialize(a)
        assert o == u
    for o in serializer, deserializer:
        assert len(o.ids_to_schemas) == 1
        assert 1 in o.ids_to_schemas


# async def test_serialization_fails(default_config_path, mock_protobuf_registry_client):
#    serializer, _ = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
#    with pytest.raises(InvalidMessageSchema):
#        schema = await serializer.get_schema_for_subject("topic")
#        await serializer.serialize(schema, {"foo": "bar"})
#
#
# async def test_deserialization_fails(default_config_path, mock_protobuf_registry_client):
#    _, deserializer = await make_ser_deser(default_config_path, mock_protobuf_registry_client)
#    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
#    with pytest.raises(InvalidMessageHeader):
#        await deserializer.deserialize(invalid_header_payload)
#
#    # for now we ignore the packed in schema id
#    invalid_data_payload = struct.pack(">bII", START_BYTE, 1, 500)
#    with pytest.raises(InvalidPayload):
#        await deserializer.deserialize(invalid_data_payload)
#
#    # but we can pass in a perfectly fine doc belonging to a diff schema
#    schema = await mock_protobuf_registry_client.get_schema_for_id(1)
#    schema = copy.deepcopy(schema.to_json())
#    schema["name"] = "BadUser"
#    schema["fields"][0]["type"] = "int"
#    obj = {"name": 100, "favorite_number": 2, "favorite_color": "bar"}
#    writer = avro.io.DatumWriter(avro.io.schema.parse(json.dumps(schema)))
#    with io.BytesIO() as bio:
#        enc = avro.io.BinaryEncoder(bio)
#        bio.write(struct.pack(HEADER_FORMAT, START_BYTE, 1))
#        writer.write(obj, enc)
#        enc_bytes = bio.getvalue()
#    with pytest.raises(InvalidPayload):
#        await deserializer.deserialize(enc_bytes)
#
