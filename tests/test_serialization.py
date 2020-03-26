import avro
import copy
import io
import json
import karapace.serialization
import pytest
import struct


def test_happy_flow(default_config_path, mock_registry_client):
    serializer = karapace.serialization.SchemaRegistryValueSerializer(
        config_path=default_config_path, registry_client=mock_registry_client
    )
    deserializer = karapace.serialization.SchemaRegistryValueDeserializer(
        config_path=default_config_path, registry_client=mock_registry_client
    )
    objects = [
        {
            "name": "First Foo",
            "favorite_number": 2,
            "favorite_color": "bar"
        },
        {
            "name": "Second Foo",
            "favorite_number": 3,
            "favorite_color": "baz"
        },
        {
            "name": "Third Foo",
            "favorite_number": 5,
            "favorite_color": "quux"
        },
    ]
    for o in serializer, deserializer:
        assert len(o.ids_to_schemas) == 0
        assert len(o.subjects_to_schemas) == 0
    for o in objects:
        assert o == deserializer.deserialize("top", serializer.serialize("top", o))
    serializer.registry_client.get_latest_schema.assert_called_with("top-value")
    for o in serializer, deserializer:
        assert len(o.ids_to_schemas) == 1
        assert 1 in o.ids_to_schemas
        assert len(o.subjects_to_schemas) == 1
        assert "top-value" in o.subjects_to_schemas


def test_config_load(schemas_config_path, mock_registry_client):
    serializer = karapace.serialization.SchemaRegistryValueSerializer(
        config_path=schemas_config_path, registry_client=mock_registry_client
    )
    assert len(serializer.ids_to_schemas) == 1
    # 0 for keys
    deserializer = karapace.serialization.SchemaRegistryKeyDeserializer(
        config_path=schemas_config_path, registry_client=mock_registry_client
    )
    assert len(deserializer.ids_to_schemas) == 0


def test_serialization_fails(default_config_path, mock_registry_client):
    serializer = karapace.serialization.SchemaRegistryValueSerializer(
        config_path=default_config_path, registry_client=mock_registry_client
    )
    with pytest.raises(karapace.serialization.InvalidMessage):
        serializer.serialize("topic", {"foo": "bar"})


def test_deserialization_fails(default_config_path, mock_registry_client):
    deserializer = karapace.serialization.SchemaRegistryValueDeserializer(
        config_path=default_config_path, registry_client=mock_registry_client
    )

    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(karapace.serialization.InvalidMessageHeader):
        deserializer.deserialize("topic", invalid_header_payload)

    # for now we ignore the packed in schema id
    invalid_data_payload = struct.pack(">bII", karapace.serialization.START_BYTE, 1, 500)
    with pytest.raises(karapace.serialization.InvalidPayload):
        deserializer.deserialize("topic", invalid_data_payload)

    # but we can pass in a perfectly fine doc belonging to a diff schema
    schema = copy.deepcopy(mock_registry_client.get_schema_for_id(1).to_json())
    schema["name"] = "BadUser"
    schema["fields"][0]["type"] = ["int", "null"]
    obj = {"name": 100, "favorite_number": 2, "favorite_color": "bar"}
    writer = avro.io.DatumWriter(avro.io.schema.parse(json.dumps(schema)))
    with io.BytesIO() as bio:
        enc = avro.io.BinaryEncoder(bio)
        bio.write(struct.pack(karapace.serialization.HEADER_FORMAT, karapace.serialization.START_BYTE, 1))
        writer.write(obj, enc)
        enc_bytes = bio.getvalue()
    with pytest.raises(karapace.serialization.InvalidPayload):
        deserializer.deserialize("topic", enc_bytes)
