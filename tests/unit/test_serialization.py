from karapace.config import read_config
from karapace.schema_reader import SchemaType, TypedSchema
from karapace.serialization import (
    HEADER_FORMAT, InvalidMessageHeader, InvalidMessageSchema, InvalidPayload, read_value, SchemaRegistryDeserializer,
    SchemaRegistrySerializer, START_BYTE, write_value
)
from tests.utils import test_objects_avro

import avro
import copy
import io
import json
import pytest
import struct


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


def assert_avro_json_round_trip(schema, record):
    typed_schema = TypedSchema.parse(SchemaType.AVRO, json.dumps(schema))
    bio = io.BytesIO()

    write_value(typed_schema, bio, record)
    assert record == read_value(typed_schema, io.BytesIO(bio.getvalue()))


def test_avro_json_round_trip():
    schema = {
        "namespace": "io.aiven.data",
        "name": "Test",
        "type": "record",
        "fields": [{
            "name": "attr1",
            "type": ["null", "string"],
        }, {
            "name": "attr2",
            "type": ["null", "string"],
        }]
    }
    record = {"attr1": {"string": "sample data"}, "attr2": None}
    assert_avro_json_round_trip(schema, record)

    record = {"attr1": None, "attr2": None}
    assert_avro_json_round_trip(schema, record)

    schema = {
        "type": "array",
        "items": {
            "namespace": "io.aiven.data",
            "name": "Test",
            "type": "record",
            "fields": [{
                "name": "attr",
                "type": ["null", "string"],
            }]
        }
    }
    record = [{"attr": {"string": "sample data"}}]
    assert_avro_json_round_trip(schema, record)
    record = [{"attr": None}]
    assert_avro_json_round_trip(schema, record)

    schema = {
        "type": "map",
        "values": {
            "namespace": "io.aiven.data",
            "name": "Test",
            "type": "record",
            "fields": [{
                "name": "attr1",
                "type": ["null", "string"],
            }]
        }
    }
    record = {"foo": {"attr1": {"string": "sample data"}}}
    assert_avro_json_round_trip(schema, record)

    schema = {"type": "array", "items": ["null", "string", "int"]}
    record = [{"string": "foo"}, None, {"int": 1}]
    assert_avro_json_round_trip(schema, record)


def assert_avro_json_write_invalid(schema, records):
    typed_schema = TypedSchema.parse(SchemaType.AVRO, json.dumps(schema))
    bio = io.BytesIO()

    for record in records:
        with pytest.raises(InvalidMessageSchema):
            write_value(typed_schema, bio, record)


def test_avro_json_write_invalid():
    schema = {
        "namespace": "io.aiven.data",
        "name": "Test",
        "type": "record",
        "fields": [{
            "name": "attr",
            "type": ["null", "string"],
        }]
    }
    records = [
        {
            "attr": {
                "string": 5
            }
        },
        {
            "attr": {
                "foo": "bar"
            }
        },
        {
            "foo": "bar"
        },
    ]
    assert_avro_json_write_invalid(schema, records)

    schema = {"type": "array", "items": ["string", "int"]}
    records = [
        [{
            "string": 1
        }],
        [1, 2],
        [{
            "string": 1
        }, 1, "foo"],
    ]
    assert_avro_json_write_invalid(schema, records)


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
