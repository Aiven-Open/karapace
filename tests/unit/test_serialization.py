from karapace.config import DEFAULTS, read_config
from karapace.schema_models import SchemaType, ValidatedTypedSchema
from karapace.serialization import (
    flatten_unions,
    HEADER_FORMAT,
    InvalidMessageHeader,
    InvalidMessageSchema,
    InvalidPayload,
    SchemaRegistryDeserializer,
    SchemaRegistrySerializer,
    START_BYTE,
    write_value,
)
from tests.utils import test_objects_avro

import avro
import copy
import io
import logging
import pytest
import struct
import ujson

log = logging.getLogger(__name__)


async def make_ser_deser(config_path, mock_client):
    with open(config_path, encoding="utf8") as handler:
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


def test_flatten_unions_record() -> None:
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        ujson.dumps(
            {
                "namespace": "io.aiven.data",
                "name": "Test",
                "type": "record",
                "fields": [
                    {
                        "name": "attr1",
                        "type": ["null", "string"],
                    },
                    {
                        "name": "attr2",
                        "type": ["null", "string"],
                    },
                ],
            }
        ),
    )
    record = {"attr1": {"string": "sample data"}, "attr2": None}
    flatten_record = {"attr1": "sample data", "attr2": None}
    assert flatten_unions(typed_schema.schema, record) == flatten_record

    record = {"attr1": None, "attr2": None}
    assert flatten_unions(typed_schema.schema, record) == record


def test_flatten_unions_array() -> None:
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        ujson.dumps(
            {
                "type": "array",
                "items": {
                    "namespace": "io.aiven.data",
                    "name": "Test",
                    "type": "record",
                    "fields": [
                        {
                            "name": "attr",
                            "type": ["null", "string"],
                        }
                    ],
                },
            }
        ),
    )
    record = [{"attr": {"string": "sample data"}}]
    flatten_record = [{"attr": "sample data"}]
    assert flatten_unions(typed_schema.schema, record) == flatten_record

    record = [{"attr": None}]
    assert flatten_unions(typed_schema.schema, record) == record


def test_flatten_unions_map() -> None:
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        ujson.dumps(
            {
                "type": "map",
                "values": {
                    "namespace": "io.aiven.data",
                    "name": "Test",
                    "type": "record",
                    "fields": [
                        {
                            "name": "attr1",
                            "type": ["null", "string"],
                        }
                    ],
                },
            }
        ),
    )
    record = {"foo": {"attr1": {"string": "sample data"}}}
    flatten_record = {"foo": {"attr1": "sample data"}}
    assert flatten_unions(typed_schema.schema, record) == flatten_record

    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        ujson.dumps({"type": "array", "items": ["null", "string", "int"]}),
    )
    record = [{"string": "foo"}, None, {"int": 1}]
    flatten_record = ["foo", None, 1]
    assert flatten_unions(typed_schema.schema, record) == flatten_record


def test_avro_json_write_invalid() -> None:
    schema = {
        "namespace": "io.aiven.data",
        "name": "Test",
        "type": "record",
        "fields": [
            {
                "name": "attr",
                "type": ["null", "string"],
            }
        ],
    }
    records = [
        {"attr": {"string": 5}},
        {"attr": {"foo": "bar"}},
        {"foo": "bar"},
    ]

    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, ujson.dumps(schema))
    bio = io.BytesIO()

    for record in records:
        with pytest.raises(avro.errors.AvroTypeException):
            write_value(DEFAULTS, typed_schema, bio, record)


def test_avro_json_write_accepts_json_encoded_data_without_tagged_unions() -> None:
    """Backwards compatibility test for Avro data using JSON encoding.

    The initial behavior of the API was incorrect, and it accept data with
    invalid encoding for union types.

    Given this schema:

        {
          "namespace": "io.aiven.data",
          "name": "Test",
          "type": "record",
          "fields": [
            {"name": "attr", "type": ["null", "string"]}
          ]
        }

    The correct JSON encoding for the `attr` field is:

        {"attr":{"string":"sample data"}}

    However, because of the lack of a parser for Avro data JSON-encoded, the
    following was accepted by the server (note the missing tag):

        {"attr":"sample data"}

    This tests the broken behavior is still supported for backwards
    compatibility.
    """

    # Regression test: The same value must be used as the record name and one
    # of the record fields. An initial iteration of write_value would always
    # call flatten_unions, which broker backwards compatibility by corrupting
    # the old format (i.e. the missing_tag_encoding_a value below should be
    # kept unadulterated).
    duplicated_name = "somename"

    schema = {
        "namespace": "io.aiven.data",
        "name": "Test",
        "type": "record",
        "fields": [
            {
                "name": "outter",
                "type": [
                    {"type": "record", "name": duplicated_name, "fields": [{"name": duplicated_name, "type": "string"}]},
                    "int",
                ],
            }
        ],
    }
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, ujson.dumps(schema))

    properly_tagged_encoding_a = {"outter": {duplicated_name: {duplicated_name: "data"}}}
    properly_tagged_encoding_b = {"outter": {"int": 1}}
    missing_tag_encoding_a = {"outter": {duplicated_name: "data"}}
    missing_tag_encoding_b = {"outter": 1}

    buffer_a = io.BytesIO()
    buffer_b = io.BytesIO()
    write_value(DEFAULTS, typed_schema, buffer_a, properly_tagged_encoding_a)
    write_value(DEFAULTS, typed_schema, buffer_b, missing_tag_encoding_a)
    assert buffer_a.getbuffer() == buffer_b.getbuffer()

    buffer_a = io.BytesIO()
    buffer_b = io.BytesIO()
    write_value(DEFAULTS, typed_schema, buffer_a, properly_tagged_encoding_b)
    write_value(DEFAULTS, typed_schema, buffer_b, missing_tag_encoding_b)
    assert buffer_a.getbuffer() == buffer_b.getbuffer()


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
    schema = copy.deepcopy(schema.to_dict())
    schema["name"] = "BadUser"
    schema["fields"][0]["type"] = "int"
    obj = {"name": 100, "favorite_number": 2, "favorite_color": "bar"}
    writer = avro.io.DatumWriter(avro.schema.make_avsc_object(schema))
    with io.BytesIO() as bio:
        enc = avro.io.BinaryEncoder(bio)
        bio.write(struct.pack(HEADER_FORMAT, START_BYTE, 1))
        writer.write(obj, enc)
        enc_bytes = bio.getvalue()
    # Avro 1.11.0 does not assert anymore if the bytes io read function
    # gives back the number of bytes expected. The invalid Avro record
    # read on following manner:
    #  * expected field is name and read as bytes
    #  * read long to indicate how many bytes are in the string = 100
    #  * 100 bytes is read from bytes io, returns 4 (b'\x04\x06bar')
    #  * bytes io position is at the end of the byte buffer
    #  * expected field is favorite number and is read as single int/long
    #  * bytes buffer is at the end and returns zero data
    #  * Avro calls `ord` with zero data and TypeError is raised.
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(enc_bytes)
