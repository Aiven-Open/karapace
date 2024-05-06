"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Path
from karapace.config import DEFAULTS, read_config
from karapace.schema_models import SchemaType, ValidatedTypedSchema
from karapace.serialization import (
    flatten_unions,
    get_subject_name,
    HEADER_FORMAT,
    InvalidMessageHeader,
    InvalidMessageSchema,
    InvalidPayload,
    SchemaRegistrySerializer,
    START_BYTE,
    write_value,
)
from karapace.typing import NameStrategy, ResolvedVersion, Subject, SubjectType
from tests.utils import schema_avro_json, test_objects_avro
from unittest.mock import call, Mock

import asyncio
import avro
import copy
import io
import json
import logging
import pytest
import struct

log = logging.getLogger(__name__)

TYPED_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
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
                {
                    "name": "attrArray",
                    "type": ["null", {"type": "array", "items": "string"}],
                },
                {
                    "name": "attrMap",
                    "type": ["null", {"type": "map", "values": "string"}],
                },
                {
                    "name": "attrRecord",
                    "type": ["null", {"type": "record", "name": "Record", "fields": [{"name": "attr1", "type": "string"}]}],
                },
            ],
        }
    ),
)

TYPED_JSON_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.JSONSCHEMA,
    json.dumps(
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Test",
            "type": "object",
            "properties": {"attr1": {"type": ["null", "string"]}, "attr2": {"type": ["null", "string"]}},
        }
    ),
)

TYPED_AVRO_SCHEMA_WITHOUT_NAMESPACE = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
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

TYPED_PROTOBUF_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.PROTOBUF,
    """\
    syntax = "proto3";

    message Test {
        string attr1 = 1;
        string attr2 = 2;
    }\
    """,
)


async def make_ser_deser(config_path: str, mock_client) -> SchemaRegistrySerializer:
    with open(config_path, encoding="utf8") as handler:
        config = read_config(handler)
    serializer = SchemaRegistrySerializer(config=config)
    await serializer.registry_client.close()
    serializer.registry_client = mock_client
    return serializer


async def test_happy_flow(default_config_path: Path):
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result(
        (1, ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), ResolvedVersion(1))
    )
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(default_config_path, mock_registry_client)
    assert len(serializer.ids_to_schemas) == 0
    schema = await serializer.get_schema_for_subject(Subject("top"))
    for o in test_objects_avro:
        assert o == await serializer.deserialize(await serializer.serialize(schema, o))
    assert len(serializer.ids_to_schemas) == 1
    assert 1 in serializer.ids_to_schemas

    assert mock_registry_client.method_calls == [call.get_schema("top"), call.get_schema_for_id(1)]


@pytest.mark.parametrize(
    ["record", "flattened_record"],
    [
        [{"attr1": {"string": "sample data"}, "attr2": None}, {"attr1": "sample data", "attr2": None}],
        [{"attr1": None, "attr2": None}, {"attr1": None, "attr2": None}],
        [{"attrArray": {"array": ["item1", "item2"]}}, {"attrArray": ["item1", "item2"]}],
        [{"attrMap": {"map": {"k1": "v1", "k2": "v2"}}}, {"attrMap": {"k1": "v1", "k2": "v2"}}],
        [{"attrRecord": {"Record": {"attr1": "test"}}}, {"attrRecord": {"attr1": "test"}}],
    ],
)
def test_flatten_unions_record(record, flattened_record) -> None:
    assert flatten_unions(TYPED_AVRO_SCHEMA.schema, record) == flattened_record


def test_flatten_unions_array() -> None:
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        json.dumps(
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
        json.dumps(
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
        json.dumps({"type": "array", "items": ["null", "string", "int"]}),
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

    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema))
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
    typed_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, json.dumps(schema))

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


async def test_serialization_fails(default_config_path: Path):
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result(
        (1, ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), ResolvedVersion(1))
    )
    mock_registry_client.get_schema.return_value = get_latest_schema_future

    serializer = await make_ser_deser(default_config_path, mock_registry_client)
    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject(Subject("topic"))
        await serializer.serialize(schema, {"foo": "bar"})

    assert mock_registry_client.method_calls == [call.get_schema("topic")]


async def test_deserialization_fails(default_config_path: Path):
    mock_registry_client = Mock()
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    deserializer = await make_ser_deser(default_config_path, mock_registry_client)
    invalid_header_payload = struct.pack(">bII", 1, 500, 500)
    with pytest.raises(InvalidMessageHeader):
        await deserializer.deserialize(invalid_header_payload)

    # for now we ignore the packed in schema id
    invalid_data_payload = struct.pack(">bII", START_BYTE, 1, 500)
    with pytest.raises(InvalidPayload):
        await deserializer.deserialize(invalid_data_payload)

    assert mock_registry_client.method_calls == [call.get_schema_for_id(1)]
    # Reset mock, next test calls the function also.
    mock_registry_client.reset_mock()

    # but we can pass in a perfectly fine doc belonging to a diff schema
    schema, _ = await mock_registry_client.get_schema_for_id(1)
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

    assert mock_registry_client.method_calls == [call.get_schema_for_id(1)]


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("foo-key"), NameStrategy.topic_name, SubjectType.key),
        (Subject("io.aiven.data.Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-io.aiven.data.Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("foo-value"), NameStrategy.topic_name, SubjectType.value),
        (Subject("io.aiven.data.Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-io.aiven.data.Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_avro(expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType):
    assert (
        get_subject_name(topic_name="foo", schema=TYPED_AVRO_SCHEMA, subject_type=subject_type, naming_strategy=strategy)
        == expected_subject
    )


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_json_schema(expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType):
    assert (
        get_subject_name(topic_name="foo", schema=TYPED_JSON_SCHEMA, subject_type=subject_type, naming_strategy=strategy)
        == expected_subject
    )


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_avro_without_namespace(
    expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType
):
    assert (
        get_subject_name(
            topic_name="foo", schema=TYPED_AVRO_SCHEMA_WITHOUT_NAMESPACE, subject_type=subject_type, naming_strategy=strategy
        )
        == expected_subject
    )


@pytest.mark.parametrize(
    "expected_subject,strategy,subject_type",
    (
        (Subject("Test"), NameStrategy.record_name, SubjectType.key),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.key),
        (Subject("Test"), NameStrategy.record_name, SubjectType.value),
        (Subject("foo-Test"), NameStrategy.topic_record_name, SubjectType.value),
    ),
)
def test_name_strategy_for_protobuf(expected_subject: Subject, strategy: NameStrategy, subject_type: SubjectType):
    assert (
        get_subject_name(topic_name="foo", schema=TYPED_PROTOBUF_SCHEMA, subject_type=subject_type, naming_strategy=strategy)
        == expected_subject
    )
