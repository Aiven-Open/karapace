"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import asyncio
import base64
import copy
import decimal
import io
import json
import logging
import struct
from unittest.mock import AsyncMock, Mock, call, patch

import avro
import pytest

from karapace.core.container import KarapaceContainer
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema, Versioner
from karapace.core.serialization import (
    HEADER_FORMAT,
    START_BYTE,
    InvalidMessageHeader,
    InvalidMessageSchema,
    InvalidPayload,
    SchemaRegistryClient,
    SchemaRegistrySerializer,
    SchemaRetrievalError,
    build_decoder,
    flatten_unions,
    get_subject_name,
    sr_authorization_ctx,
    write_value,
)
from karapace.core.typing import NameStrategy, Subject, SubjectType
from tests.utils import schema_avro_json, test_objects_avro

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

AVRO_BYTES_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.bytes",
            "name": "BytesEnvelope",
            "type": "record",
            "fields": [
                {
                    "name": "payload",
                    "type": {
                        "type": "record",
                        "name": "Payload",
                        "fields": [
                            {"name": "raw", "type": "bytes"},
                            {"name": "items", "type": {"type": "array", "items": "bytes"}},
                        ],
                    },
                }
            ],
        }
    ),
)

COMPLEX_UNION_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.minimal",
            "name": "MinimalUnionTest",
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "attrs",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "Attr",
                            "fields": [
                                {"name": "k", "type": "string"},
                                {"name": "v", "type": ["null", "string", "long", "double", "boolean"]},
                            ],
                        },
                    },
                },
                {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
            ],
        }
    ),
)


async def make_ser_deser(
    karapace_container: KarapaceContainer, mock_client: SchemaRegistryClient
) -> SchemaRegistrySerializer:
    serializer = SchemaRegistrySerializer(config=karapace_container.config())
    await serializer.registry_client.close()
    serializer.registry_client = mock_client
    return serializer


async def test_happy_flow(karapace_container: KarapaceContainer):
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
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


def test_avro_json_write_invalid(karapace_container: KarapaceContainer) -> None:
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
            write_value(karapace_container.config(), typed_schema, bio, record)


def test_avro_json_write_accepts_json_encoded_data_without_tagged_unions(karapace_container: KarapaceContainer) -> None:
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
    write_value(karapace_container.config(), typed_schema, buffer_a, properly_tagged_encoding_a)
    write_value(karapace_container.config(), typed_schema, buffer_b, missing_tag_encoding_a)
    assert buffer_a.getbuffer() == buffer_b.getbuffer()

    buffer_a = io.BytesIO()
    buffer_b = io.BytesIO()
    write_value(karapace_container.config(), typed_schema, buffer_a, properly_tagged_encoding_b)
    write_value(karapace_container.config(), typed_schema, buffer_b, missing_tag_encoding_b)
    assert buffer_a.getbuffer() == buffer_b.getbuffer()


async def test_serialization_fails(karapace_container: KarapaceContainer):
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    with pytest.raises(InvalidMessageSchema):
        schema = await serializer.get_schema_for_subject(Subject("topic"))
        await serializer.serialize(schema, {"foo": "bar"})

    assert mock_registry_client.method_calls == [call.get_schema("topic")]


async def test_deserialization_fails(karapace_container: KarapaceContainer):
    mock_registry_client = Mock()
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json), [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    deserializer = await make_ser_deser(karapace_container, mock_registry_client)
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


async def test_deserialization_propagates_schema_retrieval_error(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    mock_registry_client.get_schema_for_id.side_effect = SchemaRetrievalError("schema registry unavailable")

    deserializer = await make_ser_deser(karapace_container, mock_registry_client)
    payload = struct.pack(">bI", START_BYTE, 1)

    with pytest.raises(SchemaRetrievalError, match="schema registry unavailable"):
        await deserializer.deserialize(payload)

    assert mock_registry_client.method_calls == [call.get_schema_for_id(1)]


async def test_deserialize_offloads_avro_read_to_thread(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, COMPLEX_UNION_AVRO_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((COMPLEX_UNION_AVRO_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "id": "one",
        "attrs": [
            {"k": "text", "v": "value"},
            {"k": "count", "v": 5},
            {"k": "ratio", "v": 1.5},
            {"k": "flag", "v": True},
            {"k": "empty", "v": None},
        ],
        "props": {"present": "yes", "missing": None},
    }
    payload = await serializer.serialize(schema, record)

    to_thread_calls: list[str] = []

    async def fake_to_thread(func, *args, **kwargs):
        to_thread_calls.append(func.__name__)
        return func(*args, **kwargs)

    with patch("karapace.core.serialization.asyncio.to_thread", side_effect=fake_to_thread):
        assert await serializer.deserialize(payload) == record

    assert to_thread_calls == ["read_value"]


async def test_deserialize_compiles_avro_decoder_once_per_schema(karapace_container: KarapaceContainer) -> None:
    """The compiled decoder is cached on the schema, so repeated decodes must not rebuild it."""
    # A schema instance private to this test: _get_avro_decoder memoizes the compiled decoder on
    # the schema object, so sharing a module-level constant would make this order dependent.
    typed_schema = ValidatedTypedSchema.parse(
        SchemaType.AVRO,
        json.dumps(
            {
                "namespace": "io.aiven.minimal",
                "name": "DecoderReuseTest",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "string"},
                    {
                        "name": "attrs",
                        "type": {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "Attr",
                                "fields": [
                                    {"name": "k", "type": "string"},
                                    {"name": "v", "type": ["null", "string", "long", "double", "boolean"]},
                                ],
                            },
                        },
                    },
                    {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
                ],
            }
        ),
    )
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, typed_schema, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((typed_schema, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "id": "one",
        "attrs": [
            {"k": "text", "v": "value"},
            {"k": "count", "v": 5},
            {"k": "ratio", "v": 1.5},
            {"k": "flag", "v": True},
        ],
        "props": {"present": "yes", "missing": None},
    }
    payload = await serializer.serialize(schema, record)
    build_decoder_calls = 0

    def counting_build_decoder(avro_schema):
        nonlocal build_decoder_calls
        build_decoder_calls += 1
        return build_decoder(avro_schema)

    with patch("karapace.core.serialization.build_decoder", counting_build_decoder):
        assert await serializer.deserialize(payload) == record
        assert await serializer.deserialize(payload) == record

    assert build_decoder_calls == 1


async def test_deserialize_does_not_use_datum_reader(karapace_container: KarapaceContainer) -> None:
    """Decoding runs on the compiled decoders only, never on avro's resolution-driven reader."""
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, COMPLEX_UNION_AVRO_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((COMPLEX_UNION_AVRO_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "id": "one",
        "attrs": [{"k": "text", "v": "value"}, {"k": "count", "v": 5}],
        "props": {"present": "yes", "missing": None},
    }
    payload = await serializer.serialize(schema, record)

    def fail_on_read(self, decoder):
        raise AssertionError("DatumReader must not be used on the decode path")

    with patch.object(avro.io.DatumReader, "read", fail_on_read):
        assert await serializer.deserialize(payload) == record


async def test_deserialize_converts_avro_bytes_to_base64_strings(karapace_container: KarapaceContainer) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, AVRO_BYTES_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((AVRO_BYTES_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "payload": {
            "raw": b"\x01\x02",
            "items": [b"\x03\x04", b"\x05\x06"],
        }
    }
    payload = await serializer.serialize(schema, record)

    assert await serializer.deserialize(payload) == {
        "payload": {
            "raw": base64.b64encode(b"\x01\x02").decode("ascii"),
            "items": [
                base64.b64encode(b"\x03\x04").decode("ascii"),
                base64.b64encode(b"\x05\x06").decode("ascii"),
            ],
        }
    }


async def test_deserialize_converts_empty_avro_bytes_to_empty_base64_strings(
    karapace_container: KarapaceContainer,
) -> None:
    mock_registry_client = Mock()
    get_latest_schema_future = asyncio.Future()
    get_latest_schema_future.set_result((1, AVRO_BYTES_SCHEMA, Versioner.V(1)))
    mock_registry_client.get_schema.return_value = get_latest_schema_future
    schema_for_id_one_future = asyncio.Future()
    schema_for_id_one_future.set_result((AVRO_BYTES_SCHEMA, [Subject("stub")]))
    mock_registry_client.get_schema_for_id.return_value = schema_for_id_one_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema = await serializer.get_schema_for_subject(Subject("top"))
    record = {
        "payload": {
            "raw": b"",
            "items": [b"", b""],
        }
    }
    payload = await serializer.serialize(schema, record)

    assert await serializer.deserialize(payload) == {
        "payload": {
            "raw": "",
            "items": ["", ""],
        }
    }


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


# Authorization forwarding via sr_authorization_ctx. Tested through observed headers
# on the mocked Client — covers post_new_schema, _get_schema_recursive, get_schema_for_id,
# and the @alru_cache partitioning on get_schema.


def _make_result(json_result: dict, status: int = 200) -> Mock:
    result = Mock()
    result.ok = 200 <= status < 300
    result.status_code = status
    result.json = Mock(return_value=json_result)
    return result


async def test_post_new_schema_forwards_authorization_header(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    post_future = asyncio.Future()
    post_future.set_result(_make_result({"id": 42}))
    sr_client.client.post = Mock(return_value=post_future)

    sr_authorization_ctx.set("Bearer fwd.token")
    schema = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    schema_id = await sr_client.post_new_schema("subj", schema)

    assert schema_id == 42
    _, kwargs = sr_client.client.post.call_args
    # Authorization is forwarded; SR vendor Content-Type is preserved.
    assert kwargs["headers"] == {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
        "Authorization": "Bearer fwd.token",
    }


async def test_post_new_schema_no_authorization_header_when_ctx_unset(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    post_future = asyncio.Future()
    post_future.set_result(_make_result({"id": 42}))
    sr_client.client.post = Mock(return_value=post_future)

    schema = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    await sr_client.post_new_schema("subj", schema)

    _, kwargs = sr_client.client.post.call_args
    # Ctx unset → no Authorization; vendor Content-Type stays.
    assert kwargs["headers"] == {"Content-Type": "application/vnd.schemaregistry.v1+json"}


async def test_post_new_schema_treats_empty_token_as_unset(reset_sr_authorization_ctx) -> None:
    """Empty contextvar string must not produce an `Authorization: ` header."""
    sr_client = SchemaRegistryClient()
    post_future = asyncio.Future()
    post_future.set_result(_make_result({"id": 42}))
    sr_client.client.post = Mock(return_value=post_future)

    sr_authorization_ctx.set("")
    schema = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    await sr_client.post_new_schema("subj", schema)

    _, kwargs = sr_client.client.post.call_args
    assert "Authorization" not in kwargs["headers"]
    assert kwargs["headers"] == {"Content-Type": "application/vnd.schemaregistry.v1+json"}


async def test_get_schema_for_id_forwards_authorization_header(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    get_future = asyncio.Future()
    get_future.set_result(
        _make_result(
            {
                "schema": schema_avro_json,
                "subjects": ["subj"],
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )
    sr_client.client.get = Mock(return_value=get_future)

    sr_authorization_ctx.set("Bearer xyz")
    await sr_client.get_schema_for_id(1)

    _, kwargs = sr_client.client.get.call_args
    assert kwargs["headers"] == {"Authorization": "Bearer xyz"}


async def test_get_schema_recursive_forwards_authorization_header(reset_sr_authorization_ctx) -> None:
    sr_client = SchemaRegistryClient()
    get_future = asyncio.Future()
    get_future.set_result(
        _make_result(
            {
                "id": 7,
                "schema": schema_avro_json,
                "version": 1,
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )
    sr_client.client.get = Mock(return_value=get_future)

    sr_authorization_ctx.set("Bearer recursive")
    # Bypass @alru_cache on get_schema.
    schema_id, _, _ = await sr_client._get_schema_recursive(Subject("subj"), set(), None)

    assert schema_id == 7
    _, kwargs = sr_client.client.get.call_args
    assert kwargs["headers"] == {"Authorization": "Bearer recursive"}


async def test_get_schema_cache_partitions_by_token(reset_sr_authorization_ctx) -> None:
    """Cache key includes the token fingerprint: same token hits cache, different token misses."""

    sr_client = SchemaRegistryClient()
    sr_client.client.get = AsyncMock(
        return_value=_make_result(
            {
                "id": 11,
                "schema": schema_avro_json,
                "version": 1,
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )

    subject = Subject("uniq-subject-for-cache-partition-test")

    sr_authorization_ctx.set("Bearer first")
    await sr_client.get_schema(subject)
    await sr_client.get_schema(subject)  # same token — cache hit
    assert sr_client.client.get.call_count == 1

    sr_authorization_ctx.set("Bearer second")
    await sr_client.get_schema(subject)  # different token — cache miss, SR is consulted again
    assert sr_client.client.get.call_count == 2

    sr_authorization_ctx.set("Bearer first")
    await sr_client.get_schema(subject)  # back to first token — cache hit
    assert sr_client.client.get.call_count == 2


async def test_get_schema_cache_unauthenticated_path_unchanged(reset_sr_authorization_ctx) -> None:
    """Unauthenticated path: empty fingerprint, back-to-back calls still hit cache."""
    sr_client = SchemaRegistryClient()
    get_future = asyncio.Future()
    get_future.set_result(
        _make_result(
            {
                "id": 12,
                "schema": schema_avro_json,
                "version": 1,
                "schemaType": SchemaType.AVRO.value,
            }
        )
    )
    sr_client.client.get = Mock(return_value=get_future)

    subject = Subject("uniq-subject-for-cache-unauth-test")
    await sr_client.get_schema(subject)
    await sr_client.get_schema(subject)
    assert sr_client.client.get.call_count == 1


MAP_UNION_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.minimal",
            "name": "MapUnion",
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
            ],
        }
    ),
)


def test_write_value_validates_avro_once_for_untagged_value(karapace_container: KarapaceContainer) -> None:
    value = {"id": "x", "props": {"k": "v"}}

    top_schema = MAP_UNION_SCHEMA.schema
    original_validate = avro.io.validate
    full_tree_validations = 0

    def counting_validate(expected_schema, *args, **kwargs):
        nonlocal full_tree_validations
        if expected_schema is top_schema:
            full_tree_validations += 1
        return original_validate(expected_schema, *args, **kwargs)

    with patch("avro.io.validate", counting_validate):
        write_value(karapace_container.config(), MAP_UNION_SCHEMA, io.BytesIO(), value)

    assert full_tree_validations == 1, f"expected a single full-tree validation, got {full_tree_validations}"


def test_write_value_map_union_tagged_and_untagged_encode_identically(karapace_container: KarapaceContainer) -> None:
    untagged = {"id": "x", "props": {"k": "v"}}
    tagged = {"id": "x", "props": {"k": {"string": "v"}}}

    buf_untagged = io.BytesIO()
    buf_tagged = io.BytesIO()
    write_value(karapace_container.config(), MAP_UNION_SCHEMA, buf_untagged, untagged)
    write_value(karapace_container.config(), MAP_UNION_SCHEMA, buf_tagged, tagged)

    assert buf_untagged.getvalue() == buf_tagged.getvalue()

    reader = avro.io.DatumReader(writers_schema=MAP_UNION_SCHEMA.schema)
    decoded = reader.read(avro.io.BinaryDecoder(io.BytesIO(buf_untagged.getvalue())))
    assert decoded == untagged


DECIMAL_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.minimal",
            "name": "Decimals",
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
            ],
        }
    ),
)


def test_write_value_retry_starts_from_a_clean_buffer(karapace_container: KarapaceContainer) -> None:
    """A datum can pass avro's validation and still fail inside the encoder, halfway through.

    write_value writes the value as is and only flattens tagged unions if that raises
    AvroTypeException, which DatumWriter.write normally raises from its validation pass, before a
    single byte is emitted. The encoder raises the same exception type on its own though: a decimal
    whose exponent does not fit the schema scale raises AvroOutOfScaleException once the fields
    before it are already encoded. The retry has to start from where the first attempt started,
    otherwise it appends a second value to a partially encoded one and the message that
    SchemaRegistrySerializer.serialize returns is silently corrupt.
    """
    value = {"id": "x", "amount": decimal.Decimal("1.23456")}
    assert avro.io.validate(DECIMAL_SCHEMA.schema, value), "the failure has to come from the encoder, not validation"

    bio = io.BytesIO()
    bio.write(b"header")  # serialize() puts the magic byte and the schema id in front of the value

    buffer_when_called = []
    original_write = avro.io.DatumWriter.write

    def spy(self, datum, encoder):
        buffer_when_called.append(encoder.writer.getvalue())
        return original_write(self, datum, encoder)

    with patch.object(avro.io.DatumWriter, "write", spy), pytest.raises(avro.errors.AvroTypeException):
        write_value(karapace_container.config(), DECIMAL_SCHEMA, bio, value)

    assert len(buffer_when_called) == 2, "the encoder failed, so write_value is expected to have retried"
    assert buffer_when_called[0] == b"header", "the first attempt must encode right after the header"
    assert buffer_when_called[1] == b"header", "the retry must not see the bytes of the failed attempt"
