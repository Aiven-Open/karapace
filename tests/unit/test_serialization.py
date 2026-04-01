"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import asyncio
import copy
import io
import json
import logging
import struct
from unittest.mock import Mock, call

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
    SchemaRetrievalError,
    SchemaRegistrySerializer,
    flatten_unions,
    get_subject_name,
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


class MockLookupResult:
    def __init__(self, status: int, body: dict):
        self.status_code = status
        self._body = body

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict:
        return self._body


async def test_lookup_schema_returns_schema_id_on_success() -> None:
    """lookup_schema returns SchemaId when SR responds 200 with valid id."""
    subject = Subject("lookup-subject")
    expected_id = 42
    client = SchemaRegistryClient()
    client.client = Mock()
    post_future = asyncio.Future()
    post_future.set_result(MockLookupResult(status=200, body={"id": expected_id}))
    client.client.post.return_value = post_future

    schema_id = await client.lookup_schema(subject=subject, schema=TYPED_AVRO_SCHEMA)

    assert schema_id == expected_id
    client.client.post.assert_called_once_with(
        "subjects/lookup-subject",
        json=SchemaRegistryClient._build_schema_payload(TYPED_AVRO_SCHEMA),
    )


async def test_lookup_schema_returns_none_on_not_found() -> None:
    """lookup_schema returns None when SR responds 404 (schema not registered)."""
    client = SchemaRegistryClient()
    client.client = Mock()
    post_future = asyncio.Future()
    post_future.set_result(MockLookupResult(status=404, body={"error_code": 40403}))
    client.client.post.return_value = post_future

    schema_id = await client.lookup_schema(subject=Subject("missing-subject"), schema=TYPED_AVRO_SCHEMA)

    assert schema_id is None


async def test_lookup_schema_raises_on_unsuccessful_non_404_response() -> None:
    """lookup_schema raises SchemaRetrievalError on non-404 error responses."""
    client = SchemaRegistryClient()
    client.client = Mock()
    post_future = asyncio.Future()
    post_future.set_result(MockLookupResult(status=500, body={"message": "oops"}))
    client.client.post.return_value = post_future

    with pytest.raises(SchemaRetrievalError):
        await client.lookup_schema(subject=Subject("broken-subject"), schema=TYPED_AVRO_SCHEMA)


async def test_lookup_schema_raises_on_missing_id_in_success_response() -> None:
    """lookup_schema raises SchemaRetrievalError when 200 response has no 'id' field."""
    client = SchemaRegistryClient()
    client.client = Mock()
    post_future = asyncio.Future()
    post_future.set_result(MockLookupResult(status=200, body={"status": "ok"}))
    client.client.post.return_value = post_future

    with pytest.raises(SchemaRetrievalError):
        await client.lookup_schema(subject=Subject("bad-payload-subject"), schema=TYPED_AVRO_SCHEMA)


async def test_upsert_id_for_schema_uses_lookup_first_when_enabled_and_lookup_hits(
    karapace_container: KarapaceContainer,
) -> None:
    """With lookup_first=True and schema found, only lookup_schema is called."""
    subject = Subject("upsert-subject-lookup-hit")
    expected_id = 71
    mock_registry_client = Mock()
    lookup_future = asyncio.Future()
    lookup_future.set_result(expected_id)
    mock_registry_client.lookup_schema.return_value = lookup_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema_id = await serializer.upsert_id_for_schema(TYPED_AVRO_SCHEMA, subject, lookup_first=True)

    assert schema_id == expected_id
    assert serializer.schemas_to_ids[str(TYPED_AVRO_SCHEMA)] == expected_id
    assert serializer.ids_to_schemas[expected_id] == TYPED_AVRO_SCHEMA
    assert mock_registry_client.method_calls == [call.lookup_schema(subject, TYPED_AVRO_SCHEMA)]


async def test_upsert_id_for_schema_falls_back_to_register_when_lookup_enabled_and_lookup_misses(
    karapace_container: KarapaceContainer,
) -> None:
    """With lookup_first=True and schema not found, falls back to post_new_schema."""
    subject = Subject("upsert-subject-lookup-miss")
    expected_id = 72
    mock_registry_client = Mock()
    lookup_future = asyncio.Future()
    lookup_future.set_result(None)
    post_future = asyncio.Future()
    post_future.set_result(expected_id)
    mock_registry_client.lookup_schema.return_value = lookup_future
    mock_registry_client.post_new_schema.return_value = post_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema_id = await serializer.upsert_id_for_schema(TYPED_AVRO_SCHEMA, subject, lookup_first=True)

    assert schema_id == expected_id
    assert serializer.schemas_to_ids[str(TYPED_AVRO_SCHEMA)] == expected_id
    assert serializer.ids_to_schemas[expected_id] == TYPED_AVRO_SCHEMA
    assert mock_registry_client.method_calls == [
        call.lookup_schema(subject, TYPED_AVRO_SCHEMA),
        call.post_new_schema(subject, TYPED_AVRO_SCHEMA),
    ]


async def test_upsert_id_for_schema_registers_directly_when_lookup_disabled(
    karapace_container: KarapaceContainer,
) -> None:
    """With lookup_first=False, only post_new_schema is called (default behavior)."""
    subject = Subject("upsert-subject-no-lookup")
    expected_id = 73
    mock_registry_client = Mock()
    post_future = asyncio.Future()
    post_future.set_result(expected_id)
    mock_registry_client.post_new_schema.return_value = post_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)
    schema_id = await serializer.upsert_id_for_schema(TYPED_AVRO_SCHEMA, subject, lookup_first=False)

    assert schema_id == expected_id
    assert serializer.schemas_to_ids[str(TYPED_AVRO_SCHEMA)] == expected_id
    assert serializer.ids_to_schemas[expected_id] == TYPED_AVRO_SCHEMA
    assert mock_registry_client.method_calls == [call.post_new_schema(subject, TYPED_AVRO_SCHEMA)]


async def test_upsert_id_for_schema_uses_cache_after_first_lookup_or_register(
    karapace_container: KarapaceContainer,
) -> None:
    """Second call with same schema uses cache — no extra SR requests."""
    subject = Subject("upsert-subject-cache")
    expected_id = 74
    mock_registry_client = Mock()
    lookup_future = asyncio.Future()
    lookup_future.set_result(None)
    post_future = asyncio.Future()
    post_future.set_result(expected_id)
    mock_registry_client.lookup_schema.return_value = lookup_future
    mock_registry_client.post_new_schema.return_value = post_future

    serializer = await make_ser_deser(karapace_container, mock_registry_client)

    first_schema_id = await serializer.upsert_id_for_schema(TYPED_AVRO_SCHEMA, subject, lookup_first=True)
    second_schema_id = await serializer.upsert_id_for_schema(TYPED_AVRO_SCHEMA, subject, lookup_first=True)

    assert first_schema_id == expected_id
    assert second_schema_id == expected_id
    assert mock_registry_client.method_calls == [
        call.lookup_schema(subject, TYPED_AVRO_SCHEMA),
        call.post_new_schema(subject, TYPED_AVRO_SCHEMA),
    ]


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
