"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiohttp import BasicAuth
from async_lru import alru_cache
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from cachetools import TTLCache
from collections.abc import Callable, MutableMapping
from functools import lru_cache
from google.protobuf.message import DecodeError
from jsonschema import ValidationError
from karapace.client import Client
from karapace.config import Config
from karapace.dependency import Dependency
from karapace.errors import InvalidReferences
from karapace.protobuf.exception import ProtobufTypeException
from karapace.protobuf.io import ProtobufDatumReader, ProtobufDatumWriter
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_models import InvalidSchema, ParsedTypedSchema, SchemaType, TypedSchema, ValidatedTypedSchema, Versioner
from karapace.schema_references import LatestVersionReference, Reference, reference_from_mapping
from karapace.typing import NameStrategy, SchemaId, Subject, SubjectType, Version
from karapace.utils import json_decode, json_encode
from typing import Any
from urllib.parse import quote

import asyncio
import avro
import avro.schema
import io
import struct

START_BYTE = 0x0
HEADER_FORMAT = ">bI"
HEADER_SIZE = 5


class DeserializationError(Exception):
    pass


class InvalidMessageHeader(Exception):
    pass


class InvalidPayload(Exception):
    pass


class InvalidMessageSchema(Exception):
    pass


class SchemaError(Exception):
    pass


class SchemaRetrievalError(SchemaError):
    pass


class SchemaUpdateError(SchemaError):
    pass


class InvalidRecord(Exception):
    pass


def topic_name_strategy(
    topic_name: str,
    record_name: str | None,
    subject_type: SubjectType,
) -> Subject:
    return Subject(f"{topic_name}-{subject_type}")


def record_name_strategy(
    topic_name: str,
    record_name: str | None,
    subject_type: SubjectType,
) -> Subject:
    if record_name is None:
        raise InvalidRecord(
            "The provided record doesn't have a valid `record_name`, use another naming strategy or fix the schema"
        )

    return Subject(record_name)


def topic_record_name_strategy(
    topic_name: str,
    record_name: str | None,
    subject_type: SubjectType,
) -> Subject:
    validated_record_name = record_name_strategy(topic_name, record_name, subject_type)
    return Subject(f"{topic_name}-{validated_record_name}")


NAME_STRATEGIES = {
    NameStrategy.topic_name: topic_name_strategy,
    NameStrategy.record_name: record_name_strategy,
    NameStrategy.topic_record_name: topic_record_name_strategy,
}


class SchemaRegistryClient:
    def __init__(
        self,
        schema_registry_url: str = "http://localhost:8081",
        server_ca: str | None = None,
        session_auth: BasicAuth | None = None,
    ):
        self.client = Client(server_uri=schema_registry_url, server_ca=server_ca, session_auth=session_auth)
        self.base_url = schema_registry_url

    async def post_new_schema(
        self, subject: str, schema: ValidatedTypedSchema, references: Reference | None = None
    ) -> SchemaId:
        if schema.schema_type is SchemaType.PROTOBUF:
            if references:
                payload = {"schema": str(schema), "schemaType": schema.schema_type.value, "references": references.json()}
            else:
                payload = {"schema": str(schema), "schemaType": schema.schema_type.value}
        else:
            payload = {"schema": json_encode(schema.to_dict()), "schemaType": schema.schema_type.value}
        result = await self.client.post(f"subjects/{quote(subject)}/versions", json=payload)
        if not result.ok:
            raise SchemaRetrievalError(result.json())
        return SchemaId(result.json()["id"])

    async def _get_schema_recursive(
        self,
        subject: Subject,
        explored_schemas: set[tuple[Subject, Version | None]],
        version: Version | None = None,
    ) -> tuple[SchemaId, ValidatedTypedSchema, Version]:
        if (subject, version) in explored_schemas:
            raise InvalidSchema(
                f"The schema has at least a cycle in dependencies, "
                f"one path of the cycle is given by the following nodes: {explored_schemas}"
            )

        explored_schemas = explored_schemas | {(subject, version)}

        version_str = str(version) if version is not None else "latest"
        result = await self.client.get(f"subjects/{quote(subject)}/versions/{version_str}")

        if not result.ok:
            raise SchemaRetrievalError(result.json())

        json_result = result.json()
        if "id" not in json_result or "schema" not in json_result or "version" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")

        if "references" in json_result:
            references = [Reference.from_dict(data) for data in json_result["references"]]
            dependencies = {}
            for reference in references:
                _, schema, version = await self._get_schema_recursive(reference.subject, explored_schemas, reference.version)
                dependencies[reference.name] = Dependency(
                    name=reference.name, subject=reference.subject, version=version, target_schema=schema
                )
        else:
            references = None
            dependencies = None

        try:
            schema_type = SchemaType(json_result.get("schemaType", "AVRO"))
            return (
                SchemaId(json_result["id"]),
                ValidatedTypedSchema.parse(
                    schema_type,
                    json_result["schema"],
                    references=references,
                    dependencies=dependencies,
                ),
                Versioner.V(json_result["version"]),
            )
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    @alru_cache(maxsize=100)
    async def get_schema(
        self,
        subject: Subject,
        version: Version | None = None,
    ) -> tuple[SchemaId, ValidatedTypedSchema, Version]:
        """
        Retrieves the schema and its dependencies for the specified subject.

        Args:
            subject (Subject): The subject for which to retrieve the schema.
            version (Optional[Version]): The specific version of the schema to retrieve.
                                                    If None, the latest available schema will be returned.

        Returns:
            Tuple[SchemaId, ValidatedTypedSchema, Version]: A tuple containing:
                - SchemaId: The ID of the retrieved schema.
                - ValidatedTypedSchema: The retrieved schema, validated and typed.
                - Version: The version of the schema that was retrieved.
        """
        return await self._get_schema_recursive(subject, set(), version)

    async def get_schema_for_id(self, schema_id: SchemaId) -> tuple[TypedSchema, list[Subject]]:
        result = await self.client.get(f"schemas/ids/{schema_id}", params={"includeSubjects": "True"})
        if not result.ok:
            raise SchemaRetrievalError(result.json()["message"])
        json_result = result.json()
        if "schema" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")

        subjects = json_result.get("subjects")

        try:
            schema_type = SchemaType(json_result.get("schemaType", "AVRO"))

            references = json_result.get("references")
            parsed_references = None
            if references:
                parsed_references = []
                for reference_data in references:
                    try:
                        reference = reference_from_mapping(reference_data)
                    except (TypeError, KeyError) as exc:
                        raise InvalidReferences from exc
                    parsed_references.append(reference)
            if parsed_references:
                dependencies = {}

                for reference in parsed_references:
                    if isinstance(reference, LatestVersionReference):
                        _, schema, version = await self.get_schema(reference.subject)
                    else:
                        _, schema, version = await self.get_schema(reference.subject, reference.version)

                    dependencies[reference.name] = Dependency(reference.name, reference.subject, version, schema)
            else:
                dependencies = None

            return (
                ParsedTypedSchema.parse(
                    schema_type, json_result["schema"], references=parsed_references, dependencies=dependencies
                ),
                subjects,
            )
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    async def close(self):
        await self.client.close()


def get_subject_name(
    topic_name: str,
    schema: TypedSchema,
    subject_type: SubjectType,
    naming_strategy: NameStrategy,
) -> Subject:
    record_name = None

    if schema.schema_type is SchemaType.AVRO:
        if isinstance(schema.schema, avro.schema.NamedSchema):
            record_name = schema.schema.fullname
        else:
            record_name = None

    if schema.schema_type is SchemaType.JSONSCHEMA:
        record_name = schema.to_dict().get("title", None)

    if schema.schema_type is SchemaType.PROTOBUF:
        assert isinstance(schema.schema, ProtobufSchema), "Expecting a protobuf schema"
        record_name = schema.schema.record_name()

    naming_strategy = NAME_STRATEGIES[naming_strategy]
    return naming_strategy(topic_name, record_name, subject_type)


class SchemaRegistrySerializer:
    def __init__(
        self,
        config: Config,
    ) -> None:
        self.config = config
        self.state_lock = asyncio.Lock()
        session_auth: BasicAuth | None = None
        if self.config.registry_user and self.config.registry_password:
            session_auth = BasicAuth(self.config.registry_user, self.config.registry_password, encoding="utf8")
        if self.config.registry_ca:
            registry_url = f"https://{self.config.registry_host}:{self.config.registry_port}"
            registry_client = SchemaRegistryClient(
                registry_url, server_ca=self.config.registry_ca, session_auth=session_auth
            )
        else:
            registry_url = f"http://{self.config.registry_host}:{self.config.registry_port}"
            registry_client = SchemaRegistryClient(registry_url, session_auth=session_auth)
        self.registry_client: SchemaRegistryClient | None = registry_client
        self.ids_to_schemas: dict[int, TypedSchema] = {}
        self.ids_to_subjects: MutableMapping[int, list[Subject]] = TTLCache(maxsize=10000, ttl=600)
        self.schemas_to_ids: dict[str, SchemaId] = {}

    async def close(self) -> None:
        if self.registry_client:
            await self.registry_client.close()
            self.registry_client = None

    async def get_schema_for_subject(self, subject: Subject) -> TypedSchema:
        assert self.registry_client, "must not call this method after the object is closed."
        schema_id, schema, _ = await self.registry_client.get_schema(subject)
        async with self.state_lock:
            schema_ser = str(schema)
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema
        return schema

    async def upsert_id_for_schema(self, schema_typed: ValidatedTypedSchema, subject: str) -> SchemaId:
        assert self.registry_client, "must not call this method after the object is closed."

        schema_ser = str(schema_typed)

        if schema_ser in self.schemas_to_ids:
            return self.schemas_to_ids[schema_ser]

        # note: the post is idempotent, so it is like a get or insert (aka upsert)
        schema_id = await self.registry_client.post_new_schema(subject, schema_typed)

        async with self.state_lock:
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema_typed
        return schema_id

    async def get_schema_for_id(
        self,
        schema_id: SchemaId,
        *,
        need_new_call: Callable[[TypedSchema, list[Subject]], bool] | None = None,
    ) -> tuple[TypedSchema, list[Subject]]:
        assert self.registry_client, "must not call this method after the object is closed."
        if schema_id in self.ids_to_subjects:
            if need_new_call is None or not need_new_call(self.ids_to_schemas[schema_id], self.ids_to_subjects[schema_id]):
                return self.ids_to_schemas[schema_id], self.ids_to_subjects[schema_id]

        schema_typed, subjects = await self.registry_client.get_schema_for_id(schema_id)
        schema_ser = str(schema_typed)
        async with self.state_lock:
            # todo: get rid of the schema caching and use the same caching used in UserRestProxy
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema_typed
            self.ids_to_subjects[schema_id] = subjects
        return schema_typed, subjects

    async def serialize(self, schema: TypedSchema, value: dict) -> bytes:
        schema_id = self.schemas_to_ids[str(schema)]
        with io.BytesIO() as bio:
            bio.write(struct.pack(HEADER_FORMAT, START_BYTE, schema_id))
            try:
                write_value(self.config, schema, bio, value)
                return bio.getvalue()
            except ProtobufTypeException as e:
                raise InvalidMessageSchema("Object does not fit to stored schema") from e
            except avro.errors.AvroTypeException as e:
                raise InvalidMessageSchema("Object does not fit to stored schema") from e

    async def deserialize(self, bytes_: bytes) -> dict:
        with io.BytesIO(bytes_) as bio:
            byte_arr = bio.read(HEADER_SIZE)
            # we should probably check for compatibility here
            start_byte, schema_id = struct.unpack(HEADER_FORMAT, byte_arr)
            if start_byte != START_BYTE:
                raise InvalidMessageHeader(f"Start byte is {start_byte:x} and should be {START_BYTE:x}")
            try:
                schema, _ = await self.get_schema_for_id(schema_id)
                if schema is None:
                    raise InvalidPayload("No schema with ID from payload")
                ret_val = read_value(self.config, schema, bio)
                return ret_val
            except (UnicodeDecodeError, TypeError, avro.errors.InvalidAvroBinaryEncoding) as e:
                raise InvalidPayload("Data does not contain a valid message") from e
            except avro.errors.SchemaResolutionException as e:
                raise InvalidPayload("Data cannot be decoded with provided schema") from e


def flatten_unions(schema: avro.schema.Schema, value: Any) -> Any:
    """Recursively flattens unions to convert Avro JSON payloads to internal dictionaries

    Data encoded to Avro JSON has a special case for union types, values of these type are encoded
    as tagged union. The additional tag is not expected to be in the internal data format and has to
    be removed before further processing. This means the JSON document must be further processed to
    remove the tag, this function does just that, recursing over the JSON document and handling the
    tagged unions.

    Given this schema:

        {"name": "Test", "type": "record", "fields": [{"name": "attr", "type": ["null", "string"]}]}

    The record JSON encoded as:

        {"attr":{"string":"sample data"}}

    The python representation is:

        {"attr":"sample data"}

    This function:

    - Translates the first to the second when necessary, this adds compatibility for libraries that
      perform the _correct_ encoding.
    - Does nothing if the provided data is already in the second format. The data is improperly
      encoded, but this maintains backwards compatibility.

    See also https://avro.apache.org/docs/current/spec.html#json_encoding
    """

    if isinstance(schema, avro.schema.RecordSchema) and isinstance(value, dict):
        result = dict(value)
        for field in schema.fields:
            if field.name in value:
                result[field.name] = flatten_unions(field.type, value[field.name])
        return result

    if isinstance(schema, avro.schema.UnionSchema) and isinstance(value, dict):

        def get_name(obj) -> str:
            if isinstance(obj, avro.schema.PrimitiveSchema):
                return obj.fullname
            if isinstance(obj, (avro.schema.ArraySchema, avro.schema.MapSchema)):
                return obj.type
            return obj.name

        f = next((s for s in schema.schemas if get_name(s) in value), None)
        if f is not None:
            # Note: This is intentionally skipping the dictionary, here the JSON representation
            # is flattened to the Python representation
            return flatten_unions(f, value[get_name(f)])

    if isinstance(schema, avro.schema.ArraySchema) and isinstance(value, list):
        return [flatten_unions(schema.items, v) for v in value]

    if isinstance(schema, avro.schema.MapSchema) and isinstance(value, dict):
        return {k: flatten_unions(schema.values, v) for (k, v) in value.items()}

    return value


def read_value(config: Config, schema: TypedSchema, bio: io.BytesIO):
    if schema.schema_type is SchemaType.AVRO:
        reader = DatumReader(writers_schema=schema.schema)
        return reader.read(BinaryDecoder(bio))
    if schema.schema_type is SchemaType.JSONSCHEMA:
        value = json_decode(bio)
        try:
            schema.schema.validate(value)
        except ValidationError as e:
            raise InvalidPayload from e
        return value

    if schema.schema_type is SchemaType.PROTOBUF:
        try:
            reader = ProtobufDatumReader(config, schema.schema)
            return reader.read(bio)
        except DecodeError as e:
            raise InvalidPayload from e

    raise ValueError("Unknown schema type")


def write_value(config: Config, schema: TypedSchema, bio: io.BytesIO, value: dict) -> None:
    if schema.schema_type is SchemaType.AVRO:
        # Backwards compatibility: Support JSON encoded data without the tags for unions.
        if avro.io.validate(schema.schema, value):
            data = value
        else:
            data = flatten_unions(schema.schema, value)

        writer = DatumWriter(writers_schema=schema.schema)
        writer.write(data, BinaryEncoder(bio))
    elif schema.schema_type is SchemaType.JSONSCHEMA:
        try:
            schema.schema.validate(value)
        except ValidationError as e:
            raise InvalidPayload from e
        bio.write(json_encode(value, binary=True))

    elif schema.schema_type is SchemaType.PROTOBUF:
        # TODO: PROTOBUF* we need use protobuf validator there
        writer = ProtobufDatumWriter(config, schema.schema)
        writer.write_index(bio)
        writer.write(value, bio)

    else:
        raise ValueError("Unknown schema type")
