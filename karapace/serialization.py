"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from aiohttp import BasicAuth
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from google.protobuf.message import DecodeError
from jsonschema import ValidationError
from karapace.client import Client
from karapace.errors import InvalidReferences
from karapace.protobuf.exception import ProtobufTypeException
from karapace.protobuf.io import ProtobufDatumReader, ProtobufDatumWriter
from karapace.schema_models import InvalidSchema, ParsedTypedSchema, SchemaType, TypedSchema, ValidatedTypedSchema
from karapace.schema_references import Reference
from karapace.utils import json_decode, json_encode
from typing import Any, Dict, Optional, Tuple
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


def topic_name_strategy(topic_name: str, record_name: str) -> str:  # pylint: disable=unused-argument
    return topic_name


def record_name_strategy(topic_name: str, record_name: str) -> str:  # pylint: disable=unused-argument
    return record_name


def topic_record_name_strategy(topic_name: str, record_name: str) -> str:
    return topic_name + "-" + record_name


NAME_STRATEGIES = {
    "topic_name": topic_name_strategy,
    "record_name": record_name_strategy,
    "topic_record_name": topic_record_name_strategy,
}


class SchemaRegistryClient:
    def __init__(
        self,
        schema_registry_url: str = "http://localhost:8081",
        server_ca: Optional[str] = None,
        session_auth: Optional[BasicAuth] = None,
    ):
        self.client = Client(server_uri=schema_registry_url, server_ca=server_ca, session_auth=session_auth)
        self.base_url = schema_registry_url

    async def post_new_schema(
        self, subject: str, schema: ValidatedTypedSchema, references: Optional[Reference] = None
    ) -> int:
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
        return result.json()["id"]

    async def get_latest_schema(self, subject: str) -> Tuple[int, ParsedTypedSchema]:
        result = await self.client.get(f"subjects/{quote(subject)}/versions/latest")
        if not result.ok:
            raise SchemaRetrievalError(result.json())
        json_result = result.json()
        if "id" not in json_result or "schema" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")
        try:
            schema_type = SchemaType(json_result.get("schemaType", "AVRO"))
            return json_result["id"], ParsedTypedSchema.parse(schema_type, json_result["schema"])
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    async def get_schema_for_id(self, schema_id: int) -> ParsedTypedSchema:
        result = await self.client.get(f"schemas/ids/{schema_id}")
        if not result.ok:
            raise SchemaRetrievalError(result.json()["message"])
        json_result = result.json()
        if "schema" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")
        try:
            schema_type = SchemaType(json_result.get("schemaType", "AVRO"))

            references = json_result.get("references")
            parsed_references = None
            if references:
                parsed_references = []
                for reference in references:
                    if ["name", "subject", "version"] != sorted(reference.keys()):
                        raise InvalidReferences()
                    parsed_references.append(
                        Reference(name=reference["name"], subject=reference["subject"], version=reference["version"])
                    )
            if parsed_references:
                return ParsedTypedSchema.parse(schema_type, json_result["schema"], references=parsed_references)
            return ParsedTypedSchema.parse(schema_type, json_result["schema"])
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    async def close(self):
        await self.client.close()


class SchemaRegistrySerializer:
    def __init__(
        self,
        config: dict,
        name_strategy: str = "topic_name",
        **cfg,  # pylint: disable=unused-argument
    ) -> None:
        self.config = config
        self.state_lock = asyncio.Lock()
        session_auth: Optional[BasicAuth] = None
        if self.config.get("registry_user") and self.config.get("registry_password"):
            session_auth = BasicAuth(self.config.get("registry_user"), self.config.get("registry_password"), encoding="utf8")
        if self.config.get("registry_ca"):
            registry_url = f"https://{self.config['registry_host']}:{self.config['registry_port']}"
            registry_client = SchemaRegistryClient(
                registry_url, server_ca=self.config["registry_ca"], session_auth=session_auth
            )
        else:
            registry_url = f"http://{self.config['registry_host']}:{self.config['registry_port']}"
            registry_client = SchemaRegistryClient(registry_url, session_auth=session_auth)
        self.subject_name_strategy = NAME_STRATEGIES[name_strategy]
        self.registry_client: Optional[SchemaRegistryClient] = registry_client
        self.ids_to_schemas: Dict[int, TypedSchema] = {}
        self.schemas_to_ids: Dict[str, int] = {}

    async def close(self) -> None:
        if self.registry_client:
            await self.registry_client.close()
            self.registry_client = None

    def get_subject_name(self, topic_name: str, schema: str, subject_type: str, schema_type: SchemaType) -> str:
        schema_typed = ParsedTypedSchema.parse(schema_type, schema)
        namespace = "dummy"
        if schema_type is SchemaType.AVRO:
            if isinstance(schema_typed.schema, avro.schema.NamedSchema):
                namespace = schema_typed.schema.namespace
        if schema_type is SchemaType.JSONSCHEMA:
            namespace = schema_typed.to_dict().get("namespace", "dummy")
        #  Protobuf does not use namespaces in terms of AVRO
        if schema_type is SchemaType.PROTOBUF:
            namespace = ""
        return f"{self.subject_name_strategy(topic_name, namespace)}-{subject_type}"

    async def get_schema_for_subject(self, subject: str) -> TypedSchema:
        assert self.registry_client, "must not call this method after the object is closed."

        schema_id, schema = await self.registry_client.get_latest_schema(subject)
        async with self.state_lock:
            schema_ser = str(schema)
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema
        return schema

    async def get_id_for_schema(self, schema: str, subject: str, schema_type: SchemaType) -> int:
        assert self.registry_client, "must not call this method after the object is closed."
        try:
            schema_typed = ParsedTypedSchema.parse(schema_type, schema)
        except InvalidSchema as e:
            raise InvalidPayload(f"Schema string {schema} is invalid") from e
        schema_ser = str(schema_typed)
        if schema_ser in self.schemas_to_ids:
            return self.schemas_to_ids[schema_ser]
        schema_id = await self.registry_client.post_new_schema(subject, schema_typed)

        async with self.state_lock:
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema_typed
        return schema_id

    async def get_schema_for_id(self, schema_id: int) -> TypedSchema:
        assert self.registry_client, "must not call this method after the object is closed."
        if schema_id in self.ids_to_schemas:
            return self.ids_to_schemas[schema_id]
        schema_typed = await self.registry_client.get_schema_for_id(schema_id)
        schema_ser = str(schema_typed)
        async with self.state_lock:
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema_typed
        return schema_typed

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
                schema = await self.get_schema_for_id(schema_id)
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


def read_value(config: dict, schema: TypedSchema, bio: io.BytesIO):
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


def write_value(config: dict, schema: TypedSchema, bio: io.BytesIO, value: dict) -> None:
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
