from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from json import load
from jsonschema import ValidationError
from karapace.config import read_config
from karapace.schema_reader import InvalidSchema, SchemaType, TypedSchema
from karapace.utils import Client, json_encode
from urllib.parse import quote

import aiohttp
import asyncio
import avro
import io
import logging
import struct

log = logging.getLogger(__name__)

START_BYTE = 0x0
HEADER_FORMAT = ">bI"
HEADER_SIZE = 5


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


def topic_name_strategy(topic_name: str, record_name: str) -> str:
    # pylint: disable=W0613
    return topic_name


def record_name_strategy(topic_name: str, record_name: str) -> str:
    # pylint: disable=W0613
    return record_name


def topic_record_name_strategy(topic_name: str, record_name: str) -> str:
    return topic_name + "-" + record_name


NAME_STRATEGIES = {
    "topic_name": topic_name_strategy,
    "record_name": record_name_strategy,
    "topic_record_name": topic_record_name_strategy
}


class SchemaRegistryClient:
    def __init__(self, schema_registry_url: str = "http://localhost:8081", loop: asyncio.AbstractEventLoop = None):
        self.client = Client(server_uri=schema_registry_url, client=aiohttp.ClientSession(loop=loop))
        self.base_url = schema_registry_url

    async def post_new_schema(self, subject: str, schema: TypedSchema) -> int:
        payload = {"schema": json_encode(schema.to_json()), "schemaType": schema.schema_type.value}
        result = await self.client.post(f"subjects/{quote(subject)}/versions", json=payload)
        if not result.ok:
            raise SchemaRetrievalError(result.json())
        return result.json()["id"]

    async def get_latest_schema(self, subject: str) -> (int, TypedSchema):
        result = await self.client.get(f"subjects/{quote(subject)}/versions/latest")
        if not result.ok:
            raise SchemaRetrievalError(result.json())
        json_result = result.json()
        if "id" not in json_result or "schema" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")
        try:
            return json_result["id"], TypedSchema.try_parse_all(json_result["schema"])
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    async def get_schema_for_id(self, schema_id: int) -> TypedSchema:
        result = await self.client.get(f"schemas/ids/{schema_id}")
        if not result.ok:
            raise SchemaRetrievalError(result.json()["message"])
        json_result = result.json()
        if "schema" not in json_result:
            raise SchemaRetrievalError(f"Invalid result format: {json_result}")
        try:
            return TypedSchema.try_parse_all(json_result["schema"])
        except InvalidSchema as e:
            raise SchemaRetrievalError(f"Failed to parse schema string from response: {json_result}") from e

    async def close(self):
        await self.client.close()


class SchemaRegistrySerializerDeserializer:
    def __init__(self, **cfg):
        if not cfg.get("config_path"):
            raise ValueError("config_path is empty or missing")
        config_path = cfg["config_path"]
        self.config = read_config(config_path)
        self.state_lock = asyncio.Lock()
        registry_url = f"http://{self.config['registry_host']}:{self.config['registry_port']}"
        registry_client = SchemaRegistryClient(registry_url)
        self.subject_name_strategy = NAME_STRATEGIES[cfg.get("name_strategy", "topic_name")]
        self.registry_client = registry_client
        self.ids_to_schemas = {}
        self.schemas_to_ids = {}

    async def close(self):
        if self.registry_client:
            await self.registry_client.close()
            self.registry_client = None

    def get_subject_name(self, topic_name: str, schema: str, subject_type: str) -> str:
        schema = TypedSchema.try_parse_all(schema)
        namespace = "dummy"
        if schema.schema_type is SchemaType.AVRO:
            namespace = schema.schema.namespace
        if schema.schema_type is SchemaType.JSONSCHEMA:
            namespace = schema.to_json().get("namespace", "dummy")
        return f"{self.subject_name_strategy(topic_name, namespace)}-{subject_type}"

    async def get_schema_for_subject(self, subject: str) -> TypedSchema:
        schema_id, schema = await self.registry_client.get_latest_schema(subject)
        async with self.state_lock:
            self.schemas_to_ids[schema.__str__()] = schema_id
            self.ids_to_schemas[schema_id] = schema
        return schema

    async def get_id_for_schema(self, schema: str, subject: str, schema_type: SchemaType) -> int:
        try:
            schema = TypedSchema.parse(schema_type, schema)
        except InvalidSchema as e:
            raise InvalidPayload(f"Schema string {schema} is invalid") from e
        ser_schema = schema.__str__()
        if ser_schema in self.schemas_to_ids:
            return self.schemas_to_ids[ser_schema]
        schema_id = await self.registry_client.post_new_schema(subject, schema)
        async with self.state_lock:
            self.schemas_to_ids[ser_schema] = schema_id
            self.ids_to_schemas[schema_id] = schema
        return schema_id

    async def get_schema_for_id(self, schema_id: int) -> TypedSchema:
        if schema_id in self.ids_to_schemas:
            return self.ids_to_schemas[schema_id]
        schema = await self.registry_client.get_schema_for_id(schema_id)
        schema_ser = schema.__str__()
        async with self.state_lock:
            self.schemas_to_ids[schema_ser] = schema_id
            self.ids_to_schemas[schema_id] = schema
        return schema


def read_value(schema: TypedSchema, bio: io.BytesIO):
    if schema.schema_type is SchemaType.AVRO:
        reader = DatumReader(schema.schema)
        return reader.read(BinaryDecoder(bio))
    if schema.schema_type is SchemaType.JSONSCHEMA:
        value = load(bio)
        try:
            schema.schema.validate(value)
        except ValidationError as e:
            raise InvalidPayload from e
        return value
    raise ValueError("Unknown schema type")


def write_value(schema: TypedSchema, bio: io.BytesIO, value: dict):
    if schema.schema_type is SchemaType.AVRO:
        writer = DatumWriter(schema.schema)
        writer.write(value, BinaryEncoder(bio))
    elif schema.schema_type is SchemaType.JSONSCHEMA:
        try:
            schema.schema.validate(value)
        except ValidationError as e:
            raise InvalidPayload from e
        bio.write(json_encode(value, binary=True))
    else:
        raise ValueError("Unknown schema type")


class SchemaRegistrySerializer(SchemaRegistrySerializerDeserializer):
    async def serialize(self, schema: TypedSchema, value: dict) -> bytes:
        schema_id = self.schemas_to_ids[schema.__str__()]
        with io.BytesIO() as bio:
            bio.write(struct.pack(HEADER_FORMAT, START_BYTE, schema_id))
            try:
                write_value(schema, bio, value)
                return bio.getvalue()
            except avro.io.AvroTypeException as e:
                raise InvalidMessageSchema("Object does not fit to stored schema") from e


class SchemaRegistryDeserializer(SchemaRegistrySerializerDeserializer):
    async def deserialize(self, bytes_: bytes) -> dict:
        with io.BytesIO(bytes_) as bio:
            byte_arr = bio.read(HEADER_SIZE)
            # we should probably check for compatibility here
            start_byte, schema_id = struct.unpack(HEADER_FORMAT, byte_arr)
            if start_byte != START_BYTE:
                raise InvalidMessageHeader("Start byte is %x and should be %x" % (start_byte, START_BYTE))
            try:
                schema = await self.get_schema_for_id(schema_id)
                ret_val = read_value(schema, bio)
                return ret_val
            except AssertionError as e:
                raise InvalidPayload(f"Data does not contain a valid {schema.schema_type} message") from e
            except avro.io.SchemaResolutionException as e:
                raise InvalidPayload("Data cannot be decoded with provided schema") from e
