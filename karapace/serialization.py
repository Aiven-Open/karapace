from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from avro.schema import Schema
from karapace.config import read_config
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


def parse_schema(schema: str) -> Schema:
    return avro.io.schema.Parse(schema)


class SchemaRegistryClient:
    def __init__(self, schema_registry_url: str = "http://localhost:8081", loop: asyncio.AbstractEventLoop = None):
        self.client = Client(server_uri=schema_registry_url, client=aiohttp.ClientSession(loop=loop))
        self.base_url = schema_registry_url

    async def post_new_schema(self, subject: str, schema: Schema) -> int:
        payload = {"schema": json_encode(schema.to_json())}
        result = await self.client.post("subjects/%s/versions" % quote(subject), json=payload)
        if not result.ok():
            raise SchemaRetrievalError(result.json())
        return result.json()["id"]

    async def get_latest_schema(self, subject: str) -> (int, Schema):
        result = await self.client.get("subjects/%s/versions/latest" % quote(subject))
        if not result.ok():
            raise SchemaRetrievalError(result.json())
        json_result = result.json()
        if "id" not in json_result or "schema" not in json_result:
            raise SchemaRetrievalError("Invalid result format: %r" % json_result)
        try:
            return json_result["id"], parse_schema(json_result["schema"])
        except avro.io.schema.SchemaParseException as e:
            raise SchemaRetrievalError("Failed to parse schema string from response: %r" % json_result) from e

    async def get_schema_for_id(self, schema_id):
        result = await self.client.get("schemas/ids/%d" % schema_id)
        if not result.ok():
            raise SchemaRetrievalError(result.json()["message"])
        json_result = result.json()
        if "schema" not in json_result:
            raise SchemaRetrievalError("Invalid result format: %r" % json_result)
        try:
            return parse_schema(json_result["schema"])
        except avro.io.schema.SchemaParseException as e:
            raise SchemaRetrievalError("Failed to parse schema string from response: %r" % json_result) from e

    async def close(self):
        await self.client.close()


class SchemaRegistrySerializerDeserializer:
    def __init__(self, **config):
        if not config.get("config_path"):
            raise ValueError("config_path is empty or missing")
        config_path = config["config_path"]
        self.config = read_config(config_path)
        registry_url = "%s:%d" % (self.config["host"], self.config["port"])
        registry_client = SchemaRegistryClient(registry_url)
        self.subject_name_strategy = topic_name_strategy
        self.registry_client = registry_client
        self.ids_to_schemas = {}
        self.schemas_to_ids = {}

    @staticmethod
    def serialize_schema(schema: Schema) -> dict:
        return json_encode(schema.to_json())

    async def get_schema_for_topic(self, subject: str) -> Schema:
        schema_id, schema = await self.registry_client.get_latest_schema(subject)
        self.schemas_to_ids[self.serialize_schema(schema)] = schema_id
        self.ids_to_schemas[schema_id] = schema
        return schema

    async def get_schema_for_id(self, schema_id: int) -> Schema:
        if schema_id in self.ids_to_schemas:
            return parse_schema(self.ids_to_schemas[schema_id])
        schema = await self.registry_client.get_schema_for_id(schema_id)
        schema_ser = self.serialize_schema(schema)
        self.schemas_to_ids[schema_ser] = schema_id
        self.ids_to_schemas[schema_id] = schema_ser
        return schema


class SchemaRegistrySerializer(SchemaRegistrySerializerDeserializer):
    async def serialize(self, subject: str, value: dict) -> bytes:
        schema = await self.get_schema_for_topic(subject)
        schema_id = self.schemas_to_ids[self.serialize_schema(schema)]
        writer = DatumWriter(schema)
        with io.BytesIO() as bio:
            enc = BinaryEncoder(bio)
            bio.write(struct.pack(HEADER_FORMAT, START_BYTE, schema_id))
            try:
                writer.write(value, enc)
                return bio.getvalue()
            except avro.io.AvroTypeException as e:
                raise InvalidMessageSchema("Object does not fit to stored schema") from e


class SchemaRegistryDeserializer(SchemaRegistrySerializerDeserializer):
    async def deserialize(self, bytes_: bytes) -> dict:
        with io.BytesIO(bytes_) as bio:
            byte_arr = bio.read(HEADER_SIZE)
            dec = BinaryDecoder(bio)
            # we should probably check for compatibility here
            start_byte, schema_id = struct.unpack(HEADER_FORMAT, byte_arr)
            if start_byte != START_BYTE:
                raise InvalidMessageHeader("Start byte is %x and should be %x" % (start_byte, START_BYTE))
            try:
                schema = await self.get_schema_for_id(schema_id)
                reader = DatumReader(schema)
                ret_val = reader.read(dec)
                return ret_val
            except AssertionError as e:
                raise InvalidPayload("Data does not contain a valid avro message") from e
            except avro.io.SchemaResolutionException as e:
                raise InvalidPayload("Data cannot be decoded with provided schema") from e
