from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from kafka.serializer.abstract import Deserializer, Serializer
from karapace.config import read_config
from karapace.rapu import HTTPRequest, HTTPResponse
from karapace.utils import json_encode
from urllib.parse import quote, urljoin

import asyncio
import avro
import io
import karapace.karapace
import logging
import os
import requests
import struct

log = logging.getLogger(__name__)

START_BYTE = 0x13
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


def topic_name_strategy(topic_name, record_name):
    # pylint: disable=W0613
    return topic_name


def record_name_strategy(topic_name, record_name):
    # pylint: disable=W0613
    return record_name


def topic_record_name_strategy(topic_name, record_name):
    return topic_name + "-" + record_name


NAME_STRATEGIES = {
    "topic_name": topic_name_strategy,
    "record_name": record_name_strategy,
    "topic_record_name": topic_record_name_strategy
}


class SchemaRegistryBasicClientBase:
    def post_new_schema(self, subject: str, schema: avro.schema.RecordSchema) -> int:
        # For simplicity, assumes compatibility checks to take place inside the delegated code (karapace does that)
        # this method's purpose being solely to register a schema in case it has not been already registered, before
        # using it for ser / deser ops
        raise NotImplementedError()

    def get_latest_schema(self, subject: str) -> (int, avro.schema.RecordSchema):
        # if the schema for a particular topic key/value has not been provided,
        # try to retrieve the latest from the registry
        raise NotImplementedError()

    def get_schema_for_id(self, schema_id: int) -> avro.schema.RecordSchema:
        # get schema associated with the given id. To be used in deserialization logic
        raise NotImplementedError()


class SchemaRegistryBasicClientRemote(SchemaRegistryBasicClientBase):
    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        self.base_url = schema_registry_url

    def post_new_schema(self, subject, schema):
        payload = {"schema": json_encode(schema.to_json())}
        result = requests.post(urljoin(self.base_url, "subjects/%s/versions" % quote(subject)), json=payload)
        if not result.ok:
            raise SchemaRetrievalError(result.json()["message"])
        return result.json()["id"]

    def get_latest_schema(self, subject):
        result = requests.get(urljoin(self.base_url, "subjects/%s/versions/latest" % quote(subject)))
        if not result.ok:
            raise SchemaRetrievalError(result.json()["message"])
        json_result = result.json()
        if "id" not in json_result or "schema" not in json_result:
            raise SchemaRetrievalError("Invalid result format: %r" % json_result)
        try:
            return json_result["id"], avro.io.schema.parse(json_result["schema"])
        except avro.io.schema.SchemaParseException as e:
            raise SchemaRetrievalError("Failed to parse schema string from response: %r" % json_result) from e

    def get_schema_for_id(self, schema_id):
        result = requests.get(urljoin(self.base_url, "schemas/ids/%d" % schema_id))
        if not result.ok:
            raise SchemaRetrievalError(result.json()["message"])
        json_result = result.json()
        if "schema" not in json_result:
            raise SchemaRetrievalError("Invalid result format: %r" % json_result)
        try:
            return avro.io.schema.parse(json_result["schema"])
        except avro.io.schema.SchemaParseException as e:
            raise SchemaRetrievalError("Failed to parse schema string from response: %r" % json_result) from e


class SchemaRegistryBasicClientLocal(SchemaRegistryBasicClientBase):
    def __init__(self, krp: karapace.karapace.Karapace):
        self.krp = krp

    def post_new_schema(self, subject, schema):
        try:
            req = HTTPRequest(headers={}, query="", method="POST", path_for_stats="", url="")
            req.json = schema.to_json()
            self.krp.subject_post(request=req, subject=subject)
            raise SchemaRetrievalError("Karapace client not responding")
        except HTTPResponse as resp:
            if resp.ok():
                return resp.body["id"]
            raise SchemaRetrievalError("Unable to register schema %r for subject %r" % (schema.to_json(), subject)) from resp

    def get_latest_schema(self, subject):
        ret = None
        loop = asyncio.get_event_loop()
        try:
            ret = loop.run_until_complete(
                self.krp.subject_version_get(None, subject=subject, version="latest", return_dict=True)
            )
            schema_id = ret["id"]
            schema_str = ret["schema"]
            schema = avro.io.schema.parse(schema_str)
            return schema_id, schema
        except HTTPResponse as e:
            raise SchemaRetrievalError("Failed to retrieve schema") from e
        except KeyError as e:
            raise SchemaRetrievalError("Invalid response format: %r" % ret) from e
        except avro.io.schema.SchemaParseException as e:
            raise SchemaRetrievalError("Failed to parse schema string from response: %r" % ret) from e
        finally:
            loop.close()

    def get_schema_for_id(self, schema_id):
        try:
            self.krp.schemas_get(None, schema_id=schema_id)
            raise SchemaRetrievalError("Karapace client not responding")
        except HTTPResponse as r:
            if r.ok():
                try:
                    return avro.io.schema.parse(r.body["schema"])
                except KeyError as e:
                    raise SchemaRetrievalError("Invalid response body format: %r" % r.body) from e
                except avro.io.schema.SchemaParseException as e:
                    raise SchemaRetrievalError("Failed to parse schema string: %r" % r.body["schema"]) from e
            else:
                raise SchemaRetrievalError("Failed to retrieve schema") from r


class SchemaRegistrySerializerDeserializer:
    def __init__(self, **config):
        super().__init__(**config)
        if not config.get("config_path"):
            raise Exception("config_path is empty or missing")
        config_path = config["config_path"]
        self.config = read_config(config_path)
        if config.get("schema_registry_url"):
            registry_url = self.config["schema_registry_url"]
            registry_client = SchemaRegistryBasicClientRemote(registry_url)
        else:
            log.debug("Registry url not found in config, checking args for registry_client")
            registry_client = config["registry_client"]
        self.subject_name_strategy = topic_name_strategy
        self.registry_client = registry_client
        self.subjects_to_schemas = {}
        self.ids_to_schemas = {}
        self.schemas_to_ids = {}
        try:
            schemas_folder = self.config.pop("schemas_folder")
            self._populate_schemas_from_folder(schemas_folder)
        except KeyError:
            pass

    @staticmethod
    def serialize_schema(schema):
        return json_encode(schema.to_json())

    def _populate_schemas_from_folder(self, schemas_folder):
        extension = ".avsc"
        term = self.get_suffix() + extension
        for f in os.listdir(schemas_folder):
            if f.endswith(term):
                with open(os.path.join(schemas_folder, f), 'r') as schema_file:
                    subject = f[:-len(extension)]
                    schema = avro.io.schema.parse(schema_file.read())
                    schema_id = self.registry_client.post_new_schema(subject, schema)
                    self.subjects_to_schemas[subject] = schema
                    self.ids_to_schemas[schema_id] = schema
                    self.schemas_to_ids[self.serialize_schema(schema)] = schema_id
            else:
                log.debug("Ignoring file %r in folder %r as it does not terminate in %r", f, schemas_folder, term)
        if not self.subjects_to_schemas:
            log.warning("Folder %r did not contain any valid named schema files", schemas_folder)

    def get_schema_for_topic(self, topic):
        # apparently we can't really implement the other 2 and keep in line with the current kafka client api
        # due to the serialize / deserialize method signature only passing topic as a relevant indicator
        subject = self.subject_name_strategy(topic, None) + self.get_suffix()
        if subject in self.subjects_to_schemas:
            return self.subjects_to_schemas[subject]
        log.info("schema for subject %r not found locally, attempting to retrieve latest value", subject)
        schema_id, schema = self.registry_client.get_latest_schema(subject)
        self.subjects_to_schemas[subject] = schema
        self.schemas_to_ids[self.serialize_schema(schema)] = schema_id
        self.ids_to_schemas[schema_id] = schema
        return schema

    def get_suffix(self):
        raise NotImplementedError()


class SchemaRegistrySerializer(SchemaRegistrySerializerDeserializer, Serializer):
    def serialize(self, topic, value):
        schema = self.get_schema_for_topic(topic)
        schema_id = self.schemas_to_ids[self.serialize_schema(schema)]
        writer = DatumWriter(schema)
        with io.BytesIO() as bio:
            enc = BinaryEncoder(bio)
            bio.write(struct.pack(HEADER_FORMAT, START_BYTE, schema_id))
            try:
                writer.write(value, enc)
                enc_bytes = bio.getvalue()
                return enc_bytes
            except avro.io.AvroTypeException as e:
                raise InvalidMessageSchema("Object does not fit to stored schema") from e

    def get_suffix(self):
        # make pylint shut up and not disable it for this class
        raise NotImplementedError()


class SchemaRegistryDeserializer(SchemaRegistrySerializerDeserializer, Deserializer):
    def deserialize(self, topic, bytes_):
        schema = self.get_schema_for_topic(topic)
        reader = DatumReader(schema)
        with io.BytesIO(bytes_) as bio:
            byte_arr = bio.read(HEADER_SIZE)
            dec = BinaryDecoder(bio)
            # we should probably check for compatibility here
            start_byte, _ = struct.unpack(HEADER_FORMAT, byte_arr)
            if start_byte != START_BYTE:
                raise InvalidMessageHeader("Start byte is %x and should be %x" % (start_byte, START_BYTE))
            try:
                ret_val = reader.read(dec)
                return ret_val
            except AssertionError as e:
                raise InvalidPayload("Data does not contain a valid avro message") from e
            except avro.io.SchemaResolutionException as e:
                raise InvalidPayload("Data cannot be decoded with provided schema") from e

    def get_suffix(self):
        # make pylint shut up and not disable it for this class
        raise NotImplementedError()


class KeyHandlerMixin(SchemaRegistrySerializerDeserializer):
    def get_suffix(self):
        return "-key"


class ValueHandlerMixin(SchemaRegistrySerializerDeserializer):
    def get_suffix(self):
        return "-value"


class SchemaRegistryValueDeserializer(ValueHandlerMixin, SchemaRegistryDeserializer):
    pass


class SchemaRegistryKeyDeserializer(KeyHandlerMixin, SchemaRegistryDeserializer):
    pass


class SchemaRegistryValueSerializer(ValueHandlerMixin, SchemaRegistrySerializer):
    pass


class SchemaRegistryKeySerializer(KeyHandlerMixin, SchemaRegistrySerializer):
    pass
