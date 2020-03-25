from avro.io import BinaryEncoder, DatumWriter
from kafka.serializer.abstract import Serializer
from karapace.config import read_config

import abc
import avro
import io
import json
import karapace.karapace
import logging
import os
import struct

log = logging.getLogger(__name__)

START_BYTE = 0x13


class SchemaRegistryBasicClientBase:
    # first draft will use topic name strategy for subject names. I know the class name is horrible...
    __meta__ = abc.ABCMeta

    @abc.abstractmethod
    def post_new_schema(self, subject: str, schema: avro.schema.RecordSchema) -> int:
        # For simplicity, assumes compatibility checks to take place inside the delegated code (karapace does that)
        # this method's purpose being solely to register a schema in case it has not been already registered, before
        # using it for ser / deser ops
        pass

    @abc.abstractmethod
    def get_latest_schema(self, subject: str) -> (int, avro.schema.RecordSchema):
        # if the schema for a particular topic key/value has not been provided,
        # try to retrieve the latest from the registry
        pass

    @abc.abstractmethod
    def get_schema_for_id(self, schema_id: int) -> avro.schema.RecordSchema:
        # get schema associated with the given id. To be used in deserialization logic
        pass


class SchemaRegistryRemoteBasicClient(SchemaRegistryBasicClientBase):
    def __init__(self, schema_registry_url: str):
        pass

    def get_latest_schema(self, subject):
        raise NotImplementedError()

    def get_schema_for_id(self, schema_id):
        raise NotImplementedError()

    def post_new_schema(self, subject, schema):
        raise NotImplementedError()


class SchemaRegistryLocalBasicClient(SchemaRegistryBasicClientBase):
    def __init__(self, krp: karapace.karapace.Karapace):
        self.krp = krp

    def get_latest_schema(self, subject):
        raise NotImplementedError()

    def get_schema_for_id(self, schema_id):
        raise NotImplementedError()

    def post_new_schema(self, subject, schema):
        raise NotImplementedError()


class SchemaRegistrySerializer(Serializer):
    def __init__(self, **config):
        super().__init__(**config)
        config_path = config.pop("config_path")
        self.config = read_config(config_path)
        try:
            registry_url = self.config.pop("schema_registry_url")
            registry_client = SchemaRegistryRemoteBasicClient(registry_url)
        except KeyError:
            log.debug("Registry url not found in config, checking args for registry_client")
            registry_client = config.pop("registry_client")
            assert isinstance(registry_client, karapace.karapace.Karapace), "local client should be Karapace instance"
        self.registry_client = registry_client
        self.subjects_to_schemas = {}
        self.ids_to_schemas = {}
        self.schemas_to_ids = {}
        self.ids_to_writers = {}
        try:
            schemas_folder = config.pop("schemas_folder")
            self._populate_schemas_from_folder(schemas_folder)
        except KeyError:
            pass

    @staticmethod
    def serialize_schema(schema):
        return json.dumps(schema.to_json(), sort_keys=True)

    @staticmethod
    def deserialize_schema(value: str):
        return avro.io.schema.parse(json.loads(value))

    def _populate_schemas_from_folder(self, schemas_folder):
        extension = ".avsc"
        term = self.get_suffix() + extension
        for f in os.listdir(schemas_folder):
            if f.endswith(term):
                with open(os.path.join(schemas_folder, f), 'r') as schema_file:
                    subject = f[:-len(extension)]
                    schema = self.deserialize_schema(schema_file.read())
                    schema_id = self.registry_client.post_new_schema(subject, schema)
                    self.subjects_to_schemas[subject] = schema
                    self.ids_to_schemas[schema_id] = schema
                    self.schemas_to_ids[self.serialize_schema(schema)] = schema_id
            else:
                log.warning("Ignoring file %r in folder %r as it does not terminate in %r", f, schemas_folder, term)
        if not self.subjects_to_schemas:
            log.warning("Folder %r did not contain any valid named schema files", schemas_folder)

    def get_schema_for_topic(self, topic):
        subject = topic + self.get_suffix()
        if subject in self.subjects_to_schemas:
            return self.subjects_to_schemas[subject]
        schema_id, schema = self.registry_client.get_latest_schema(subject)
        self.subjects_to_schemas[subject] = schema
        self.schemas_to_ids[self.serialize_schema(schema)] = schema_id
        self.ids_to_schemas[schema_id] = schema
        self.ids_to_writers[schema_id] = DatumWriter(schema)
        return schema

    def get_suffix(self):
        raise NotImplementedError()

    def serialize(self, topic, value):
        schema = self.get_schema_for_topic(topic)
        schema_id = self.schemas_to_ids[self.serialize_schema(schema)]
        writer = self.ids_to_writers[schema_id]
        with io.BytesIO() as bio:
            enc = BinaryEncoder(bio)
            writer.write(struct.pack(">bI", START_BYTE, schema_id))
            writer.write(value, enc)
            enc_bytes = bio.getvalue()
            return enc_bytes


class SchemaRegistryValueSerializer(SchemaRegistrySerializer):
    def get_suffix(self):
        return "-value"


class SchemaRegistryKeySerializer(SchemaRegistrySerializer):
    def get_suffix(self):
        return "-key"
