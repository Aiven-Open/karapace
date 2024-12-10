"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from abc import ABC, abstractmethod
from cachetools import TTLCache
from collections.abc import MutableMapping
from karapace.schema_models import TypedSchema
from karapace.typing import SchemaId, Subject
from typing import Final

import hashlib


class SchemaCacheProtocol(ABC):
    @abstractmethod
    def get_schema_id(self, schema: TypedSchema) -> SchemaId | None:
        pass

    @abstractmethod
    def has_schema_id(self, schema_id: SchemaId) -> bool:
        pass

    @abstractmethod
    def set_schema(self, schema_id: SchemaId, schema: TypedSchema) -> None:
        pass

    @abstractmethod
    def get_schema(self, schema_id: SchemaId) -> TypedSchema | None:
        pass

    @abstractmethod
    def get_schema_str(self, schema_id: SchemaId) -> str | None:
        pass


class TopicSchemaCache:
    def __init__(self) -> None:
        self._topic_cache: dict[Subject, SchemaCache] = {}
        self._empty_schema_cache: Final = EmptySchemaCache()

    def get_schema_id(self, topic: Subject, schema: TypedSchema) -> SchemaId | None:
        return self._topic_cache.get(topic, self._empty_schema_cache).get_schema_id(schema)

    def has_schema_id(self, topic: Subject, schema_id: SchemaId) -> bool:
        return self._topic_cache.get(topic, self._empty_schema_cache).has_schema_id(schema_id)

    def set_schema(self, topic: str, schema_id: SchemaId, schema: TypedSchema) -> None:
        schema_cache_with_defaults = self._topic_cache.setdefault(Subject(topic), SchemaCache())
        schema_cache_with_defaults.set_schema(schema_id, schema)

    def get_schema(self, topic: Subject, schema_id: SchemaId) -> TypedSchema | None:
        schema_cache = self._topic_cache.get(topic, self._empty_schema_cache)
        return schema_cache.get_schema(schema_id)

    def get_schema_str(self, topic: Subject, schema_id: SchemaId) -> str | None:
        schema_cache = self._topic_cache.get(topic, self._empty_schema_cache)
        return schema_cache.get_schema_str(schema_id)


class SchemaCache(SchemaCacheProtocol):
    def __init__(self) -> None:
        self._schema_hash_str_to_id: dict[str, SchemaId] = {}
        self._id_to_schema_str: MutableMapping[SchemaId, TypedSchema] = TTLCache(maxsize=100, ttl=600)

    def get_schema_id(self, schema: TypedSchema) -> SchemaId | None:
        fingerprint = hashlib.sha1(str(schema).encode("utf8")).hexdigest()

        maybe_id = self._schema_hash_str_to_id.get(fingerprint)

        if maybe_id is not None and maybe_id not in self._id_to_schema_str:
            del self._schema_hash_str_to_id[fingerprint]
            return None

        return maybe_id

    def has_schema_id(self, schema_id: SchemaId) -> bool:
        return schema_id in self._id_to_schema_str

    def set_schema(self, schema_id: SchemaId, schema: TypedSchema) -> None:
        fingerprint = hashlib.sha1(str(schema).encode("utf8")).hexdigest()
        self._schema_hash_str_to_id[fingerprint] = schema_id
        self._id_to_schema_str[schema_id] = schema

    def get_schema(self, schema_id: SchemaId) -> TypedSchema | None:
        return self._id_to_schema_str.get(schema_id)

    def get_schema_str(self, schema_id: SchemaId) -> str | None:
        maybe_schema = self.get_schema(schema_id)
        return None if maybe_schema is None else str(maybe_schema)


class EmptySchemaCache(SchemaCacheProtocol):
    def get_schema_id(self, schema: TypedSchema) -> None:
        return None

    def has_schema_id(self, schema_id: SchemaId) -> bool:
        return False

    def set_schema(self, schema_id: SchemaId, schema: TypedSchema) -> None:
        raise NotImplementedError("Empty schema cache. Cannot set schemas.")

    def get_schema(self, schema_id: SchemaId) -> None:
        return None

    def get_schema_str(self, schema_id: SchemaId) -> None:
        return None
