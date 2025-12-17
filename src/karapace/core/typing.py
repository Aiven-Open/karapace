"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum, unique
from karapace.core.errors import InvalidVersion
from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler, ValidationInfo
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from typing import Any, ClassVar, NewType, TypeAlias, Union

import functools

JsonArray: TypeAlias = list["JsonData"]
JsonObject: TypeAlias = dict[str, "JsonData"]
JsonScalar: TypeAlias = Union[str, int, float, None]
JsonData: TypeAlias = Union[JsonScalar, JsonObject, JsonArray]

# JSON types suitable as arguments, i.e. using abstract types that don't allow mutation.
ArgJsonArray: TypeAlias = Sequence["ArgJsonData"]
ArgJsonObject: TypeAlias = Mapping[str, "ArgJsonData"]
ArgJsonData: TypeAlias = Union[JsonScalar, ArgJsonObject, ArgJsonArray]

VersionTag = Union[str, int]
SchemaMetadata = NewType("SchemaMetadata", dict[str, Any])
SchemaRuleSet = NewType("SchemaRuleSet", dict[str, Any])

# note: the SchemaID is a unique id among all the schemas (and each version should be assigned to a different id)
# basically the same SchemaID refer always to the same TypedSchema.
SchemaId = NewType("SchemaId", int)
TopicName = NewType("TopicName", str)


class Subject(str):
    @classmethod
    def __get_pydantic_core_schema__(cls, source: type[Any], handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
        # Validate as a string and then apply our custom validator; serialize back to string.
        return core_schema.no_info_after_validator_function(
            cls.validate,
            core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema)

    @classmethod
    def __get_validators__(cls) -> Generator[Callable[[str, ValidationInfo], str], None, None]:
        # Kept for backward compatibility with pydantic v1 style validation.
        yield cls.validate

    @classmethod
    def validate(cls, subject_str: str, _: ValidationInfo | None = None) -> str:
        """Subject may not contain control characters."""
        if bool([c for c in subject_str if (ord(c) <= 31 or (ord(c) >= 127 and ord(c) <= 159))]):
            raise ValueError(f"The specified subject '{subject_str}' is not a valid.")
        return subject_str


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return str(self.value)


@unique
class ElectionStrategy(Enum):
    highest = "highest"
    lowest = "lowest"


@unique
class NameStrategy(StrEnum, Enum):
    topic_name = "topic_name"
    record_name = "record_name"
    topic_record_name = "topic_record_name"


@unique
class SubjectType(StrEnum, Enum):
    key = "key"
    value = "value"
    # partition it's a function of `str` and StrEnum its inherits from it.
    partition_ = "partition"


@unique
class Mode(StrEnum):
    readwrite = "READWRITE"


@functools.total_ordering
class Version:
    LATEST_VERSION_TAG: ClassVar[str] = "latest"
    MINUS_1_VERSION_TAG: ClassVar[int] = -1

    def __init__(self, version: int) -> None:
        if not isinstance(version, int):
            raise InvalidVersion(f"Invalid version {version}")
        if version < Version.MINUS_1_VERSION_TAG:
            raise InvalidVersion(f"Invalid version {version}")
        self._value = version

    def __str__(self) -> str:
        return f"{int(self._value)}"

    def __repr__(self) -> str:
        return f"Version({int(self._value)})"

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Version):
            return self._value < other.value
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Version):
            return self._value == other.value
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)

    @property
    def value(self) -> int:
        return self._value

    @property
    def is_latest(self) -> bool:
        return self.value == self.MINUS_1_VERSION_TAG


class SchemaReaderStoppper(ABC):
    @abstractmethod
    def ready(self) -> bool:
        pass

    @abstractmethod
    def set_not_ready(self) -> None:
        pass


@dataclass(frozen=True)
class PrimaryInfo:
    primary: bool
    primary_url: str | None
