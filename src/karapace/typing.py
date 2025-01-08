"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Mapping, Sequence
from enum import Enum, unique
from karapace.errors import InvalidVersion
from pydantic import ValidationInfo
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
    # TODO[pydantic]: We couldn't refactor `__get_validators__`, please create the `__get_pydantic_core_schema__` manually.
    # Check https://docs.pydantic.dev/latest/migration/#defining-custom-types for more information.
    def __get_validators__(cls) -> Generator[Callable[[str, ValidationInfo], str], None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, subject_str: str, _: ValidationInfo) -> str:
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
