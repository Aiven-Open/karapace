"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from enum import Enum, unique
from karapace.errors import InvalidVersion
from typing import ClassVar, Dict, List, Mapping, NewType, Sequence, Union
from typing_extensions import TypeAlias

JsonArray: TypeAlias = List["JsonData"]
JsonObject: TypeAlias = Dict[str, "JsonData"]
JsonScalar: TypeAlias = Union[str, int, float, None]
JsonData: TypeAlias = Union[JsonScalar, JsonObject, JsonArray]

# JSON types suitable as arguments, i.e. using abstract types that don't allow mutation.
ArgJsonArray: TypeAlias = Sequence["ArgJsonData"]
ArgJsonObject: TypeAlias = Mapping[str, "ArgJsonData"]
ArgJsonData: TypeAlias = Union[JsonScalar, ArgJsonObject, ArgJsonArray]

Subject = NewType("Subject", str)
VersionTag = Union[str, int]

# note: the SchemaID is a unique id among all the schemas (and each version should be assigned to a different id)
# basically the same SchemaID refer always to the same TypedSchema.
SchemaId = NewType("SchemaId", int)
TopicName = NewType("TopicName", str)


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


class Version(int):
    LATEST_VERSION_TAG: ClassVar[str] = "latest"
    MINUS_1_VERSION_TAG: ClassVar[int] = -1

    def __new__(cls, version: int) -> Version:
        if not isinstance(version, int):
            raise InvalidVersion(f"Invalid version {version}")
        if (version < cls.MINUS_1_VERSION_TAG) or (version == 0):
            raise InvalidVersion(f"Invalid version {version}")
        return super().__new__(cls, version)

    def __str__(self) -> str:
        return f"{int(self)}"

    def __repr__(self) -> str:
        return f"Version={int(self)}"

    @property
    def is_latest(self) -> bool:
        return self == self.MINUS_1_VERSION_TAG
