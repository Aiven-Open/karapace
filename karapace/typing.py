"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from enum import Enum, unique
from typing import Dict, List, Mapping, NewType, Sequence, Union
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
Version = Union[int, str]
ResolvedVersion = NewType("ResolvedVersion", int)
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
    # value it's a property of the Enum class, avoiding the collision.
    value_ = "value"
    # partition it's a function of `str` and StrEnum its inherits from it.
    partition_ = "partition"
