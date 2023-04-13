"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from typing import Any, Dict, List, Mapping, Sequence, Union
from typing_extensions import TypeAlias

JsonArray: TypeAlias = List["JsonData"]
JsonObject: TypeAlias = Dict[str, "JsonData"]
JsonScalar: TypeAlias = Union[str, int, float, None]
JsonData: TypeAlias = Union[JsonScalar, JsonObject, JsonArray]

# JSON types suitable as arguments, i.e. using abstract types that don't allow mutation.
ArgJsonArray: TypeAlias = Sequence["ArgJsonData"]
ArgJsonObject: TypeAlias = Mapping[str, "ArgJsonData"]
ArgJsonData: TypeAlias = Union[JsonScalar, ArgJsonObject, ArgJsonArray]

Subject: TypeAlias = str
Version: TypeAlias = Union[int, str]
ResolvedVersion: TypeAlias = int
SchemaId: TypeAlias = int
Schema = Dict[str, Any]
