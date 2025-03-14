"""
karapace schema_references

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Mapping
from karapace.core.dataclasses import default_dataclass
from karapace.core.typing import JsonData, JsonObject, SchemaId, Subject, Version
from typing import cast, NewType, TypeVar

Referents = NewType("Referents", set[SchemaId])

T = TypeVar("T")


def _read_typed(
    data: Mapping[str, object],
    key: str,
    value_type: type[T],
) -> T:
    value = data[key]
    if not isinstance(value, value_type):
        raise TypeError(f"Expected key `name` to contain value of type {value_type.__name__!r}, found {type(value)!r}.")
    return value


# Represents a reference with an alias to latest version, where the version has
# not yet been determined. Representing this as a separate entity gives us nice
# static type checking semantics, and means we cannot pass an unresolved object
# where a resolved one is expected.
@default_dataclass
class LatestVersionReference:
    name: str
    subject: Subject

    def resolve(self, version: Version) -> Reference:
        return Reference(
            name=self.name,
            subject=self.subject,
            version=version,
        )


@default_dataclass
class Reference:
    name: str
    subject: Subject
    version: Version

    def __post_init__(self) -> None:
        assert self.version != Version.MINUS_1_VERSION_TAG

    def __repr__(self) -> str:
        return f"{{name='{self.name}', subject='{self.subject}', version={self.version}}}"

    def to_dict(self) -> JsonData:
        return {
            "name": self.name,
            "subject": self.subject,
            "version": self.version.value,
        }

    @staticmethod
    def from_dict(data: JsonObject) -> Reference:
        return Reference(
            name=str(data["name"]),
            subject=Subject(str(data["subject"])),
            version=Version(cast(int, data["version"])),
        )


# TODO remove
def reference_from_mapping(
    data: Mapping[str, object],
) -> Reference | LatestVersionReference:
    name = _read_typed(data, "name", str)
    subject = Subject(_read_typed(data, "subject", str))
    version = _read_typed(data, "version", int)
    return (
        LatestVersionReference(
            name=name,
            subject=subject,
        )
        # -1 is alias to latest version
        if version == -1
        else Reference(
            name=name,
            subject=subject,
            version=Version(version),
        )
    )
