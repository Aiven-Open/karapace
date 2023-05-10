"""
karapace schema_references

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.typing import JsonData, ResolvedVersion, SchemaId, Subject
from typing import List, Mapping, NewType, TypeVar
from typing_extensions import Self

Referents = NewType("Referents", List[SchemaId])


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


class Reference:
    def __init__(
        self,
        name: str,
        subject: Subject,
        version: ResolvedVersion,
    ) -> None:
        self.name = name
        self.subject = subject
        self.version = version

    def to_dict(self) -> JsonData:
        return {
            "name": self.name,
            "subject": self.subject,
            "version": self.version,
        }

    @classmethod
    def from_mapping(cls, data: Mapping[str, object]) -> Self:
        return cls(
            name=_read_typed(data, "name", str),
            subject=Subject(_read_typed(data, "subject", str)),
            version=ResolvedVersion(_read_typed(data, "version", int)),
        )

    def __repr__(self) -> str:
        return f"{{name='{self.name}', subject='{self.subject}', version={self.version}}}"

    def __hash__(self) -> int:
        return hash((self.name, self.subject, self.version))

    def __eq__(self, other: object) -> bool:
        if other is None or not isinstance(other, Reference):
            return False
        return self.name == other.name and self.subject == other.subject and self.version == other.version
