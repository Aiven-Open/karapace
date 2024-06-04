"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from karapace.errors import InvalidVersion, VersionNotFoundException
from karapace.schema_models import SchemaVersion
from typing import ClassVar, Mapping, Union

VersionTag = Union[str, int]


class Version(int):
    LATEST_VERSION_TAG: ClassVar[str] = "latest"
    MINUS_1_VERSION_TAG: ClassVar[int] = -1

    @property
    def is_latest(self) -> bool:
        return self == self.MINUS_1_VERSION_TAG

    def from_schema_versions(self, schema_versions: Mapping[Version, SchemaVersion]) -> Version:
        max_version = max(schema_versions)
        if self.is_latest:
            return max_version
        if self <= max_version and self in schema_versions:
            return self
        raise VersionNotFoundException()

    @classmethod
    def resolve_tag(cls, tag: VersionTag) -> int:
        return cls.MINUS_1_VERSION_TAG if tag == cls.LATEST_VERSION_TAG else int(tag)

    @classmethod
    def V(cls, tag: VersionTag) -> Version:
        cls.validate_tag(tag=tag)
        return Version(version=Version.resolve_tag(tag))

    @classmethod
    def validate_tag(cls, tag: VersionTag) -> None:
        try:
            version = cls.resolve_tag(tag=tag)
            if (version < cls.MINUS_1_VERSION_TAG) or (version == 0):
                raise InvalidVersion(f"Invalid version {tag}")
        except ValueError as exc:
            if tag != cls.LATEST_VERSION_TAG:
                raise InvalidVersion(f"Invalid version {tag}") from exc

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
