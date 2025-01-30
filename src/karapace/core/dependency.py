"""
karapace - dependency

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.core.errors import InvalidSchema
from karapace.core.protobuf.protopace.protopace import Proto
from karapace.core.schema_references import Reference
from karapace.core.schema_type import SchemaType
from karapace.core.typing import JsonData, Subject, Version
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.core.schema_models import ValidatedTypedSchema


class DependencyVerifierResult:
    def __init__(self, result: bool, message: str | None = "") -> None:
        self.result = result
        self.message = message


class Dependency:
    def __init__(
        self,
        name: str,
        subject: Subject,
        version: Version,
        target_schema: ValidatedTypedSchema,
    ) -> None:
        self.name = name
        self.subject = subject
        self.version = version
        self.schema = target_schema

    def get_schema(self) -> ValidatedTypedSchema:
        return self.schema

    @staticmethod
    def of(reference: Reference, target_schema: ValidatedTypedSchema) -> Dependency:
        return Dependency(reference.name, reference.subject, reference.version, target_schema)

    def to_proto(self) -> Proto:
        if self.schema.schema_type is not SchemaType.PROTOBUF:
            raise InvalidSchema("Only supported for Protobuf")

        dependencies = []
        if self.schema.dependencies:
            for _, dep in self.schema.dependencies.items():
                dependencies.append(dep.to_proto())
        return Proto(self.name, self.schema.schema_str, dependencies)

    def to_dict(self) -> JsonData:
        return {
            "name": self.name,
            "subject": self.subject,
            "version": self.version.value,
        }

    def identifier(self) -> str:
        return self.name + "_" + self.subject + "_" + str(self.version)

    def __hash__(self) -> int:
        return hash((self.name, self.subject, self.version, self.schema))

    def __eq__(self, other: object) -> bool:
        if other is None or not isinstance(other, Dependency):
            return False
        return (
            self.name == other.name
            and self.subject == other.subject
            and self.version == other.version
            and self.schema == other.schema
        )
