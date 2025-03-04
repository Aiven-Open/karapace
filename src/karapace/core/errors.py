"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.core.schema_references import Referents
    from karapace.core.typing import Version


class VersionNotFoundException(Exception):
    pass


class InvalidVersion(Exception):
    pass


class IncompatibleSchema(Exception):
    pass


class InvalidSchema(Exception):
    pass


class InvalidTest(Exception):
    pass


class InvalidSchemaType(Exception):
    pass


class InvalidReferences(Exception):
    pass


class SchemasNotFoundException(Exception):
    pass


class SchemaVersionSoftDeletedException(Exception):
    pass


class SchemaVersionNotSoftDeletedException(Exception):
    pass


class SubjectNotFoundException(Exception):
    pass


class SubjectNotSoftDeletedException(Exception):
    pass


class ReferenceExistsException(Exception):
    def __init__(self, referenced_by: Referents, version: Version) -> None:
        super().__init__()
        self.referenced_by = referenced_by
        self.version = version


class SubjectSoftDeletedException(Exception):
    pass


class SchemaTooLargeException(Exception):
    pass


class ShutdownException(Exception):
    """Raised when the service has encountered an error where it should not continue and shutdown."""


class CorruptKafkaRecordException(ShutdownException):
    """
    Raised when a corrupt schema is present in the `_schemas` topic. This should halt the service as
    we will end up with a corrupt state and could lead to various runtime issues and data mismatch.
    """
