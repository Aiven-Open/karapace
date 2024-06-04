"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.schema_references import Referents
    from karapace.typing import Version


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
