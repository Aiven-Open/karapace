"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.schema_references import Referents
from karapace.typing import ResolvedVersion


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
    def __init__(self, referenced_by: Referents, version: ResolvedVersion) -> None:
        super().__init__()
        self.version = version
        self.referenced_by = referenced_by


class SubjectSoftDeletedException(Exception):
    pass


class SchemaTooLargeException(Exception):
    pass
