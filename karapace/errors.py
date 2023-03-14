"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.typing import Version
from typing import List


class VersionNotFoundException(Exception):
    pass


class InvalidVersion(Exception):
    pass


class IncompatibleSchema(Exception):
    pass


class InvalidSchema(Exception):
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
    def __init__(self, referenced_by: List, version: Version):
        super().__init__()
        self.version = version
        self.referenced_by = referenced_by


class SubjectSoftDeletedException(Exception):
    pass


class SchemaTooLargeException(Exception):
    pass
