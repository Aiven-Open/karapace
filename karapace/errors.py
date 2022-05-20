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


class SchemaTooLargeException(Exception):
    pass
