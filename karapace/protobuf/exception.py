def error(message: str):
    raise Exception(message)


class ProtobufParserRuntimeException(Exception):
    pass


class IllegalStateException(Exception):

    def __init__(self, message="IllegalStateException"):
        self.message = message
        super().__init__(self.message)


class Error(Exception):
    """Base class for errors in this module."""
    pass


class ProtobufException(Error):
    """Generic Avro schema error."""
    pass


class SchemaParseException(ProtobufException):
    """Error while parsing a JSON schema descriptor."""
    pass
