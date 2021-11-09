def error(message: str) -> None:
    raise Exception(message)


class ProtobufParserRuntimeException(Exception):
    pass


class IllegalStateException(Exception):
    def __init__(self, message="IllegalStateException"):
        self.message = message
        super().__init__(self.message)


class IllegalArgumentException(Exception):
    def __init__(self, message="IllegalArgumentException"):
        self.message = message
        super().__init__(self.message)


class Error(Exception):
    """Base class for errors in this module."""


class ProtobufException(Error):
    """Generic Protobuf schema error."""


class SchemaParseException(ProtobufException):
    """Error while parsing a Protobuf schema descriptor."""
