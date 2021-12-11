import json


class ProtobufParserRuntimeException(Exception):
    pass


class IllegalStateException(Exception):
    def __init__(self, message="IllegalStateException") -> None:
        self.message = message
        super().__init__(self.message)


class IllegalArgumentException(Exception):
    def __init__(self, message="IllegalArgumentException") -> None:
        self.message = message
        super().__init__(self.message)


class Error(Exception):
    """Base class for errors in this module."""


class ProtobufException(Error):
    """Generic Protobuf schema error."""


class ProtobufTypeException(Error):
    """Generic Protobuf type error."""


class SchemaParseException(ProtobufException):
    """Error while parsing a Protobuf schema descriptor."""


class ProtobufSchemaResolutionException(ProtobufException):
    def __init__(self, fail_msg: str, writer_schema=None, reader_schema=None) -> None:
        writer_dump = json.dumps(json.loads(str(writer_schema)), indent=2)
        reader_dump = json.dumps(json.loads(str(reader_schema)), indent=2)
        if writer_schema:
            fail_msg += "\nWriter's Schema: %s" % writer_dump
        if reader_schema:
            fail_msg += "\nReader's Schema: %s" % reader_dump
        super().__init__(self, fail_msg)
