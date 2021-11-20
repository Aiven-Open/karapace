import json


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


class ProtobufTypeException(Error):
    """Generic Protobuf type error."""


class SchemaParseException(ProtobufException):
    """Error while parsing a Protobuf schema descriptor."""


class ProtobufSchemaResolutionException(ProtobufException):
    def __init__(self, fail_msg, writer_schema=None, reader_schema=None):
        writer_dump = json.dumps(json.loads(str(writer_schema)), indent=2)
        reader_dump = json.dumps(json.loads(str(reader_schema)), indent=2)
        if writer_schema:
            fail_msg += "\nWriter's Schema: %s" % writer_dump
        if reader_schema:
            fail_msg += "\nReader's Schema: %s" % reader_dump
        ProtobufException.__init__(self, fail_msg)
