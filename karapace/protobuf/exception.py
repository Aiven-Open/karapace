import json


class ProtobufParserRuntimeException(Exception):
    pass


class IllegalStateException(Exception):
    pass


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


def pretty_print_json(obj: str) -> str:
    return json.dumps(json.loads(obj), indent=2)


class ProtobufSchemaResolutionException(ProtobufException):
    def __init__(self, fail_msg: str, writer_schema=None, reader_schema=None) -> None:
        writer_dump = pretty_print_json(str(writer_schema))
        reader_dump = pretty_print_json(str(reader_schema))
        if writer_schema:
            fail_msg += "\nWriter's Schema: %s" % writer_dump
        if reader_schema:
            fail_msg += "\nReader's Schema: %s" % reader_dump
        super().__init__(self, fail_msg)
