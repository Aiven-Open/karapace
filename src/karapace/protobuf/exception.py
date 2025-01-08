"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import json

if TYPE_CHECKING:
    from karapace.protobuf.schema import ProtobufSchema


class Error(Exception):
    """Base class for errors in this module."""


class ProtobufException(Error):
    """Generic Protobuf schema error."""


class ProtobufTypeException(ProtobufException):
    """Generic Protobuf type error."""


class IllegalStateException(ProtobufException):
    pass


class IllegalArgumentException(ProtobufException):
    pass


class ProtobufUnresolvedDependencyException(ProtobufException):
    """a Protobuf schema has unresolved dependency"""


class SchemaParseException(ProtobufException):
    """Error while parsing a Protobuf schema descriptor."""


def pretty_print_json(obj: str) -> str:
    return json.dumps(json.loads(obj), indent=2)


class ProtobufSchemaResolutionException(ProtobufException):
    def __init__(self, fail_msg: str, writer_schema: ProtobufSchema, reader_schema: ProtobufSchema) -> None:
        writer_dump = pretty_print_json(str(writer_schema))
        reader_dump = pretty_print_json(str(reader_schema))
        if writer_schema:
            fail_msg += "\nWriter's Schema: %s" % writer_dump
        if reader_schema:
            fail_msg += "\nReader's Schema: %s" % reader_dump
        super().__init__(self, fail_msg)
