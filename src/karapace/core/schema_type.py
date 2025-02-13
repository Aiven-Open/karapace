"""
karapace - schema_type

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from enum import Enum, unique


@unique
class SchemaType(str, Enum):
    AVRO = "AVRO"
    JSONSCHEMA = "JSON"
    PROTOBUF = "PROTOBUF"
