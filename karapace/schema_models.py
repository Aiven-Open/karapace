from avro.errors import SchemaParseException
from avro.schema import parse as avro_parse, Schema as AvroSchema
from enum import Enum, unique
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError
from karapace.protobuf.exception import (
    Error as ProtobufError,
    IllegalArgumentException,
    IllegalStateException,
    ProtobufException,
    ProtobufParserRuntimeException,
    SchemaParseException as ProtobufSchemaParseException,
)
from karapace.protobuf.schema import ProtobufSchema
from karapace.utils import json_encode
from typing import Any, Dict, Union

import json
import ujson


def parse_avro_schema_definition(s: str) -> AvroSchema:
    """Compatibility function with Avro which ignores trailing data in JSON
    strings.

    The Python stdlib `json` module doesn't allow to ignore trailing data. If
    parsing fails because of it, the extra data can be removed and parsed
    again.
    """
    try:
        json_data = json.loads(s)
    except json.JSONDecodeError as e:
        if e.msg != "Extra data":
            raise

        json_data = json.loads(s[: e.pos])

    return avro_parse(json.dumps(json_data))


def parse_jsonschema_definition(schema_definition: str) -> Draft7Validator:
    """Parses and validates `schema_definition`.

    Raises:
        SchemaError: If `schema_definition` is not a valid Draft7 schema.
    """
    schema = ujson.loads(schema_definition)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def parse_protobuf_schema_definition(schema_definition: str) -> ProtobufSchema:
    """Parses and validates `schema_definition`.

    Raises:
        Nothing yet.

    """

    return ProtobufSchema(schema_definition)


class InvalidSchemaType(Exception):
    pass


class InvalidSchema(Exception):
    pass


@unique
class SchemaType(str, Enum):
    AVRO = "AVRO"
    JSONSCHEMA = "JSON"
    PROTOBUF = "PROTOBUF"


class TypedSchema:
    def __init__(self, schema_type: SchemaType, schema_str: str):
        """Schema with type information

        Args:
            schema_type (SchemaType): The type of the schema
            schema_str (str): The original schema string
        """
        self.schema_type = schema_type
        self.schema_str = schema_str

    def to_dict(self) -> Dict[str, Any]:
        if self.schema_type is SchemaType.PROTOBUF:
            raise InvalidSchema("Protobuf do not support to_dict serialization")
        return ujson.loads(self.schema_str)

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return self.schema_str
        return json_encode(self.to_dict())

    def __repr__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return f"TypedSchema(type={self.schema_type}, schema={str(self)})"
        return f"TypedSchema(type={self.schema_type}, schema={json_encode(self.to_dict())})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, TypedSchema) and self.__str__() == other.__str__() and self.schema_type is other.schema_type


class ValidatedTypedSchema(TypedSchema):
    def __init__(self, schema_type: SchemaType, schema_str: str, schema: Union[Draft7Validator, AvroSchema, ProtobufSchema]):
        super().__init__(schema_type=schema_type, schema_str=schema_str)
        self.schema = schema

    @staticmethod
    def parse(schema_type: SchemaType, schema_str: str) -> "ValidatedTypedSchema":
        if schema_type not in [SchemaType.AVRO, SchemaType.JSONSCHEMA, SchemaType.PROTOBUF]:
            raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

        parsed_schema: Union[Draft7Validator, AvroSchema, ProtobufSchema]
        if schema_type is SchemaType.AVRO:
            try:
                parsed_schema = parse_avro_schema_definition(schema_str)
            except (SchemaParseException, ValueError, TypeError) as e:
                raise InvalidSchema from e

        elif schema_type is SchemaType.JSONSCHEMA:
            try:
                parsed_schema = parse_jsonschema_definition(schema_str)
                # TypeError - Raised when the user forgets to encode the schema as a string.
            except (TypeError, ValueError, SchemaError, AssertionError) as e:
                raise InvalidSchema from e

        elif schema_type is SchemaType.PROTOBUF:
            try:
                parsed_schema = parse_protobuf_schema_definition(schema_str)
            except (
                TypeError,
                SchemaError,
                AssertionError,
                ProtobufParserRuntimeException,
                IllegalStateException,
                IllegalArgumentException,
                ProtobufError,
                ProtobufException,
                ProtobufSchemaParseException,
            ) as e:
                raise InvalidSchema from e
        else:
            raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

        return ValidatedTypedSchema(schema_type=schema_type, schema_str=schema_str, schema=parsed_schema)

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return str(self.schema)
        return super().__str__()
