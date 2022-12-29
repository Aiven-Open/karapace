from avro.errors import SchemaParseException
from avro.schema import parse as avro_parse, Schema as AvroSchema
from enum import Enum, unique
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError
from karapace.errors import InvalidSchema
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
from typing import Any, cast, Dict, Optional, Union

import json


def parse_avro_schema_definition(s: str, validate_enum_symbols: bool = True, validate_names: bool = True) -> AvroSchema:
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

    return avro_parse(json.dumps(json_data), validate_enum_symbols=validate_enum_symbols, validate_names=validate_names)


def parse_jsonschema_definition(schema_definition: str) -> Draft7Validator:
    """Parses and validates `schema_definition`.

    Raises:
        SchemaError: If `schema_definition` is not a valid Draft7 schema.
    """
    schema = json.loads(schema_definition)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def parse_protobuf_schema_definition(schema_definition: str) -> ProtobufSchema:
    """Parses and validates `schema_definition`.

    Raises:
        Nothing yet.

    """

    return ProtobufSchema(schema_definition)


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
        self.max_id: Optional[int] = None

        self._str_cached: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        if self.schema_type is SchemaType.PROTOBUF:
            raise InvalidSchema("Protobuf do not support to_dict serialization")
        return json.loads(self.schema_str)

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return self.schema_str

        if self._str_cached is None:
            self._str_cached = json_encode(self.to_dict())
        return self._str_cached

    def __repr__(self) -> str:
        return f"TypedSchema(type={self.schema_type}, schema={str(self)})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, TypedSchema) and self.__str__() == other.__str__() and self.schema_type is other.schema_type


def parse(
    schema_type: SchemaType,
    schema_str: str,
    validate_avro_enum_symbols: bool,
    validate_avro_names: bool,
) -> "ParsedTypedSchema":
    if schema_type not in [SchemaType.AVRO, SchemaType.JSONSCHEMA, SchemaType.PROTOBUF]:
        raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

    parsed_schema: Union[Draft7Validator, AvroSchema, ProtobufSchema]
    if schema_type is SchemaType.AVRO:
        try:
            parsed_schema = parse_avro_schema_definition(
                schema_str,
                validate_enum_symbols=validate_avro_enum_symbols,
                validate_names=validate_avro_names,
            )
        except (SchemaParseException, json.JSONDecodeError, TypeError) as e:
            raise InvalidSchema from e

    elif schema_type is SchemaType.JSONSCHEMA:
        try:
            parsed_schema = parse_jsonschema_definition(schema_str)
            # TypeError - Raised when the user forgets to encode the schema as a string.
        except (TypeError, json.JSONDecodeError, SchemaError, AssertionError) as e:
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

    return ParsedTypedSchema(schema_type=schema_type, schema_str=schema_str, schema=parsed_schema)


class ParsedTypedSchema(TypedSchema):
    """Parsed but unvalidated schema resource.

    This class is used when reading and parsing existing schemas from data store. The intent of this class is to provide
    representation of the schema which can be used to compare existing versions with new version in compatibility check
    and when storing new version.

    This class shall not be used for new schemas received through the public API.

    The intent of this class is not to bypass validation of the syntax of the schema.
    Assumption is that schema is syntactically correct.

    Validations that are bypassed:
     * AVRO: enumeration symbols, namespace and name validity.

    Existing schemas may have been produced with backing schema SDKs that may have passed validation on schemas that
    are considered by the current version of the SDK invalid.
    """

    def __init__(self, schema_type: SchemaType, schema_str: str, schema: Union[Draft7Validator, AvroSchema, ProtobufSchema]):
        super().__init__(schema_type=schema_type, schema_str=schema_str)
        self.schema = schema

    @staticmethod
    def parse(schema_type: SchemaType, schema_str: str) -> "ParsedTypedSchema":
        return parse(
            schema_type=schema_type,
            schema_str=schema_str,
            validate_avro_enum_symbols=False,
            validate_avro_names=False,
        )

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return str(self.schema)
        return super().__str__()


class ValidatedTypedSchema(ParsedTypedSchema):
    """Validated schema resource.

    This class is used when receiving a new schema from through the public API. The intent of this class is to
    provide validation of the schema.
    This class shall not be used when reading and parsing existing schemas.

    The intent of this class is not to validate the syntax of the schema.
    Assumption is that schema is syntactically correct.

    Existing schemas may have been produced with backing schema SDKs that may have passed validation on schemas that
    are considered by the current version of the SDK invalid.
    """

    def __init__(self, schema_type: SchemaType, schema_str: str, schema: Union[Draft7Validator, AvroSchema, ProtobufSchema]):
        super().__init__(schema_type=schema_type, schema_str=schema_str, schema=schema)

    @staticmethod
    def parse(schema_type: SchemaType, schema_str: str) -> "ValidatedTypedSchema":
        parsed_schema = parse(
            schema_type=schema_type,
            schema_str=schema_str,
            validate_avro_enum_symbols=True,
            validate_avro_names=True,
        )
        return cast(ValidatedTypedSchema, parsed_schema)
