from avro.errors import SchemaParseException
from avro.schema import parse as avro_parse, Schema as AvroSchema
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError
from karapace.errors import InvalidSchema
from karapace.protobuf.exception import (
    Error as ProtobufError,
    IllegalArgumentException,
    IllegalStateException,
    ProtobufException,
    ProtobufParserRuntimeException,
    ProtobufUnresolvedDependencyException,
    SchemaParseException as ProtobufSchemaParseException,
)
from karapace.protobuf.schema import Dependency, ProtobufSchema
from karapace.schema_references import References
from karapace.schema_type import SchemaType
from karapace.utils import json_encode
from typing import Any, Dict, Optional, Union

import json


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
    schema = json.loads(schema_definition)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def parse_protobuf_schema_definition(
    schema_definition: str, references: Optional[References] = None, dependencies: Optional[Dict[str, Dependency]] = None
) -> ProtobufSchema:
    """Parses and validates `schema_definition`.

    Raises:
        Nothing yet.

    """
    protobuf_schema = ProtobufSchema(schema_definition, references, dependencies)
    result = protobuf_schema.verify_schema_dependencies()
    if not result.result:
        raise ProtobufUnresolvedDependencyException(f"{result.message}")
    return protobuf_schema


class TypedSchema:
    def __init__(
        self,
        schema_type: SchemaType,
        schema_str: str,
        references: Optional[References] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):
        """Schema with type information

        Args:
            schema_type (SchemaType): The type of the schema
            schema_str (str): The original schema string
            references(References): The references of schema
        """
        self.schema_type = schema_type
        self.schema_str = schema_str
        self.references = references
        self.dependencies = dependencies
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

    def get_references(self) -> Optional["References"]:
        return self.references

    def __eq__(self, other: Any) -> bool:
        schema_is_equal = (
            isinstance(other, TypedSchema) and self.schema_type is other.schema_type and self.__str__() == other.__str__()
        )
        if not schema_is_equal:
            return False
        if self.references is not None:
            return self.references == other.references
        return other.references is None


class ValidatedTypedSchema(TypedSchema):
    def __init__(
        self,
        schema_type: SchemaType,
        schema_str: str,
        schema: Union[Draft7Validator, AvroSchema, ProtobufSchema],
        references: Optional["References"] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):
        super().__init__(schema_type=schema_type, schema_str=schema_str, references=references, dependencies=dependencies)
        self.schema = schema

    @staticmethod
    def parse(
        schema_type: SchemaType,
        schema_str: str,
        references: Optional["References"] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ) -> "ValidatedTypedSchema":
        if schema_type not in [SchemaType.AVRO, SchemaType.JSONSCHEMA, SchemaType.PROTOBUF]:
            raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

        parsed_schema: Union[Draft7Validator, AvroSchema, ProtobufSchema]
        if schema_type is SchemaType.AVRO:
            try:
                parsed_schema = parse_avro_schema_definition(schema_str)
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
                parsed_schema = parse_protobuf_schema_definition(schema_str, references, dependencies)
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

        return ValidatedTypedSchema(
            schema_type=schema_type,
            schema_str=schema_str,
            schema=parsed_schema,
            references=references,
            dependencies=dependencies,
        )

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return str(self.schema)
        return super().__str__()
