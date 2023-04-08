"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from avro.errors import SchemaParseException
from avro.schema import parse as avro_parse, Schema as AvroSchema
from dataclasses import dataclass
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError
from karapace.dependency import Dependency
from karapace.errors import InvalidSchema
from karapace.protobuf.exception import (
    Error as ProtobufError,
    IllegalArgumentException,
    IllegalStateException,
    ProtobufException,
    ProtobufUnresolvedDependencyException,
    SchemaParseException as ProtobufSchemaParseException,
)
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_references import Reference
from karapace.schema_type import SchemaType
from karapace.typing import ResolvedVersion, SchemaId, Subject
from karapace.utils import json_decode, json_encode, JSONDecodeError
from typing import Any, cast, Dict, List, NoReturn, Optional, Union

import hashlib
import logging

LOG = logging.getLogger(__name__)


def parse_avro_schema_definition(s: str, validate_enum_symbols: bool = True, validate_names: bool = True) -> AvroSchema:
    """Compatibility function with Avro which ignores trailing data in JSON
    strings.

    The Python stdlib `json` module doesn't allow to ignore trailing data. If
    parsing fails because of it, the extra data can be removed and parsed
    again.
    """
    json_data = json_decode(s)
    return avro_parse(json_encode(json_data), validate_enum_symbols=validate_enum_symbols, validate_names=validate_names)


def parse_jsonschema_definition(schema_definition: str) -> Draft7Validator:
    """Parses and validates `schema_definition`.

    Raises:
        SchemaError: If `schema_definition` is not a valid Draft7 schema.
    """
    schema = json_decode(schema_definition)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def parse_protobuf_schema_definition(
    schema_definition: str,
    references: Optional[List[Reference]] = None,
    dependencies: Optional[Dict[str, Dependency]] = None,
    validate_references: bool = True,
) -> ProtobufSchema:
    """Parses and validates `schema_definition`.

    Raises:
        ProtobufUnresolvedDependencyException if Protobuf dependency cannot be resolved.

    """
    protobuf_schema = ProtobufSchema(schema_definition, references, dependencies)
    if validate_references:
        result = protobuf_schema.verify_schema_dependencies()
        if not result.result:
            raise ProtobufUnresolvedDependencyException(f"{result.message}")
    return protobuf_schema


def _assert_never(no_return: NoReturn) -> NoReturn:
    raise AssertionError(f"Expected to be unreachable {no_return}")


class TypedSchema:
    def __init__(
        self,
        *,
        schema_type: SchemaType,
        schema_str: str,
        schema: Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]] = None,
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):
        """Schema with type information

        Args:
            schema_type (SchemaType): The type of the schema
            schema_str (str): The original schema string
            schema (Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]]): The parsed and validated schema
             references (Optional[List[Dependency]]): The references of schema
        """
        self.schema_type = schema_type
        self.references = references
        self.dependencies = dependencies
        self.schema_str = TypedSchema.normalize_schema_str(schema_str, schema_type, schema)
        self.max_id: Optional[SchemaId] = None
        self._fingerprint_cached: Optional[str] = None
        self._str_cached: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        if self.schema_type is SchemaType.PROTOBUF:
            raise InvalidSchema("Protobuf do not support to_dict serialization")
        return json_decode(self.schema_str, Dict[str, Any])

    def fingerprint(self) -> str:
        if self._fingerprint_cached is None:
            self._fingerprint_cached = hashlib.sha1(str(self).encode("utf8")).hexdigest()
        return self._fingerprint_cached

    @staticmethod
    def normalize_schema_str(
        schema_str: str,
        schema_type: SchemaType,
        schema: Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]] = None,
        # references: Optional[List[Reference]] = None,
        # dependencies: Optional[Dict[str, Dependency]] = None,
    ) -> str:
        if schema_type is SchemaType.AVRO or schema_type is SchemaType.JSONSCHEMA:
            try:
                schema_str = json_encode(json_decode(schema_str), compact=True, sort_keys=True)
            except JSONDecodeError as e:
                LOG.error("Schema is not valid JSON")
                raise e
        elif schema_type == SchemaType.PROTOBUF:
            if schema:
                schema_str = str(schema)
            else:
                try:
                    schema_str = str(parse_protobuf_schema_definition(schema_str, None, None, False))
                except InvalidSchema as e:
                    LOG.exception("Schema is not valid ProtoBuf definition")
                    raise e

        else:
            _assert_never(schema_type)
        return schema_str

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return self.schema_str

        if self._str_cached is None:
            self._str_cached = json_encode(self.to_dict())
        return self._str_cached

    def __repr__(self) -> str:
        return f"TypedSchema(type={self.schema_type}, schema={str(self)})"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, (TypedSchema))
            and self.schema_type is other.schema_type
            and str(self) == str(other)
            and self.references == other.references
        )

    @property
    def schema(self) -> Union[Draft7Validator, AvroSchema, ProtobufSchema]:
        parsed_typed_schema = parse(
            schema_type=self.schema_type,
            schema_str=self.schema_str,
            validate_avro_names=True,
            validate_avro_enum_symbols=True,
            references=self.references,
            dependencies=self.dependencies,
        )
        return parsed_typed_schema.schema


def parse(
    schema_type: SchemaType,
    schema_str: str,
    validate_avro_enum_symbols: bool,
    validate_avro_names: bool,
    references: Optional[List[Reference]] = None,
    dependencies: Optional[Dict[str, Dependency]] = None,
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
        except (SchemaParseException, JSONDecodeError, TypeError) as e:
            raise InvalidSchema from e

    elif schema_type is SchemaType.JSONSCHEMA:
        try:
            parsed_schema = parse_jsonschema_definition(schema_str)
            # TypeError - Raised when the user forgets to encode the schema as a string.
        except (TypeError, JSONDecodeError, SchemaError, AssertionError) as e:
            raise InvalidSchema from e

    elif schema_type is SchemaType.PROTOBUF:
        try:
            parsed_schema = parse_protobuf_schema_definition(schema_str, references, dependencies)
        except (
            TypeError,
            SchemaError,
            AssertionError,
            IllegalStateException,
            IllegalArgumentException,
            ProtobufError,
            ProtobufException,
            ProtobufSchemaParseException,
        ) as e:
            raise InvalidSchema from e
    else:
        raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

    return ParsedTypedSchema(
        schema_type=schema_type,
        schema_str=schema_str,
        schema=parsed_schema,
        references=references,
        dependencies=dependencies,
    )


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

    def __init__(
        self,
        schema_type: SchemaType,
        schema_str: str,
        schema: Union[Draft7Validator, AvroSchema, ProtobufSchema],
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):
        self._schema_cached: Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]] = schema

        super().__init__(
            schema_type=schema_type, schema_str=schema_str, references=references, dependencies=dependencies, schema=schema
        )

    @staticmethod
    def parse(
        schema_type: SchemaType,
        schema_str: str,
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ) -> "ParsedTypedSchema":
        return parse(
            schema_type=schema_type,
            schema_str=schema_str,
            validate_avro_enum_symbols=False,
            validate_avro_names=False,
            references=references,
            dependencies=dependencies,
        )

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return str(self.schema)
        return super().__str__()

    @property
    def schema(self) -> Union[Draft7Validator, AvroSchema, ProtobufSchema]:
        if self._schema_cached is not None:
            return self._schema_cached
        self._schema_cached = super().schema
        return self._schema_cached

    def get_references(self) -> Optional[List[Reference]]:
        return self.references


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

    def __init__(
        self,
        schema_type: SchemaType,
        schema_str: str,
        schema: Union[Draft7Validator, AvroSchema, ProtobufSchema],
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ):
        super().__init__(
            schema_type=schema_type, schema_str=schema_str, references=references, dependencies=dependencies, schema=schema
        )

    @staticmethod
    def parse(
        schema_type: SchemaType,
        schema_str: str,
        references: Optional[List[Reference]] = None,
        dependencies: Optional[Dict[str, Dependency]] = None,
    ) -> "ValidatedTypedSchema":
        parsed_schema = parse(
            schema_type=schema_type,
            schema_str=schema_str,
            validate_avro_enum_symbols=True,
            validate_avro_names=True,
            references=references,
            dependencies=dependencies,
        )

        return cast(ValidatedTypedSchema, parsed_schema)


@dataclass
class SchemaVersion:
    subject: Subject
    version: ResolvedVersion
    deleted: bool
    schema_id: SchemaId
    schema: TypedSchema
    references: Optional[List[Reference]]
