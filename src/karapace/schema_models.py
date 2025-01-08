"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from avro.errors import SchemaParseException
from avro.schema import parse as avro_parse, Schema as AvroSchema
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError
from karapace.dependency import Dependency
from karapace.errors import InvalidSchema, InvalidVersion, VersionNotFoundException
from karapace.protobuf import protopace
from karapace.protobuf.exception import (
    Error as ProtobufError,
    IllegalArgumentException,
    IllegalStateException,
    ProtobufException,
    ProtobufUnresolvedDependencyException,
    SchemaParseException as ProtobufSchemaParseException,
)
from karapace.protobuf.proto_normalizations import NormalizedProtobufSchema
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_references import Reference
from karapace.schema_type import SchemaType
from karapace.typing import JsonObject, SchemaId, Subject, Version, VersionTag
from karapace.utils import assert_never, json_decode, json_encode, JSONDecodeError
from typing import Any, cast, Final, final

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
    # TODO: Annotations dictate Mapping[str, Any] here, but we have unit tests that
    #  use bool values and fail if we assert isinstance(_, dict).
    Draft7Validator.check_schema(schema)  # type: ignore[arg-type]
    return Draft7Validator(schema)  # type: ignore[arg-type]


def _format_protobuf(schema: str, dependencies: Collection[Dependency], name: str = "schema.proto") -> str:
    deps = [dep.to_proto() for dep in dependencies]
    proto = protopace.Proto(name, schema, deps)
    return protopace.format_proto(proto)


def parse_protobuf_schema_definition(
    schema_definition: str,
    references: Sequence[Reference] | None = None,
    dependencies: Mapping[str, Dependency] | None = None,
    validate_references: bool = True,
    normalize: bool = False,
    use_protobuf_formatter: bool = False,
) -> ProtobufSchema:
    """Parses and validates `schema_definition`.

    Raises:
        ProtobufUnresolvedDependencyException if Protobuf dependency cannot be resolved.

    """
    if use_protobuf_formatter:
        try:
            schema_definition = _format_protobuf(schema_definition, dependencies.values() if dependencies else [])
        except InvalidSchema as err:
            LOG.error("Error formatting schema %s", err)

    protobuf_schema = (
        ProtobufSchema(schema_definition, references, dependencies)
        if not normalize
        else NormalizedProtobufSchema.from_protobuf_schema(ProtobufSchema(schema_definition, references, dependencies))
    )
    if validate_references:
        result = protobuf_schema.verify_schema_dependencies()
        if not result.result:
            raise ProtobufUnresolvedDependencyException(f"{result.message}")

    return protobuf_schema


class TypedSchema:
    def __init__(
        self,
        *,
        schema_type: SchemaType,
        schema_str: str,
        schema: Draft7Validator | AvroSchema | ProtobufSchema | None = None,
        references: Sequence[Reference] | None = None,
        dependencies: Mapping[str, Dependency] | None = None,
    ) -> None:
        """Schema with type information

        Args:
            schema_type (SchemaType): The type of the schema
            schema_str (str): The original schema string
            schema (Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]]): The parsed and validated schema
            references (Optional[List[Dependency]]): The references of schema
        """
        self.schema_type: Final = schema_type
        self.references: Final = references
        self.dependencies: Final = dependencies
        self.schema_str: Final = TypedSchema.normalize_schema_str(schema_str, schema_type, schema)
        self.max_id: SchemaId | None = None
        self._fingerprint_cached: str | None = None

    def to_dict(self) -> JsonObject:
        if self.schema_type is SchemaType.PROTOBUF:
            raise InvalidSchema("Protobuf do not support to_dict serialization")
        return json_decode(self.schema_str, dict[str, Any])

    def fingerprint(self) -> str:
        if self._fingerprint_cached is None:
            fingerprint_str = str(self)
            if self.references is not None:
                reference_str = "\n".join([repr(reference) for reference in self.references])
                fingerprint_str = fingerprint_str + reference_str
            self._fingerprint_cached = hashlib.sha1(fingerprint_str.encode("utf8")).hexdigest()
        return self._fingerprint_cached

    # This is marked @final because __init__ references this statically, hence
    # allowing overriding this in a subclass could lead to confusing bugs.
    @staticmethod
    @final
    def normalize_schema_str(
        schema_str: str,
        schema_type: SchemaType,
        schema: Draft7Validator | AvroSchema | ProtobufSchema | None = None,
    ) -> str:
        if schema_type is SchemaType.AVRO or schema_type is SchemaType.JSONSCHEMA:
            try:
                schema_str = json_encode(json_decode(schema_str), compact=True, sort_keys=True)
            except JSONDecodeError as e:
                LOG.info("Schema is not valid JSON")
                raise e
        elif schema_type == SchemaType.PROTOBUF:
            if schema:
                schema_str = str(schema)
            else:
                try:
                    schema_str = str(parse_protobuf_schema_definition(schema_str, None, None, False))
                except InvalidSchema as e:
                    LOG.info("Schema is not valid ProtoBuf definition")
                    raise e

        else:
            assert_never(schema_type)
        return schema_str

    def __str__(self) -> str:
        return self.schema_str

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
    def schema(self) -> Draft7Validator | AvroSchema | ProtobufSchema:
        parsed_typed_schema = parse(
            schema_type=self.schema_type,
            schema_str=self.schema_str,
            validate_avro_names=True,
            validate_avro_enum_symbols=True,
            references=self.references,
            dependencies=self.dependencies,
            normalize=False,
        )
        return parsed_typed_schema.schema


def parse(
    schema_type: SchemaType,
    schema_str: str,
    validate_avro_enum_symbols: bool,
    validate_avro_names: bool,
    references: Sequence[Reference] | None = None,
    dependencies: Mapping[str, Dependency] | None = None,
    normalize: bool = False,
    use_protobuf_formatter: bool = False,
) -> ParsedTypedSchema:
    if schema_type not in [SchemaType.AVRO, SchemaType.JSONSCHEMA, SchemaType.PROTOBUF]:
        raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

    parsed_schema: Draft7Validator | AvroSchema | ProtobufSchema
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
            parsed_schema = parse_protobuf_schema_definition(
                schema_str,
                references,
                dependencies,
                validate_references=True,
                normalize=normalize,
                use_protobuf_formatter=use_protobuf_formatter,
            )
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
        schema: Draft7Validator | AvroSchema | ProtobufSchema,
        references: Sequence[Reference] | None = None,
        dependencies: Mapping[str, Dependency] | None = None,
    ) -> None:
        self._schema_cached: Draft7Validator | AvroSchema | ProtobufSchema | None = schema

        super().__init__(
            schema_type=schema_type,
            schema_str=schema_str,
            references=references,
            dependencies=dependencies,
            schema=schema,
        )

    @staticmethod
    def parse(
        schema_type: SchemaType,
        schema_str: str,
        references: Sequence[Reference] | None = None,
        dependencies: Mapping[str, Dependency] | None = None,
        normalize: bool = False,
        use_protobuf_formatter: bool = False,
    ) -> ParsedTypedSchema:
        return parse(
            schema_type=schema_type,
            schema_str=schema_str,
            validate_avro_enum_symbols=False,
            validate_avro_names=False,
            references=references,
            dependencies=dependencies,
            normalize=normalize,
            use_protobuf_formatter=use_protobuf_formatter,
        )

    def __str__(self) -> str:
        if self.schema_type == SchemaType.PROTOBUF:
            return str(self.schema)
        return super().__str__()

    def match(self, other: ParsedTypedSchema) -> bool:
        """Match the schema with given one.

        Special case function where schema is matched to other. The parsed schema object and references are matched.
        The parent class equality function works based on the normalized schema string. That does not take into account
        the canonical forms of any schema type. This function uses the parsed form of the schema to match if schemas
        are equal. For example Avro schemas `{"type": "int", "name": schema_name}` and `{"type": "int"}` are equal by
        Avro spec.
        References are also matched and the refered schemas and the versions of those must match.

        :param other: The schema to match against.
        :return: True if schema match, False if not.
        """
        return self.schema_type is other.schema_type and self.schema == other.schema and self.references == other.references

    @property
    def schema(self) -> Draft7Validator | AvroSchema | ProtobufSchema:
        if self._schema_cached is not None:
            return self._schema_cached
        self._schema_cached = super().schema
        return self._schema_cached

    def get_references(self) -> Sequence[Reference] | None:
        return self.references

    def serialize(self) -> str:
        if isinstance(self.schema, ProtobufSchema):
            return self.schema.serialize()
        return self.schema_str


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
        schema: Draft7Validator | AvroSchema | ProtobufSchema,
        references: list[Reference] | None = None,
        dependencies: dict[str, Dependency] | None = None,
    ) -> None:
        super().__init__(
            schema_type=schema_type,
            schema_str=schema_str,
            references=references,
            dependencies=dependencies,
            schema=schema,
        )

    @staticmethod
    def parse(
        schema_type: SchemaType,
        schema_str: str,
        references: Sequence[Reference] | None = None,
        dependencies: Mapping[str, Dependency] | None = None,
        normalize: bool = False,
        use_protobuf_formatter: bool = False,
    ) -> ValidatedTypedSchema:
        parsed_schema = parse(
            schema_type=schema_type,
            schema_str=schema_str,
            validate_avro_enum_symbols=True,
            validate_avro_names=True,
            references=references,
            dependencies=dependencies,
            normalize=normalize,
            use_protobuf_formatter=use_protobuf_formatter,
        )

        return cast(ValidatedTypedSchema, parsed_schema)


@dataclass
class SchemaVersion:
    subject: Subject
    version: Version
    deleted: bool
    schema_id: SchemaId
    schema: TypedSchema
    references: Sequence[Reference] | None


class Versioner:
    @classmethod
    def V(cls, tag: VersionTag) -> Version:
        cls.validate_tag(tag=tag)
        return Version(version=cls.resolve_tag(tag))

    @classmethod
    def validate_tag(cls, tag: VersionTag) -> None:
        try:
            version = cls.resolve_tag(tag=tag)
            if (version < Version.MINUS_1_VERSION_TAG) or (version == 0):
                raise InvalidVersion(f"Invalid version {tag}")
        except ValueError as exc:
            if tag != Version.LATEST_VERSION_TAG:
                raise InvalidVersion(f"Invalid version {tag}") from exc

    @staticmethod
    def resolve_tag(tag: VersionTag) -> int:
        return Version.MINUS_1_VERSION_TAG if tag == Version.LATEST_VERSION_TAG else int(tag)

    @staticmethod
    def from_schema_versions(schema_versions: Mapping[Version, SchemaVersion], version: Version) -> Version:
        max_version = max(schema_versions)
        if version.is_latest:
            return max_version
        if version in schema_versions and version <= max_version:
            return version
        raise VersionNotFoundException()
