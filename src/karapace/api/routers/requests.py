"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.api.routers.errors import KarapaceValidationError, SchemaErrorCodes, SchemaErrorMessages
from karapace.core.schema_type import SchemaType
from karapace.core.typing import Subject
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Any, Final

# Schema ids and versions are int32 on the wire.
MAX_INT32: Final = 2**31 - 1


def _is_valid_int32_id(value: Any) -> bool:
    # bool is an int in Python, so JSON `true` would otherwise validate as 1.
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_INT32


class SchemaReference(BaseModel):
    name: str
    subject: Subject
    version: int


class SchemaRequest(BaseModel):
    schema_str: str = Field(alias="schema")
    schema_type: SchemaType = Field(alias="schemaType", default=SchemaType.AVRO)
    references: list[SchemaReference] | None = None
    metadata: Any | None = None
    ruleSet: Any | None = None
    schema_id: int | None = Field(alias="id", default=None)
    schema_version: int | None = Field(alias="version", default=None)
    model_config = ConfigDict(extra="ignore")

    @field_validator("schema_str")
    @classmethod
    def validate_schema(cls, schema_str: str) -> str:
        if not schema_str and not schema_str.strip():
            raise KarapaceValidationError(
                error_code=42201,
                error="Empty schema",
            )
        return schema_str

    # mode="before" so non-integer input also gets a Karapace shaped error body.
    @field_validator("schema_id", mode="before")
    @classmethod
    def validate_schema_id(cls, schema_id: Any) -> Any:
        if schema_id is not None and not _is_valid_int32_id(schema_id):
            raise KarapaceValidationError(
                error_code=SchemaErrorCodes.INVALID_SCHEMA_ID.value,
                error=SchemaErrorMessages.INVALID_SCHEMA_ID_RANGE_FMT.value.format(schema_id=schema_id),
            )
        return schema_id

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version(cls, version: Any) -> Any:
        if version is not None and not _is_valid_int32_id(version):
            raise KarapaceValidationError(
                error_code=SchemaErrorCodes.INVALID_VERSION_ID.value,
                error=SchemaErrorMessages.INVALID_VERSION_RANGE_FMT.value.format(version=version),
            )
        return version


class SchemaResponse(BaseModel):
    subject: Subject
    version: int
    schema_id: int = Field(alias="id")
    schema_str: str = Field(alias="schema")
    schema_type: SchemaType | None = Field(alias="schemaType", default=None)


class SchemasResponse(BaseModel):
    schema_str: str = Field(alias="schema")
    subjects: list[Subject] | None = None
    schema_type: SchemaType | None = Field(alias="schemaType", default=None)
    references: list[Any] | None = None  # TODO: typing
    maxId: int | None = None


class SchemaListingItem(BaseModel):
    subject: Subject
    schema_str: str = Field(alias="schema")
    version: int
    schema_id: int = Field(alias="id")
    schema_type: SchemaType | None = Field(alias="schemaType", default=None)
    references: list[Any] | None = None


class SchemaIdResponse(BaseModel):
    schema_id: int = Field(alias="id")


class CompatibilityRequest(BaseModel):
    compatibility: str


class CompatibilityResponse(BaseModel):
    compatibility: str


class CompatibilityLevelResponse(BaseModel):
    compatibility_level: str = Field(alias="compatibilityLevel")


class CompatibilityCheckResponse(BaseModel):
    is_compatible: bool
    messages: list[str] | None = None


class ModeResponse(BaseModel):
    mode: str


class ModeUpdateRequest(BaseModel):
    mode: str


class SubjectVersion(BaseModel):
    subject: Subject
    version: int


class SubjectSchemaVersionResponse(BaseModel):
    subject: Subject
    version: int
    schema_id: int = Field(alias="id")
    schema_str: str = Field(alias="schema")
    references: list[Any] | None = None
    schema_type: SchemaType | None = Field(alias="schemaType", default=None)
    compatibility: str | None = None
