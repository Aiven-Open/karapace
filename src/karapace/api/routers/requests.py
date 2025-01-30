"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.api.routers.errors import KarapaceValidationError
from karapace.core.schema_type import SchemaType
from karapace.core.typing import Subject
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Any


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
    model_config = ConfigDict(extra="forbid")

    @field_validator("schema_str")
    @classmethod
    def validate_schema(cls, schema_str: str) -> str:
        if not schema_str and not schema_str.strip():
            raise KarapaceValidationError(
                error_code=42201,
                error="Empty schema",
            )
        return schema_str


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
