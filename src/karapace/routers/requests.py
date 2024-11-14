"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.schema_type import SchemaType
from pydantic import BaseModel, Field
from typing import Any


class SchemaRequest(BaseModel):
    schema_str: str = Field(alias="schema")
    schema_type: SchemaType = Field(alias="schemaType", default=SchemaType.AVRO)
    references: list[Any] = Field(default_factory=list)
    metadata: Any | None
    ruleSet: Any | None


class SchemaResponse(BaseModel):
    subject: str
    version: int
    schema_id: int = Field(alias="id")
    schema_str: str = Field(alias="schema")
    schema_type: SchemaType | None = Field(alias="schemaType", default=None)


class SchemasResponse(BaseModel):
    schema_str: str = Field(alias="schema")
    subjects: list[str] | None = None
    schema_type: SchemaType | None = None
    references: list[Any] | None = None  # TODO: typing
    maxId: int | None = None


class SchemaListingItem(BaseModel):
    subject: str
    schema_str: str = Field(alias="schema")
    version: int
    schema_id: int = Field(alias="id")
    schemaType: str
    references: list[Any] | None


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
    subject: str
    version: int


class SubjectSchemaVersionResponse(BaseModel):
    subject: str
    version: int
    schema_id: int = Field(alias="id")
    schema_str: str = Field(alias="schema")
    references: list[Any] | None = None
    schema_type: str | None = None
    compatibility: str | None = None
