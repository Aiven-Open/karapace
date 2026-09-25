"""
karapace - unit tests for request models

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from karapace.api.http_handlers import setup_exception_handlers
from karapace.api.routers.errors import KarapaceValidationError, SchemaErrorCodes
from karapace.api.routers.requests import MAX_INT32, SchemaRequest


class TestSchemaRequestExtraFields:
    """Extra fields must be silently ignored to match v4 behavior.

    v4 parsed the request body as a plain dict and only read the keys it
    needed.  The 5.x Pydantic model must not reject unknown keys so that
    existing clients that send extra properties (e.g. ``compatibility``)
    continue to work after upgrading.
    """

    def test_extra_fields_are_ignored(self) -> None:
        req = SchemaRequest.model_validate(
            {
                "schema": '{"type": "string"}',
                "compatibility": "BACKWARD",
            }
        )
        assert req.schema_str == '{"type": "string"}'
        assert not hasattr(req, "compatibility")

    def test_multiple_extra_fields_are_ignored(self) -> None:
        req = SchemaRequest.model_validate(
            {
                "schema": '{"type": "string"}',
                "compatibility": "BACKWARD",
                "unknown_prop": 123,
                "another": True,
            }
        )
        assert req.schema_str == '{"type": "string"}'

    def test_required_field_still_validated(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SchemaRequest.model_validate(
                {
                    "compatibility": "BACKWARD",
                }
            )
        errors = exc_info.value.errors()
        assert any(e["type"] == "missing" for e in errors)

    def test_valid_request_with_all_fields(self) -> None:
        req = SchemaRequest.model_validate(
            {
                "schema": '{"type": "string"}',
                "schemaType": "AVRO",
                "references": None,
            }
        )
        assert req.schema_str == '{"type": "string"}'
        assert req.schema_type.value == "AVRO"


class TestSchemaRequestImportFields:
    """``id`` and ``version`` are only meaningful in IMPORT mode, and are bounded to int32."""

    def test_absent_fields_default_to_none(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}'})
        assert req.schema_id is None
        assert req.schema_version is None

    def test_fields_are_read_from_aliases(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "id": 5, "version": 3})
        assert req.schema_id == 5
        assert req.schema_version == 3

    def test_snake_case_names_are_ignored(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "schema_id": 5, "schema_version": 3})
        assert req.schema_id is None
        assert req.schema_version is None

    def test_explicit_null_is_accepted(self) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "id": None, "version": None})
        assert req.schema_id is None
        assert req.schema_version is None

    @pytest.mark.parametrize("value", [1, MAX_INT32])
    def test_boundary_values_are_accepted(self, value: int) -> None:
        req = SchemaRequest.model_validate({"schema": '{"type": "string"}', "id": value, "version": value})
        assert req.schema_id == value
        assert req.schema_version == value

    @pytest.mark.parametrize("value", [0, -1, MAX_INT32 + 1, 2**63, True, "3", 1.5])
    def test_out_of_range_id_is_rejected(self, value: object) -> None:
        with pytest.raises(KarapaceValidationError) as exc_info:
            SchemaRequest.model_validate({"schema": '{"type": "string"}', "id": value})
        assert exc_info.value.error_code == SchemaErrorCodes.INVALID_SCHEMA_ID.value
        assert str(value) in exc_info.value.body

    @pytest.mark.parametrize("value", [0, -1, MAX_INT32 + 1, True, "latest", 1.5])
    def test_out_of_range_version_is_rejected(self, value: object) -> None:
        with pytest.raises(KarapaceValidationError) as exc_info:
            SchemaRequest.model_validate({"schema": '{"type": "string"}', "version": value})
        assert exc_info.value.error_code == SchemaErrorCodes.INVALID_VERSION_ID.value
        assert str(value) in exc_info.value.body

    @pytest.mark.parametrize(
        ("body", "expected_error_code"),
        [
            ({"schema": '{"type": "string"}', "id": 0}, SchemaErrorCodes.INVALID_SCHEMA_ID.value),
            ({"schema": '{"type": "string"}', "version": 0}, SchemaErrorCodes.INVALID_VERSION_ID.value),
        ],
    )
    def test_rejection_renders_a_karapace_error_body(self, body: dict, expected_error_code: int) -> None:
        """The body must carry error_code and a string message, not a pydantic error list."""
        app = FastAPI()
        setup_exception_handlers(app=app)

        @app.post("/subjects/{subject}/versions")
        async def subject_post(subject: str, schema_request: SchemaRequest) -> dict:
            return {"id": 1}

        response = TestClient(app).post("/subjects/s/versions", json=body)

        assert response.status_code == 422
        assert response.json()["error_code"] == expected_error_code
        assert isinstance(response.json()["message"], str)
