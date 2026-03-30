"""
karapace - unit tests for request models

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import pytest
from pydantic import ValidationError

from karapace.api.routers.requests import SchemaRequest


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
