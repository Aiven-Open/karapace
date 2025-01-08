"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from fastapi.exceptions import HTTPException
from karapace.rapu import HTTPResponse
from karapace.schema_models import SchemaType, ValidatedTypedSchema
from schema_registry.container import SchemaRegistryContainer
from schema_registry.reader import KafkaSchemaReader
from unittest.mock import Mock, patch, PropertyMock

import asyncio
import json
import pytest

TYPED_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.data",
            "name": "Test",
            "type": "record",
            "fields": [
                {
                    "name": "attr1",
                    "type": ["null", "string"],
                }
            ],
        }
    ),
)


async def test_validate_schema_request_body(schema_registry_container: SchemaRegistryContainer) -> None:
    schema_registry_container.schema_registry_controller()._validate_schema_type(  # pylint: disable=W0212
        {"schema": "{}", "schemaType": "JSON", "references": [], "metadata": {}, "ruleSet": {}}
    )

    with pytest.raises(HTTPException) as exc_info:
        schema_registry_container.schema_registry_controller()._validate_schema_type(  # pylint: disable=W0212
            {"schema": "{}", "schemaType": "DOES_NOT_EXIST", "references": [], "unexpected_field_name": {}, "ruleSet": {}},
        )
    assert exc_info.type is HTTPException
    assert str(exc_info.value) == "422: {'error_code': 422, 'message': 'Invalid schemaType DOES_NOT_EXIST'}"


async def test_forward_when_not_ready(schema_registry_container: SchemaRegistryContainer) -> None:
    with patch("schema_registry.container.KarapaceSchemaRegistry") as schema_registry_class:
        schema_reader_mock = Mock(spec=KafkaSchemaReader)
        ready_property_mock = PropertyMock(return_value=False)
        type(schema_reader_mock).ready = ready_property_mock
        schema_registry_class.schema_reader = schema_reader_mock

        schema_registry_class.schemas_get.return_value = TYPED_AVRO_SCHEMA
        schema_registry_class.get_master.return_value = (False, "http://primary-url")

        close_future_result = asyncio.Future()
        close_future_result.set_result(True)
        close_func = Mock()
        close_func.return_value = close_future_result
        schema_registry_class.close = close_func

        controller = schema_registry_container.schema_registry_controller()
        controller.schema_registry = schema_registry_class

        mock_forward_func_future = asyncio.Future()
        mock_forward_func_future.set_exception(HTTPResponse({"mock": "response"}))
        mock_forward_func = Mock()
        mock_forward_func.return_value = mock_forward_func_future
        controller._forward_request_remote = mock_forward_func  # pylint: disable=protected-access

        assert await controller.schemas_get(
            schema_id=1,
            include_subjects=False,
            fetch_max_id=False,
            format_serialized="",
            user=None,
            authorizer=None,
        )
        with pytest.raises(HTTPResponse):
            # prevent `future exception was never retrieved` warning logs
            # future: <Future finished exception=HTTPResponse(status=200 body={'mock': 'response'})>
            await mock_forward_func_future
