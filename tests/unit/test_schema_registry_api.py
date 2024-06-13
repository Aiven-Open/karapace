"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from aiohttp.test_utils import TestClient, TestServer
from karapace.config import DEFAULTS, set_config_defaults
from karapace.rapu import HTTPResponse
from karapace.schema_reader import KafkaSchemaReader
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.schema_registry_apis import KarapaceSchemaRegistryController
from unittest.mock import ANY, AsyncMock, Mock, patch, PropertyMock

import asyncio


async def test_forward_when_not_ready():
    with patch("karapace.schema_registry_apis.KarapaceSchemaRegistry") as schema_registry_class:
        schema_reader_mock = Mock(spec=KafkaSchemaReader)
        ready_property_mock = PropertyMock(return_value=False)
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        type(schema_reader_mock).ready = ready_property_mock
        schema_registry.schema_reader = schema_reader_mock
        schema_registry_class.return_value = schema_registry

        schema_registry.get_master.return_value = (False, "http://primary-url")

        close_future_result = asyncio.Future()
        close_future_result.set_result(True)
        close_func = Mock()
        close_func.return_value = close_future_result
        schema_registry.close = close_func

        controller = KarapaceSchemaRegistryController(config=set_config_defaults(DEFAULTS))
        mock_forward_func_future = asyncio.Future()
        mock_forward_func_future.set_exception(HTTPResponse({"mock": "response"}))
        mock_forward_func = Mock()
        mock_forward_func.return_value = mock_forward_func_future
        controller._forward_request_remote = mock_forward_func  # pylint: disable=protected-access

        test_server = TestServer(controller.app)
        async with TestClient(test_server) as client:
            await client.get("/schemas/ids/1", headers={"Content-Type": "application/json"})

            ready_property_mock.assert_called_once()
            schema_registry.get_master.assert_called_once()
            mock_forward_func.assert_called_once_with(
                request=ANY, body=None, url="http://primary-url/schemas/ids/1", content_type="application/json", method="GET"
            )
