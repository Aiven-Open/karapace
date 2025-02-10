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
from karapace.typing import PrimaryInfo
from unittest.mock import AsyncMock, Mock, patch, PropertyMock

import asyncio


async def test_forwarding_not_a_primary_and_own_primary_url() -> None:
    with patch("karapace.schema_registry_apis.KarapaceSchemaRegistry") as schema_registry_class:
        schema_reader_mock = Mock(spec=KafkaSchemaReader)
        ready_property_mock = PropertyMock(return_value=lambda: False)
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        type(schema_reader_mock).ready = ready_property_mock
        schema_registry.schema_reader = schema_reader_mock
        schema_registry_class.return_value = schema_registry

        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url=None)

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
            await client.put("/config/foo", headers={"Content-Type": "application/json"}, json={"compatibility": "FORWARD"})

            ready_property_mock.assert_called_once()
            schema_registry.get_master.assert_called_once()
            mock_forward_func.assert_not_called()


async def test_forwarding_to_a_primary() -> None:
    with patch("karapace.schema_registry_apis.KarapaceSchemaRegistry") as schema_registry_class:
        schema_reader_mock = Mock(spec=KafkaSchemaReader)
        ready_property_mock = PropertyMock(return_value=lambda: False)
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        type(schema_reader_mock).ready = ready_property_mock
        schema_registry.schema_reader = schema_reader_mock
        schema_registry_class.return_value = schema_registry

        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://127.0.0.1:8082")

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
            await client.put("/config/foo", headers={"Content-Type": "application/json"}, json={"compatibility": "FORWARD"})

            ready_property_mock.assert_called_once()
            schema_registry.get_master.assert_called_once()
            mock_forward_func.assert_called_once()


async def test_no_forwarding_as_instance_is_primary() -> None:
    with patch("karapace.schema_registry_apis.KarapaceSchemaRegistry") as schema_registry_class:
        schema_reader_mock = Mock(spec=KafkaSchemaReader)
        ready_property_mock = PropertyMock(return_value=lambda: False)
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        type(schema_reader_mock).ready = ready_property_mock
        schema_registry.schema_reader = schema_reader_mock
        schema_registry_class.return_value = schema_registry

        schema_registry.get_master.return_value = PrimaryInfo(primary=True, primary_url=None)

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
            await client.put("/config/foo", headers={"Content-Type": "application/json"}, json={"compatibility": "FORWARD"})

            ready_property_mock.assert_called_once()
            schema_registry.get_master.assert_called_once()
            mock_forward_func.assert_not_called()
