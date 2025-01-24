"""
karapace - test request forwarding in router

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from fastapi import Request
from fastapi.exceptions import HTTPException
from karapace.core.forward_client import ForwardClient
from karapace.core.typing import PrimaryInfo
from schema_registry.controller import KarapaceSchemaRegistryController
from schema_registry.registry import KarapaceSchemaRegistry
from schema_registry.routers.config import config_put
from unittest.mock import AsyncMock, Mock

from schema_registry.routers.requests import CompatibilityRequest

import pytest


async def test_forwarding_not_a_primary_and_own_primary_url() -> None:
    compatibility_request = CompatibilityRequest(compatibility="FORWARD")
    forward_client_mock = AsyncMock(spec=ForwardClient)
    schema_registry_mock = AsyncMock(spec=KarapaceSchemaRegistry)
    schema_registry_mock.get_master.return_value = PrimaryInfo(primary=False, primary_url=None)

    with pytest.raises(HTTPException):
        await config_put(
            request=Mock(spec=Request),
            compatibility_level_request=compatibility_request,
            schema_registry=schema_registry_mock,
            user=None,
            forward_client=forward_client_mock,
            authorizer=None,
            controller=None,
        )

    forward_client_mock.forward_request_remote.assert_not_called()


async def test_forwarding_to_a_primary() -> None:
    compatibility_request = CompatibilityRequest(compatibility="FORWARD")
    forward_client_mock = AsyncMock(spec=ForwardClient)
    schema_registry_mock = AsyncMock(spec=KarapaceSchemaRegistry)
    schema_registry_mock.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://127.0.0.1:8082")

    await config_put(
        request=Mock(spec=Request),
        compatibility_level_request=compatibility_request,
        schema_registry=schema_registry_mock,
        user=None,
        forward_client=forward_client_mock,
        authorizer=None,
        controller=None,
    )

    forward_client_mock.forward_request_remote.assert_called_once()


async def test_no_forwarding_as_instance_is_primary() -> None:
    compatibility_request = CompatibilityRequest(compatibility="FORWARD")
    forward_client_mock = AsyncMock(spec=ForwardClient)
    controller_mock = AsyncMock(spec=KarapaceSchemaRegistryController)
    schema_registry_mock = AsyncMock(spec=KarapaceSchemaRegistry)
    schema_registry_mock.get_master.return_value = PrimaryInfo(primary=True, primary_url=None)

    await config_put(
        request=Mock(spec=Request),
        compatibility_level_request=compatibility_request,
        schema_registry=schema_registry_mock,
        user=None,
        forward_client=forward_client_mock,
        authorizer=None,
        controller=controller_mock,
    )

    controller_mock.config_set.assert_called_once()
    forward_client_mock.forward_request_remote.assert_not_called()
