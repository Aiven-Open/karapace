"""
karapace - test request forwarding in router
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from fastapi import Request
from fastapi.exceptions import HTTPException
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.forward_client import ForwardClient
from karapace.api.routers.config import config_put
from karapace.api.routers.requests import CompatibilityRequest
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.typing import PrimaryInfo
from unittest.mock import AsyncMock, Mock

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


async def test_forwarding_invalid_compatibility_from_leader() -> None:
    """Test that follower properly handles invalid compatibility settings error from leader."""
    from fastapi import HTTPException, status
    from karapace.api.routers.errors import SchemaErrorCodes, SchemaErrorMessages

    compatibility_request = CompatibilityRequest(compatibility="INVALID_MODE")
    forward_client_mock = AsyncMock(spec=ForwardClient)
    schema_registry_mock = AsyncMock(spec=KarapaceSchemaRegistry)
    schema_registry_mock.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://127.0.0.1:8082")

    # Simulate leader returning error response for invalid compatibility
    error_response = {
        "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
        "message": SchemaErrorMessages.INVALID_COMPATIBILITY_LEVEL.value,
    }

    forward_client_mock.forward_request_remote.side_effect = HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=error_response,
    )

    with pytest.raises(HTTPException) as exc_info:
        await config_put(
            request=Mock(spec=Request),
            compatibility_level_request=compatibility_request,
            schema_registry=schema_registry_mock,
            user=None,
            forward_client=forward_client_mock,
            authorizer=None,
            controller=None,
        )

    # Verify the error is properly propagated
    assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value
    assert exc_info.value.detail["message"] == SchemaErrorMessages.INVALID_COMPATIBILITY_LEVEL.value
    forward_client_mock.forward_request_remote.assert_called_once()


async def test_forwarding_invalid_compatibility_subject_from_leader() -> None:
    """Test that follower properly handles invalid compatibility settings error for subject from leader."""
    from fastapi import HTTPException, status
    from karapace.api.routers.errors import SchemaErrorCodes
    from karapace.api.routers.config import config_set_subject

    compatibility_request = CompatibilityRequest(compatibility="RANDOM_STRING")
    forward_client_mock = AsyncMock(spec=ForwardClient)
    schema_registry_mock = AsyncMock(spec=KarapaceSchemaRegistry)
    schema_registry_mock.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://127.0.0.1:8082")

    # Simulate leader returning error response for invalid compatibility
    error_response = {
        "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
        "message": "Invalid compatibility level",
    }

    forward_client_mock.forward_request_remote.side_effect = HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=error_response,
    )

    with pytest.raises(HTTPException) as exc_info:
        await config_set_subject(
            request=Mock(spec=Request),
            subject="test-subject",
            compatibility_level_request=compatibility_request,
            user=None,
            schema_registry=schema_registry_mock,
            forward_client=forward_client_mock,
            authorizer=None,
            controller=None,
        )

    # Verify the error is properly propagated
    assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert exc_info.value.detail["error_code"] == SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value
    forward_client_mock.forward_request_remote.assert_called_once()
