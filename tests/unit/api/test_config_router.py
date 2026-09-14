"""
Tests for the `/config` FastAPI router (``karapace.api.routers.config``).

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException, Request

from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.forward_client import ForwardClient
from karapace.api.routers.config import (
    config_delete_subject,
    config_get,
    config_get_subject,
    config_put,
    config_set_subject,
)
from karapace.api.routers.requests import CompatibilityRequest
from karapace.core.auth import AuthenticatorAndAuthorizer, Operation
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.typing import PrimaryInfo, Subject

SUBJECT = Subject("test-subject")


def _denying_authorizer(operation: Operation) -> AuthenticatorAndAuthorizer:
    authorizer = MagicMock(spec=AuthenticatorAndAuthorizer)
    authorizer.check_authorization = MagicMock(side_effect=lambda _user, op, _res: op != operation)
    return authorizer


class TestConfigGet:
    async def test_returns_global_config_when_authorized(self) -> None:
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await config_get(user=None, authorizer=None, controller=controller)

        controller.config_get.assert_called_once_with()
        assert result is controller.config_get.return_value

    async def test_denied_read_raises_unauthorized(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await config_get(
                user=None,
                authorizer=_denying_authorizer(Operation.Read),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 403


class TestConfigPut:
    async def test_denied_write_raises_unauthorized(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await config_put(
                request=MagicMock(spec=Request),
                compatibility_level_request=CompatibilityRequest(compatibility="FULL"),
                user=None,
                schema_registry=AsyncMock(spec=KarapaceSchemaRegistry),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=_denying_authorizer(Operation.Write),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 403

    async def test_forwards_to_primary_when_not_primary_itself(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://primary:8081")
        forward_client = AsyncMock(spec=ForwardClient)

        result = await config_put(
            request=MagicMock(spec=Request),
            compatibility_level_request=CompatibilityRequest(compatibility="FULL"),
            user=None,
            schema_registry=schema_registry,
            forward_client=forward_client,
            authorizer=None,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert result is forward_client.forward_request_remote.return_value

    async def test_sets_config_when_primary(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=True, primary_url=None)
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await config_put(
            request=MagicMock(spec=Request),
            compatibility_level_request=CompatibilityRequest(compatibility="FULL"),
            user=None,
            schema_registry=schema_registry,
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            controller=controller,
        )

        controller.config_set.assert_called_once()
        assert result is controller.config_set.return_value

    async def test_raises_no_primary_url_error_when_no_primary_known(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url=None)

        with pytest.raises(HTTPException) as exc_info:
            await config_put(
                request=MagicMock(spec=Request),
                compatibility_level_request=CompatibilityRequest(compatibility="FULL"),
                user=None,
                schema_registry=schema_registry,
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500


class TestConfigGetSubject:
    async def test_returns_subject_config_when_authorized(self) -> None:
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await config_get_subject(
            subject=SUBJECT,
            user=None,
            defaultToGlobal=True,
            authorizer=None,
            controller=controller,
        )

        controller.config_subject_get.assert_called_once_with(subject=SUBJECT, default_to_global=True)
        assert result is controller.config_subject_get.return_value

    async def test_denied_read_raises_subject_not_found(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await config_get_subject(
                subject=SUBJECT,
                user=None,
                defaultToGlobal=False,
                authorizer=_denying_authorizer(Operation.Read),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 404


class TestConfigSetSubject:
    async def test_sets_subject_config_when_primary(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=True, primary_url=None)
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await config_set_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            compatibility_level_request=CompatibilityRequest(compatibility="NONE"),
            user=None,
            schema_registry=schema_registry,
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            controller=controller,
        )

        controller.config_subject_set.assert_called_once()
        assert result is controller.config_subject_set.return_value

    async def test_forwards_to_primary_when_not_primary_itself(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://primary:8081")
        forward_client = AsyncMock(spec=ForwardClient)

        result = await config_set_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            compatibility_level_request=CompatibilityRequest(compatibility="NONE"),
            user=None,
            schema_registry=schema_registry,
            forward_client=forward_client,
            authorizer=None,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert result is forward_client.forward_request_remote.return_value

    async def test_raises_no_primary_url_error_when_no_primary_known(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url=None)

        with pytest.raises(HTTPException) as exc_info:
            await config_set_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                compatibility_level_request=CompatibilityRequest(compatibility="NONE"),
                user=None,
                schema_registry=schema_registry,
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500

    async def test_denied_write_raises_subject_not_found(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await config_set_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                compatibility_level_request=CompatibilityRequest(compatibility="NONE"),
                user=None,
                schema_registry=AsyncMock(spec=KarapaceSchemaRegistry),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=_denying_authorizer(Operation.Write),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 404


class TestConfigDeleteSubject:
    async def test_deletes_subject_config_when_primary(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=True, primary_url=None)
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await config_delete_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            user=None,
            schema_registry=schema_registry,
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            controller=controller,
        )

        controller.config_subject_delete.assert_called_once_with(subject=SUBJECT)
        assert result is controller.config_subject_delete.return_value

    async def test_forwards_to_primary_when_not_primary_itself(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://primary:8081")
        forward_client = AsyncMock(spec=ForwardClient)

        result = await config_delete_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            user=None,
            schema_registry=schema_registry,
            forward_client=forward_client,
            authorizer=None,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert result is forward_client.forward_request_remote.return_value

    async def test_raises_no_primary_url_error_when_no_primary_known(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url=None)

        with pytest.raises(HTTPException) as exc_info:
            await config_delete_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                user=None,
                schema_registry=schema_registry,
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500

    async def test_denied_write_raises_subject_not_found(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await config_delete_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                user=None,
                schema_registry=AsyncMock(spec=KarapaceSchemaRegistry),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=_denying_authorizer(Operation.Write),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 404
