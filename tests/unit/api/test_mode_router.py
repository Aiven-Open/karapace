"""
Tests for the `/mode` FastAPI router (``karapace.api.routers.mode``).

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException, Request

from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.forward_client import ForwardClient
from karapace.api.routers.mode import (
    mode_delete_subject,
    mode_get,
    mode_get_subject,
    mode_put,
    mode_put_subject,
)
from karapace.api.routers.requests import ModeResponse, ModeUpdateRequest
from karapace.core.auth import AuthenticatorAndAuthorizer, Operation
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.typing import PrimaryInfo, Subject

SUBJECT = Subject("test-subject")
IMPORT_REQUEST = ModeUpdateRequest(mode="IMPORT")


def _denying_authorizer(operation: Operation) -> AuthenticatorAndAuthorizer:
    authorizer = MagicMock(spec=AuthenticatorAndAuthorizer)
    authorizer.check_authorization = MagicMock(side_effect=lambda _user, op, _res: op != operation)
    return authorizer


def _registry(*, primary: bool, primary_url: str | None) -> KarapaceSchemaRegistry:
    schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
    schema_registry.get_master.return_value = PrimaryInfo(primary=primary, primary_url=primary_url)
    return schema_registry


class TestModeGet:
    async def test_returns_global_mode_when_authorized(self) -> None:
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await mode_get(user=None, authorizer=None, controller=controller)

        controller.get_global_mode.assert_called_once_with()
        assert result is controller.get_global_mode.return_value

    async def test_denied_read_raises_unauthorized(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_get(
                user=None,
                authorizer=_denying_authorizer(Operation.Read),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 403


class TestModePut:
    async def test_denied_write_raises_unauthorized(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_put(
                request=MagicMock(spec=Request),
                mode_request=IMPORT_REQUEST,
                user=None,
                schema_registry=AsyncMock(spec=KarapaceSchemaRegistry),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=_denying_authorizer(Operation.Write),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 403

    async def test_sets_global_mode_when_primary(self) -> None:
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await mode_put(
            request=MagicMock(spec=Request),
            mode_request=IMPORT_REQUEST,
            user=None,
            schema_registry=_registry(primary=True, primary_url=None),
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            controller=controller,
            force=True,
        )

        controller.set_global_mode.assert_called_once_with(mode_request=IMPORT_REQUEST, force=True)
        assert result is controller.set_global_mode.return_value

    async def test_forwards_to_primary_when_not_primary_itself(self) -> None:
        forward_client = AsyncMock(spec=ForwardClient)

        result = await mode_put(
            request=MagicMock(spec=Request),
            mode_request=IMPORT_REQUEST,
            user=None,
            schema_registry=_registry(primary=False, primary_url="http://primary:8081"),
            forward_client=forward_client,
            authorizer=None,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert forward_client.forward_request_remote.await_args.kwargs["response_type"] is ModeResponse
        assert result is forward_client.forward_request_remote.return_value

    async def test_raises_no_primary_url_error_when_no_primary_known(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_put(
                request=MagicMock(spec=Request),
                mode_request=IMPORT_REQUEST,
                user=None,
                schema_registry=_registry(primary=False, primary_url=None),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500


class TestModeGetSubject:
    async def test_returns_subject_mode_when_authorized(self) -> None:
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await mode_get_subject(
            subject=SUBJECT,
            user=None,
            defaultToGlobal=True,
            authorizer=None,
            controller=controller,
        )

        controller.get_subject_mode.assert_called_once_with(subject=SUBJECT, default_to_global=True)
        assert result is controller.get_subject_mode.return_value

    async def test_denied_read_raises_subject_not_found(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_get_subject(
                subject=SUBJECT,
                user=None,
                defaultToGlobal=False,
                authorizer=_denying_authorizer(Operation.Read),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 404


class TestModePutSubject:
    async def test_denied_write_raises_subject_not_found(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_put_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                mode_request=IMPORT_REQUEST,
                user=None,
                schema_registry=AsyncMock(spec=KarapaceSchemaRegistry),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=_denying_authorizer(Operation.Write),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail["error_code"] == 40401

    async def test_sets_subject_mode_when_primary(self) -> None:
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await mode_put_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            mode_request=IMPORT_REQUEST,
            user=None,
            schema_registry=_registry(primary=True, primary_url=None),
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            controller=controller,
            force=True,
        )

        controller.set_subject_mode.assert_called_once_with(subject=SUBJECT, mode_request=IMPORT_REQUEST, force=True)
        assert result is controller.set_subject_mode.return_value

    async def test_forwards_to_primary_when_not_primary_itself(self) -> None:
        forward_client = AsyncMock(spec=ForwardClient)

        result = await mode_put_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            mode_request=IMPORT_REQUEST,
            user=None,
            schema_registry=_registry(primary=False, primary_url="http://primary:8081"),
            forward_client=forward_client,
            authorizer=None,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert forward_client.forward_request_remote.await_args.kwargs["response_type"] is ModeResponse
        assert result is forward_client.forward_request_remote.return_value

    async def test_raises_no_primary_url_error_when_no_primary_known(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_put_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                mode_request=IMPORT_REQUEST,
                user=None,
                schema_registry=_registry(primary=False, primary_url=None),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500


class TestModeDeleteSubject:
    async def test_denied_write_raises_subject_not_found(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_delete_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                user=None,
                schema_registry=AsyncMock(spec=KarapaceSchemaRegistry),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=_denying_authorizer(Operation.Write),
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 404

    async def test_deletes_subject_mode_when_primary(self) -> None:
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await mode_delete_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            user=None,
            schema_registry=_registry(primary=True, primary_url=None),
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            controller=controller,
        )

        controller.delete_subject_mode.assert_called_once_with(subject=SUBJECT)
        assert result is controller.delete_subject_mode.return_value

    async def test_forwards_to_primary_when_not_primary_itself(self) -> None:
        forward_client = AsyncMock(spec=ForwardClient)

        result = await mode_delete_subject(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            user=None,
            schema_registry=_registry(primary=False, primary_url="http://primary:8081"),
            forward_client=forward_client,
            authorizer=None,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert forward_client.forward_request_remote.await_args.kwargs["response_type"] is ModeResponse
        assert result is forward_client.forward_request_remote.return_value

    async def test_raises_no_primary_url_error_when_no_primary_known(self) -> None:
        with pytest.raises(HTTPException) as exc_info:
            await mode_delete_subject(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                user=None,
                schema_registry=_registry(primary=False, primary_url=None),
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500
