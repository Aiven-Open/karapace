"""
Tests for the `/subjects` FastAPI router (``karapace.api.routers.subjects``) success paths.

Denied-authorization paths are covered separately in ``test_subject_authz_probing.py``;
this file focuses on the happy paths and primary/forwarding branches.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException, Request

from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.forward_client import ForwardClient
from karapace.api.routers.requests import SchemaRequest
from karapace.api.routers.subjects import (
    subjects_get,
    subjects_subject_delete,
    subjects_subject_post,
    subjects_subject_version_delete,
    subjects_subject_version_get,
    subjects_subject_version_referenced_by,
    subjects_subject_version_schema_get,
    subjects_subject_versions_list,
    subjects_subject_versions_post,
)
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.typing import PrimaryInfo, Subject

SUBJECT = Subject("test-subject")


async def test_subjects_get_delegates_to_controller() -> None:
    controller = AsyncMock(spec=KarapaceSchemaRegistryController)

    result = await subjects_get(user=None, deleted=True, authorizer=None, controller=controller)

    controller.subjects_list.assert_called_once_with(deleted=True, user=None, authorizer=None)
    assert result is controller.subjects_list.return_value


async def test_subjects_subject_post_returns_controller_response_when_authorized() -> None:
    controller = AsyncMock(spec=KarapaceSchemaRegistryController)
    schema_request = SchemaRequest(schema="{}")

    result = await subjects_subject_post(
        subject=SUBJECT,
        user=None,
        schema_request=schema_request,
        deleted=False,
        normalize=True,
        authorizer=None,
        controller=controller,
    )

    controller.subjects_schema_post.assert_called_once_with(
        subject=SUBJECT,
        schema_request=schema_request,
        deleted=False,
        normalize=True,
    )
    assert result is controller.subjects_schema_post.return_value


class TestSubjectsSubjectDelete:
    async def test_deletes_locally_when_primary(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=True, primary_url=None)
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await subjects_subject_delete(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            user=None,
            permanent=True,
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            schema_registry=schema_registry,
            controller=controller,
        )

        controller.subject_delete.assert_called_once_with(subject=SUBJECT, permanent=True)
        assert result is controller.subject_delete.return_value

    async def test_forwards_when_not_primary(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://primary:8081")
        forward_client = AsyncMock(spec=ForwardClient)

        result = await subjects_subject_delete(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            user=None,
            permanent=False,
            forward_client=forward_client,
            authorizer=None,
            schema_registry=schema_registry,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert result is forward_client.forward_request_remote.return_value

    async def test_raises_no_primary_url_error_when_primary_unknown(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url=None)

        with pytest.raises(HTTPException) as exc_info:
            await subjects_subject_delete(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                user=None,
                permanent=False,
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                schema_registry=schema_registry,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500


async def test_subjects_subject_versions_post_delegates_to_controller() -> None:
    controller = AsyncMock(spec=KarapaceSchemaRegistryController)
    schema_request = SchemaRequest(schema="{}")
    forward_client = AsyncMock(spec=ForwardClient)
    request = MagicMock(spec=Request)

    result = await subjects_subject_versions_post(
        request=request,
        subject=SUBJECT,
        schema_request=schema_request,
        user=None,
        forward_client=forward_client,
        authorizer=None,
        normalize=False,
        controller=controller,
    )

    controller.subject_post.assert_called_once_with(
        subject=SUBJECT,
        schema_request=schema_request,
        normalize=False,
        forward_client=forward_client,
        request=request,
    )
    assert result is controller.subject_post.return_value


async def test_subjects_subject_versions_list_returns_controller_response() -> None:
    controller = AsyncMock(spec=KarapaceSchemaRegistryController)

    result = await subjects_subject_versions_list(
        subject=SUBJECT, user=None, deleted=True, authorizer=None, controller=controller
    )

    controller.subject_versions_list.assert_called_once_with(subject=SUBJECT, deleted=True)
    assert result is controller.subject_versions_list.return_value


async def test_subjects_subject_version_get_returns_controller_response() -> None:
    controller = AsyncMock(spec=KarapaceSchemaRegistryController)

    result = await subjects_subject_version_get(
        subject=SUBJECT, version="latest", user=None, deleted=True, authorizer=None, controller=controller
    )

    controller.subject_version_get.assert_called_once_with(subject=SUBJECT, version="latest", deleted=True)
    assert result is controller.subject_version_get.return_value


class TestSubjectsSubjectVersionDelete:
    async def test_deletes_locally_when_primary(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=True, primary_url=None)
        controller = AsyncMock(spec=KarapaceSchemaRegistryController)

        result = await subjects_subject_version_delete(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            version="3",
            user=None,
            permanent=True,
            forward_client=AsyncMock(spec=ForwardClient),
            authorizer=None,
            schema_registry=schema_registry,
            controller=controller,
        )

        controller.subject_version_delete.assert_called_once_with(subject=SUBJECT, version="3", permanent=True)
        assert result is controller.subject_version_delete.return_value

    async def test_forwards_when_not_primary(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url="http://primary:8081")
        forward_client = AsyncMock(spec=ForwardClient)

        result = await subjects_subject_version_delete(
            request=MagicMock(spec=Request),
            subject=SUBJECT,
            version="3",
            user=None,
            permanent=False,
            forward_client=forward_client,
            authorizer=None,
            schema_registry=schema_registry,
            controller=AsyncMock(spec=KarapaceSchemaRegistryController),
        )

        forward_client.forward_request_remote.assert_called_once()
        assert result is forward_client.forward_request_remote.return_value

    async def test_raises_no_primary_url_error_when_primary_unknown(self) -> None:
        schema_registry = AsyncMock(spec=KarapaceSchemaRegistry)
        schema_registry.get_master.return_value = PrimaryInfo(primary=False, primary_url=None)

        with pytest.raises(HTTPException) as exc_info:
            await subjects_subject_version_delete(
                request=MagicMock(spec=Request),
                subject=SUBJECT,
                version="3",
                user=None,
                permanent=False,
                forward_client=AsyncMock(spec=ForwardClient),
                authorizer=None,
                schema_registry=schema_registry,
                controller=AsyncMock(spec=KarapaceSchemaRegistryController),
            )
        assert exc_info.value.status_code == 500


async def test_subjects_subject_version_schema_get_returns_controller_response() -> None:
    controller = AsyncMock(spec=KarapaceSchemaRegistryController)

    result = await subjects_subject_version_schema_get(
        subject=SUBJECT, version="latest", user=None, authorizer=None, controller=controller
    )

    controller.subject_version_schema_get.assert_called_once_with(subject=SUBJECT, version="latest")
    assert result is controller.subject_version_schema_get.return_value


async def test_subjects_subject_version_referenced_by_returns_controller_response() -> None:
    controller = AsyncMock(spec=KarapaceSchemaRegistryController)

    result = await subjects_subject_version_referenced_by(
        subject=SUBJECT, version="latest", user=None, authorizer=None, controller=controller
    )

    controller.subject_version_referencedby_get.assert_called_once_with(subject=SUBJECT, version="latest")
    assert result is controller.subject_version_referencedby_get.return_value
