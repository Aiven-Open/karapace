"""
Unit tests for the subject probing protection.

When an authenticated caller is denied access to a subject-scoped resource,
the response must be byte-identical to the response for a genuinely-missing
subject (status 404 with the canonical 40401 body). Otherwise an attacker can
enumerate which subjects exist by comparing 403 vs 404.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException, status

from karapace.api.routers.compatibility import compatibility_post
from karapace.api.routers.config import config_delete_subject, config_get_subject, config_set_subject
from karapace.api.routers.errors import SchemaErrorCodes, SchemaErrorMessages, subject_not_found, unauthorized
from karapace.api.routers.mode import mode_get_subject
from karapace.api.routers.requests import CompatibilityRequest, SchemaRequest
from karapace.api.routers.subjects import (
    subjects_subject_delete,
    subjects_subject_post,
    subjects_subject_version_delete,
    subjects_subject_version_get,
    subjects_subject_version_referenced_by,
    subjects_subject_version_schema_get,
    subjects_subject_versions_list,
    subjects_subject_versions_post,
)
from karapace.core.auth import Operation
from karapace.core.typing import Subject


SUBJECT = Subject("secret-subject")


# ---------------------------------------------------------------------------
# subject_not_found helper: body must match the genuine "subject does not exist"
# response raised by KarapaceSchemaRegistryController._subject_get.
# ---------------------------------------------------------------------------


def test_subject_not_found_returns_404_with_canonical_body() -> None:
    exc = subject_not_found(SUBJECT)
    assert exc.status_code == status.HTTP_404_NOT_FOUND
    assert exc.detail == {
        "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
        "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=SUBJECT),
    }


def test_subject_not_found_body_matches_genuine_missing_response() -> None:
    """The probing-protection 404 must be indistinguishable from a real not-found 404."""
    forbidden = subject_not_found(SUBJECT)
    # Build the same shape KarapaceSchemaRegistryController._subject_get raises
    # for a SubjectNotFoundException (see src/karapace/api/controller.py).
    genuine = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={
            "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
            "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=SUBJECT),
        },
    )
    assert forbidden.status_code == genuine.status_code
    assert forbidden.detail == genuine.detail


def test_unauthorized_helper_still_returns_403() -> None:
    """The 403 helper must remain available for non-subject scopes (Config:, method-level OIDC)."""
    exc = unauthorized()
    assert exc.status_code == status.HTTP_403_FORBIDDEN
    assert exc.detail == {"message": "Forbidden"}


# ---------------------------------------------------------------------------
# Per-route assertions: a denying authorizer must produce the 404 helper, not
# the legacy 403. Calling the route function directly with a MagicMock
# authorizer is enough — we are only exercising the early authZ branch.
# ---------------------------------------------------------------------------


def _denying_authorizer() -> MagicMock:
    authorizer = MagicMock()
    authorizer.check_authorization.return_value = False
    return authorizer


def _assert_subject_not_found(exc_info: pytest.ExceptionInfo[HTTPException]) -> None:
    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    assert exc_info.value.detail == {
        "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
        "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=SUBJECT),
    }


# subjects.py


async def test_subjects_subject_post_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_post(
            subject=SUBJECT,
            user=None,
            schema_request=SchemaRequest(schema="{}"),
            deleted=False,
            normalize=False,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_subjects_subject_delete_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_delete(
            request=MagicMock(),
            subject=SUBJECT,
            user=None,
            permanent=False,
            forward_client=MagicMock(),
            authorizer=_denying_authorizer(),
            schema_registry=MagicMock(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_subjects_subject_versions_post_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_versions_post(
            request=MagicMock(),
            subject=SUBJECT,
            schema_request=SchemaRequest(schema="{}"),
            user=None,
            forward_client=MagicMock(),
            authorizer=_denying_authorizer(),
            normalize=False,
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_subjects_subject_versions_list_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_versions_list(
            subject=SUBJECT,
            user=None,
            deleted=False,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_subjects_subject_version_get_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_version_get(
            subject=SUBJECT,
            version="latest",
            user=None,
            deleted=False,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_subjects_subject_version_delete_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_version_delete(
            request=MagicMock(),
            subject=SUBJECT,
            version="1",
            user=None,
            permanent=False,
            forward_client=MagicMock(),
            authorizer=_denying_authorizer(),
            schema_registry=MagicMock(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_subjects_subject_version_schema_get_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_version_schema_get(
            subject=SUBJECT,
            version="latest",
            user=None,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_subjects_subject_version_referenced_by_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await subjects_subject_version_referenced_by(
            subject=SUBJECT,
            version="latest",
            user=None,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


# config.py — only the subject-scoped routes; global Config: stays 403 elsewhere.


async def test_config_get_subject_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await config_get_subject(
            subject=SUBJECT,
            user=None,
            defaultToGlobal=False,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_config_set_subject_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await config_set_subject(
            request=MagicMock(),
            subject=SUBJECT,
            compatibility_level_request=CompatibilityRequest(compatibility="NONE"),
            user=None,
            schema_registry=MagicMock(),
            forward_client=MagicMock(),
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


async def test_config_delete_subject_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await config_delete_subject(
            request=MagicMock(),
            subject=SUBJECT,
            user=None,
            schema_registry=MagicMock(),
            forward_client=MagicMock(),
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


# mode.py


async def test_mode_get_subject_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await mode_get_subject(
            subject=SUBJECT,
            user=None,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


# compatibility.py


async def test_compatibility_post_denied_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await compatibility_post(
            subject=SUBJECT,
            version="1",
            schema_request=SchemaRequest(schema="{}"),
            user=None,
            authorizer=_denying_authorizer(),
            controller=MagicMock(),
        )
    _assert_subject_not_found(exc_info)


# ---------------------------------------------------------------------------
# Cross-check: the authZ resource must be the Subject:* form. Confirms we did
# not accidentally swap a Config: site to subject_not_found().
# ---------------------------------------------------------------------------


async def test_subject_scoped_authz_uses_subject_resource_form() -> None:
    authorizer = _denying_authorizer()
    with pytest.raises(HTTPException):
        await subjects_subject_versions_list(
            subject=SUBJECT,
            user=None,
            deleted=False,
            authorizer=authorizer,
            controller=MagicMock(),
        )
    args, _kwargs = authorizer.check_authorization.call_args
    user_arg, op_arg, resource_arg = args
    assert op_arg == Operation.Read
    assert resource_arg == f"Subject:{SUBJECT}"
