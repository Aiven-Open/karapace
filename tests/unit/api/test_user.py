"""
Tests for the basic-auth dependency (``karapace.api.user.get_current_user``), including the
OIDC pass-through branch used by scheme-based auth dispatch.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from karapace.api.user import get_current_user


def _request(**state) -> MagicMock:
    request = MagicMock()
    request.state = SimpleNamespace(**state)
    return request


async def test_returns_external_user_when_oidc_authenticated() -> None:
    authorizer = MagicMock()

    user = await get_current_user(
        request=_request(oidc_authenticated=True, user="subject-1"),
        credentials=None,
        authorizer=authorizer,
    )

    assert user is not None
    assert user.username == "subject-1"
    assert user.authenticated_externally is True
    # Basic authentication is skipped entirely when OIDC already authenticated the request.
    authorizer.authenticate.assert_not_called()


async def test_external_user_uses_empty_username_when_subject_missing() -> None:
    user = await get_current_user(
        request=_request(oidc_authenticated=True, user=None),
        credentials=None,
        authorizer=MagicMock(),
    )

    assert user is not None
    assert user.username == ""
    assert user.authenticated_externally is True


async def test_requires_basic_when_not_oidc_authenticated() -> None:
    authorizer = MagicMock()
    authorizer.MUST_AUTHENTICATE = True

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(request=_request(), credentials=None, authorizer=authorizer)

    assert exc_info.value.status_code == 401
