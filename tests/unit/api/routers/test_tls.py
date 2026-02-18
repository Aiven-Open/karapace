"""Unit tests for TLS reload endpoint.

Covers token-based authorization and basic success/error flows.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from karapace.api.routers.tls import reload_tls
from karapace.core.config import Config
from karapace.core.tls import TlsContextHolder, TlsReloadResult


@pytest.fixture
def config_with_token() -> Config:
    config = Config()
    config.server_tls_reload_token = "test-token-123"
    return config


@pytest.fixture
def tls_holder_mock() -> TlsContextHolder:
    return AsyncMock(spec=TlsContextHolder)


@pytest.mark.asyncio
async def test_reload_tls_success(config_with_token: Config, tls_holder_mock: TlsContextHolder) -> None:
    """Endpoint returns ok when reload succeeds."""

    result = TlsReloadResult(success=True, message="ok", reloaded_at=datetime.now(timezone.utc))
    tls_holder_mock.reload.return_value = result

    response = await reload_tls(
        x_tls_reload_token="test-token-123",
        config=config_with_token,
        tls_context_holder=tls_holder_mock,
    )

    assert response.status == "ok"
    assert response.mode == "prepare_only"
    assert response.reloaded_at is not None
    tls_holder_mock.reload.assert_awaited_once()


@pytest.mark.asyncio
async def test_reload_tls_invalid_token(config_with_token: Config, tls_holder_mock: TlsContextHolder) -> None:
    """Invalid token results in 403 and holder is not called."""

    with pytest.raises(HTTPException) as exc_info:
        await reload_tls(
            x_tls_reload_token="wrong-token",
            config=config_with_token,
            tls_context_holder=tls_holder_mock,
        )

    assert exc_info.value.status_code == 403
    tls_holder_mock.reload.assert_not_awaited()


@pytest.mark.asyncio
async def test_reload_tls_missing_token(config_with_token: Config, tls_holder_mock: TlsContextHolder) -> None:
    """Missing token results in 403."""

    with pytest.raises(HTTPException) as exc_info:
        await reload_tls(
            x_tls_reload_token=None,
            config=config_with_token,
            tls_context_holder=tls_holder_mock,
        )

    assert exc_info.value.status_code == 403
    tls_holder_mock.reload.assert_not_awaited()


@pytest.mark.asyncio
async def test_reload_tls_token_not_configured(tls_holder_mock: TlsContextHolder) -> None:
    """When token is not configured, endpoint returns 503."""

    config = Config()
    config.server_tls_reload_token = None

    with pytest.raises(HTTPException) as exc_info:
        await reload_tls(
            x_tls_reload_token="any-token",
            config=config,
            tls_context_holder=tls_holder_mock,
        )

    assert exc_info.value.status_code == 503
    tls_holder_mock.reload.assert_not_awaited()


@pytest.mark.asyncio
async def test_reload_tls_reload_failure(config_with_token: Config, tls_holder_mock: TlsContextHolder) -> None:
    """If holder fails to reload TLS, endpoint returns 500."""

    result = TlsReloadResult(success=False, message="Certificate file not found", reloaded_at=None)
    tls_holder_mock.reload.return_value = result

    with pytest.raises(HTTPException) as exc_info:
        await reload_tls(
            x_tls_reload_token="test-token-123",
            config=config_with_token,
            tls_context_holder=tls_holder_mock,
        )

    assert exc_info.value.status_code == 500
    assert "Certificate file not found" in str(exc_info.value.detail)
    tls_holder_mock.reload.assert_awaited_once()
