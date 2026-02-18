"""
Unit tests for TlsContextHolder.

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from karapace.core.config import Config, InvalidConfiguration
from karapace.core import tls as tls_module
from karapace.core.tls import TlsContextHolder


@pytest.fixture
def base_config() -> Config:
    return Config()


@pytest.mark.asyncio
async def test_tls_holder_reload_success(monkeypatch, base_config: Config) -> None:
    """On successful reload, context and last_reload_at are updated."""

    fake_context = object()

    def fake_create_server_ssl_context(cfg: Config):  # type: ignore[override]
        assert cfg is base_config
        return fake_context

    monkeypatch.setattr(tls_module, "create_server_ssl_context", fake_create_server_ssl_context)

    holder = TlsContextHolder(config=base_config, min_reload_interval_seconds=0)

    assert holder.context is None
    assert holder.last_reload_at is None

    result = await holder.reload()

    assert result.success is True
    assert result.message == "ok"
    assert result.reloaded_at is not None
    assert holder.context is fake_context
    assert holder.last_reload_at == result.reloaded_at


@pytest.mark.asyncio
async def test_tls_holder_reload_invalid_configuration(monkeypatch, base_config: Config) -> None:
    """If configuration is invalid, holder keeps old context and reports error."""

    def raising_create_server_ssl_context(cfg: Config):  # type: ignore[override]
        raise InvalidConfiguration("bad tls configuration")

    monkeypatch.setattr(tls_module, "create_server_ssl_context", raising_create_server_ssl_context)

    holder = TlsContextHolder(config=base_config, min_reload_interval_seconds=0)

    result = await holder.reload()

    assert result.success is False
    assert "bad tls configuration" in result.message
    assert result.reloaded_at is None
    assert holder.context is None
    assert holder.last_reload_at is None


@pytest.mark.asyncio
async def test_tls_holder_rate_limiting(monkeypatch, base_config: Config) -> None:
    """Second reload within min interval is rejected without calling create_server_ssl_context again."""

    fake_context = object()
    calls = []

    def fake_create_server_ssl_context(cfg: Config):  # type: ignore[override]
        calls.append(datetime.now(timezone.utc))
        return fake_context

    monkeypatch.setattr(tls_module, "create_server_ssl_context", fake_create_server_ssl_context)

    holder = TlsContextHolder(config=base_config, min_reload_interval_seconds=60)

    # First reload succeeds
    first_result = await holder.reload()
    assert first_result.success is True
    assert holder.context is fake_context

    # Force last_reload_at to "now" so that the next call is within interval
    holder._last_reload_at = datetime.now(timezone.utc)  # type: ignore[attr-defined]

    second_result = await holder.reload()
    assert second_result.success is False
    assert "too frequently" in second_result.message
    # create_server_ssl_context should have been called only once
    assert len(calls) == 1
