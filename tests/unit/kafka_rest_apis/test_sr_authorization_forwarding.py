"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details

Tests for the REST Proxy → Schema Registry Authorization-header forwarding gate.
The gate lives in `UserRestProxy.publish` and `UserRestProxy.fetch`: the inbound
Authorization header is copied into `sr_authorization_ctx` only when
`sasl_oauthbearer_authentication_enabled` is true.
"""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest

from karapace.core.config import Config
from karapace.core.serialization import sr_authorization_ctx
from karapace.kafka_rest_apis import UserRestProxy


@pytest.fixture
def reset_sr_authorization_ctx():
    """Reset the contextvar between tests."""
    token = sr_authorization_ctx.set(None)
    try:
        yield
    finally:
        sr_authorization_ctx.reset(token)


def _make_proxy(*, sasl_oauthbearer_authentication_enabled: bool) -> UserRestProxy:
    """Build a UserRestProxy stub without going through __init__ (which needs a live Kafka).
    Only the `config` attribute is populated; downstream calls are stubbed per-test."""
    proxy = UserRestProxy.__new__(UserRestProxy)
    proxy.config = Config(sasl_oauthbearer_authentication_enabled=sasl_oauthbearer_authentication_enabled)
    return proxy


def _make_request(*, headers: dict[str, str] | None = None) -> Mock:
    request = Mock()
    request.headers = headers or {}
    request.query = {}
    request.accepts = {"embedded_format": "binary"}
    request.content_type = {"embedded_format": "binary"}
    request.json = {"records": []}
    return request


# fetch() gate — only line before consumer_manager.fetch is the gate itself.


async def test_fetch_does_not_set_ctxvar_when_flag_disabled(reset_sr_authorization_ctx) -> None:
    """Flag off: existing OIDC-on-proxy deployments must NOT propagate the inbound bearer.
    The static session_auth (Basic) on the SR client stays in charge."""
    proxy = _make_proxy(sasl_oauthbearer_authentication_enabled=False)

    captured: list[str | None] = []

    async def fake_consumer_fetch(**_kwargs) -> None:
        captured.append(sr_authorization_ctx.get())

    proxy.consumer_manager = Mock()
    proxy.consumer_manager.fetch = fake_consumer_fetch

    request = _make_request(headers={"Authorization": "Bearer leak.if.broken"})
    await proxy.fetch("group", "instance", "application/json", request=request)

    assert captured == [None]


async def test_fetch_sets_ctxvar_to_inbound_bearer_when_flag_enabled(reset_sr_authorization_ctx) -> None:
    proxy = _make_proxy(sasl_oauthbearer_authentication_enabled=True)

    captured: list[str | None] = []

    async def fake_consumer_fetch(**_kwargs) -> None:
        captured.append(sr_authorization_ctx.get())

    proxy.consumer_manager = Mock()
    proxy.consumer_manager.fetch = fake_consumer_fetch

    request = _make_request(headers={"Authorization": "Bearer good.token"})
    await proxy.fetch("group", "instance", "application/json", request=request)

    assert captured == ["Bearer good.token"]


async def test_fetch_sets_ctxvar_to_none_when_flag_enabled_but_no_header(reset_sr_authorization_ctx) -> None:
    """Flag on but no inbound Authorization: contextvar set to None so the SR client
    falls back to its session_auth (Basic). Covers the flag-true + Basic-SR path."""
    proxy = _make_proxy(sasl_oauthbearer_authentication_enabled=True)

    captured: list[str | None] = []

    async def fake_consumer_fetch(**_kwargs) -> None:
        captured.append(sr_authorization_ctx.get())

    proxy.consumer_manager = Mock()
    proxy.consumer_manager.fetch = fake_consumer_fetch

    request = _make_request(headers={})  # no Authorization
    await proxy.fetch("group", "instance", "application/json", request=request)

    assert captured == [None]


# publish() gate — stub get_topic_info to raise right after the gate runs,
# so we can read the contextvar without touching Kafka or the producer.


class _StopAfterGate(Exception):
    """Sentinel raised by stubbed get_topic_info to short-circuit publish."""


async def _drive_publish_capture_ctx(proxy: UserRestProxy, request: Mock) -> str | None:
    captured: list[str | None] = []

    async def fake_get_topic_info(_topic: str, _content_type: str) -> dict:
        captured.append(sr_authorization_ctx.get())
        raise _StopAfterGate()

    proxy.get_topic_info = fake_get_topic_info

    with pytest.raises(_StopAfterGate):
        await proxy.publish(topic="t", partition_id=None, content_type="application/json", request=request)

    assert captured
    return captured[0]


async def test_publish_does_not_set_ctxvar_when_flag_disabled(reset_sr_authorization_ctx) -> None:
    proxy = _make_proxy(sasl_oauthbearer_authentication_enabled=False)
    request = _make_request(headers={"Authorization": "Bearer leak.if.broken"})

    observed = await _drive_publish_capture_ctx(proxy, request)
    assert observed is None


async def test_publish_sets_ctxvar_to_inbound_bearer_when_flag_enabled(reset_sr_authorization_ctx) -> None:
    proxy = _make_proxy(sasl_oauthbearer_authentication_enabled=True)
    request = _make_request(headers={"Authorization": "Bearer publish.token"})

    observed = await _drive_publish_capture_ctx(proxy, request)
    assert observed == "Bearer publish.token"


async def test_publish_sets_ctxvar_to_none_when_flag_enabled_but_no_header(reset_sr_authorization_ctx) -> None:
    proxy = _make_proxy(sasl_oauthbearer_authentication_enabled=True)
    request = _make_request(headers={})

    observed = await _drive_publish_capture_ctx(proxy, request)
    assert observed is None


# Concurrent requests must not bleed tokens into each other.
# asyncio.gather copies context per coroutine, so each call sees only its own value.


async def test_concurrent_requests_isolate_ctxvar(reset_sr_authorization_ctx) -> None:
    captured: dict[str, str | None] = {}

    async def fake_get_topic_info(topic: str, _content_type: str) -> dict:
        # Yield so the two coroutines interleave before reading the contextvar.
        await asyncio.sleep(0)
        captured[topic] = sr_authorization_ctx.get()
        raise _StopAfterGate()

    async def drive(topic: str, token: str) -> None:
        proxy = _make_proxy(sasl_oauthbearer_authentication_enabled=True)
        proxy.get_topic_info = fake_get_topic_info
        request = _make_request(headers={"Authorization": token})
        with pytest.raises(_StopAfterGate):
            await proxy.publish(topic=topic, partition_id=None, content_type="application/json", request=request)

    await asyncio.gather(drive("topic-a", "Bearer A"), drive("topic-b", "Bearer B"))

    assert captured == {"topic-a": "Bearer A", "topic-b": "Bearer B"}
