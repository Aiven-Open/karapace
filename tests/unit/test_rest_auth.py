"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import asyncio
import datetime
import time
from unittest.mock import Mock, call

from karapace.core.container import KarapaceContainer
from karapace.kafka_rest_apis import AUTH_EXPIRY_TOLERANCE, KafkaRest, UserRestProxy


def _create_mock_proxy(
    *,
    num_consumers: int,
    last_used: float,
    auth_expiry: datetime.datetime | None = None,
    _with_close_future: bool = False,
) -> Mock:
    proxy = Mock(spec=UserRestProxy)
    proxy.num_consumers.return_value = num_consumers
    proxy.last_used = last_used
    proxy.auth_expiry = auth_expiry

    if _with_close_future:
        close_future = asyncio.Future()
        close_future.set_result(True)
        proxy.aclose.return_value = close_future

    return proxy


async def test_rest_proxy_janitor_expiring_credentials(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
        }
    )
    instance = KafkaRest(config=config)

    proxy_expiring_within_tolerance = _create_mock_proxy(
        num_consumers=99,
        last_used=time.monotonic(),
        auth_expiry=datetime.datetime.now(datetime.timezone.utc) + (AUTH_EXPIRY_TOLERANCE / 2),
        _with_close_future=True,
    )
    instance.proxies["proxy_expiring_within_tolerance"] = proxy_expiring_within_tolerance

    proxy_already_expired = _create_mock_proxy(
        num_consumers=99,
        last_used=time.monotonic(),
        auth_expiry=datetime.datetime.now(datetime.timezone.utc) - AUTH_EXPIRY_TOLERANCE,
        _with_close_future=True,
    )
    instance.proxies["proxy_already_expired"] = proxy_already_expired

    active_proxy_expiring_later_than_tolerance = _create_mock_proxy(
        num_consumers=0,
        last_used=time.monotonic(),
        auth_expiry=datetime.datetime.now(datetime.timezone.utc) + 2 * AUTH_EXPIRY_TOLERANCE,
    )
    instance.proxies["active_proxy_expiring_later_than_tolerance"] = active_proxy_expiring_later_than_tolerance

    unused_proxy_expiring_later_than_tolerance = _create_mock_proxy(
        num_consumers=0,
        last_used=time.monotonic() - 600,
        auth_expiry=datetime.datetime.now(datetime.timezone.utc) + 2 * AUTH_EXPIRY_TOLERANCE,
        _with_close_future=True,
    )
    instance.proxies["unused_proxy_expiring_later_than_tolerance"] = unused_proxy_expiring_later_than_tolerance

    # Needs to be called multiple times to clean all expected proxies, this method only
    # releases the first one it finds, then exits.
    await instance._disconnect_idle_proxy_if_any()
    await instance._disconnect_idle_proxy_if_any()
    await instance._disconnect_idle_proxy_if_any()

    assert instance.proxies.get("proxy_expiring_within_tolerance") is None
    assert instance.proxies.get("proxy_already_expired") is None
    assert instance.proxies.get("active_proxy_expiring_later_than_tolerance") is not None
    assert instance.proxies.get("unused_proxy_expiring_later_than_tolerance") is None
    assert len(instance.proxies) == 1

    assert proxy_expiring_within_tolerance.method_calls == [call.aclose()]
    assert proxy_already_expired.method_calls == [call.aclose()]
    assert active_proxy_expiring_later_than_tolerance.method_calls == [call.num_consumers()]
    assert unused_proxy_expiring_later_than_tolerance.method_calls == [call.num_consumers(), call.aclose()]


async def test_rest_proxy_janitor_default(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
        }
    )
    instance = KafkaRest(config=config)

    active_proxy_without_consumers = _create_mock_proxy(num_consumers=0, last_used=time.monotonic())
    instance.proxies["active_proxy_without_consumers"] = active_proxy_without_consumers

    active_proxy_with_consumers = _create_mock_proxy(num_consumers=99, last_used=time.monotonic())
    instance.proxies["active_proxy_with_consumers"] = active_proxy_with_consumers

    unused_proxy_without_consumers = _create_mock_proxy(
        num_consumers=0,
        last_used=time.monotonic() - 600,
        _with_close_future=True,
    )
    instance.proxies["unused_proxy_without_consumers"] = unused_proxy_without_consumers

    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_without_consumers") is None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 2

    assert unused_proxy_without_consumers.method_calls == [call.num_consumers(), call.aclose()]
    assert active_proxy_without_consumers.method_calls == [call.num_consumers()]
    assert active_proxy_with_consumers.method_calls == [call.num_consumers()]

    unused_proxy_without_consumers.reset_mock()
    active_proxy_without_consumers.reset_mock()
    active_proxy_with_consumers.reset_mock()

    # Proxy with consumers is not deleted without explicit config
    unused_proxy_with_consumers = _create_mock_proxy(
        num_consumers=99,
        last_used=time.monotonic() - 600,
        _with_close_future=True,
    )
    instance.proxies["unused_proxy_with_consumers"] = unused_proxy_with_consumers

    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 3

    assert unused_proxy_without_consumers.method_calls == []
    assert unused_proxy_with_consumers.method_calls == [call.num_consumers()]
    assert active_proxy_without_consumers.method_calls == [call.num_consumers()]
    assert active_proxy_with_consumers.method_calls == [call.num_consumers()]


async def test_rest_proxy_janitor_destructive(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
            "consumer_idle_disconnect_timeout": 120,
        }
    )
    instance = KafkaRest(config=config)

    active_proxy_without_consumers = _create_mock_proxy(num_consumers=0, last_used=time.monotonic())
    instance.proxies["active_proxy_without_consumers"] = active_proxy_without_consumers

    active_proxy_with_consumers = _create_mock_proxy(num_consumers=99, last_used=time.monotonic())
    instance.proxies["active_proxy_with_consumers"] = active_proxy_with_consumers

    unused_proxy_without_consumers = _create_mock_proxy(
        num_consumers=0,
        last_used=time.monotonic() - 600,
        _with_close_future=True,
    )
    instance.proxies["unused_proxy_without_consumers"] = unused_proxy_without_consumers

    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_without_consumers") is None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 2

    assert unused_proxy_without_consumers.method_calls == [call.num_consumers(), call.aclose()]
    assert active_proxy_without_consumers.method_calls == [call.num_consumers()]
    assert active_proxy_with_consumers.method_calls == [call.num_consumers()]

    unused_proxy_without_consumers.reset_mock()
    active_proxy_without_consumers.reset_mock()
    active_proxy_with_consumers.reset_mock()

    # Proxy with consumers gets deleted after enough time has passed
    unused_proxy_with_consumers = _create_mock_proxy(
        num_consumers=99,
        last_used=time.monotonic() - 600,
        _with_close_future=True,
    )
    instance.proxies["unused_proxy_with_consumers"] = unused_proxy_with_consumers

    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_with_consumers") is None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 2

    assert unused_proxy_without_consumers.method_calls == []
    assert unused_proxy_with_consumers.method_calls == [call.num_consumers(), call.aclose()]
    assert active_proxy_without_consumers.method_calls == [call.num_consumers()]
    assert active_proxy_with_consumers.method_calls == [call.num_consumers()]
