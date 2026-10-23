"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import asyncio
import base64
import datetime
import time
from http import HTTPStatus
from unittest.mock import Mock, call, patch

import jwt
import pytest

from karapace.core.container import KarapaceContainer
from karapace.kafka_rest_apis import AUTH_EXPIRY_TOLERANCE, KafkaRest, UserRestProxy
from karapace.rapu import HTTPResponse


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


def _oauth_service_instance(karapace_container: KarapaceContainer, *, sasl_mechanism: str) -> tuple[KafkaRest, object]:
    """Build a KafkaRest whose service config carries both a static OAuth token and a provider."""
    config = karapace_container.config().set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
            "sasl_mechanism": sasl_mechanism,
            "sasl_oauth_token": "service-static-token",
        }
    )
    service_provider = object()
    config._sasl_oauth_token_provider = service_provider
    return KafkaRest(config=config), service_provider


def _request_with(auth_header: str) -> Mock:
    request = Mock()
    request.headers = {"Authorization": auth_header}
    return request


async def test_user_proxy_does_not_inherit_service_oauth_credentials_for_bearer(
    karapace_container: KarapaceContainer,
) -> None:
    instance, service_provider = _oauth_service_instance(karapace_container, sasl_mechanism="OAUTHBEARER")
    user_token = jwt.encode({"exp": int(time.time()) + 3600}, "secret")

    with patch("karapace.kafka_rest_apis.UserRestProxy") as user_proxy_cls:
        await instance.get_user_proxy(_request_with(f"Bearer {user_token}"))

    user_config = user_proxy_cls.call_args.args[0]
    assert user_config.sasl_mechanism == "OAUTHBEARER"
    assert user_config.sasl_oauth_token == user_token
    assert user_config._sasl_oauth_token_provider is None
    # The service config itself is left untouched.
    assert instance.config.sasl_oauth_token == "service-static-token"
    assert instance.config._sasl_oauth_token_provider is service_provider


async def test_user_proxy_rejects_basic_credentials_on_oauthbearer_cluster(
    karapace_container: KarapaceContainer,
) -> None:
    instance, _ = _oauth_service_instance(karapace_container, sasl_mechanism="OAUTHBEARER")

    with patch("karapace.kafka_rest_apis.UserRestProxy") as user_proxy_cls:
        with pytest.raises(HTTPResponse) as excinfo:
            await instance.get_user_proxy(_request_with("Basic " + base64.b64encode(b"user:pass").decode()))

    assert excinfo.value.status == HTTPStatus.UNAUTHORIZED
    user_proxy_cls.assert_not_called()
    assert instance.proxies == {}


async def test_user_proxy_does_not_inherit_service_oauth_credentials_for_basic(
    karapace_container: KarapaceContainer,
) -> None:
    instance, _ = _oauth_service_instance(karapace_container, sasl_mechanism="SCRAM-SHA-256")

    with patch("karapace.kafka_rest_apis.UserRestProxy") as user_proxy_cls:
        await instance.get_user_proxy(_request_with("Basic " + base64.b64encode(b"user:pass").decode()))

    user_config = user_proxy_cls.call_args.args[0]
    assert user_config.sasl_mechanism == "SCRAM-SHA-256"
    assert user_config.sasl_plain_username == "user"
    assert user_config.sasl_plain_password == "pass"
    assert user_config.sasl_oauth_token is None
    assert user_config._sasl_oauth_token_provider is None
