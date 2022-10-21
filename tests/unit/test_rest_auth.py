# pylint: disable=protected-access
from karapace.config import set_config_defaults
from karapace.kafka_rest_apis import KafkaRest
from unittest.mock import call, Mock

import asyncio
import time


async def test_rest_proxy_janitor_default():
    config = set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
        }
    )
    instance = KafkaRest(config=config)

    active_proxy_without_consumers = Mock()
    active_proxy_without_consumers.num_consumers.return_value = 0
    active_proxy_without_consumers.last_used = time.monotonic()
    instance.proxies["active_proxy_without_consumers"] = active_proxy_without_consumers

    active_proxy_with_consumers = Mock()
    active_proxy_with_consumers.num_consumers.return_value = 99
    active_proxy_with_consumers.last_used = time.monotonic()
    instance.proxies["active_proxy_with_consumers"] = active_proxy_with_consumers

    unused_proxy_without_consumers = Mock()
    unused_proxy_without_consumers.num_consumers.return_value = 0
    unused_proxy_without_consumers.last_used = time.monotonic() - 600
    close_future = asyncio.Future()
    close_future.set_result(True)
    unused_proxy_without_consumers.aclose.return_value = close_future
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
    unused_proxy_with_consumers = Mock()
    unused_proxy_with_consumers.num_consumers.return_value = 99
    unused_proxy_with_consumers.last_used = time.monotonic() - 600
    close_future = asyncio.Future()
    close_future.set_result(True)
    unused_proxy_with_consumers.aclose.return_value = close_future
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


async def test_rest_proxy_janitor_destructive():
    config = set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
            "consumer_idle_disconnect_timeout": 120,
        }
    )
    instance = KafkaRest(config=config)

    active_proxy_without_consumers = Mock()
    active_proxy_without_consumers.num_consumers.return_value = 0
    active_proxy_without_consumers.last_used = time.monotonic()
    instance.proxies["active_proxy_without_consumers"] = active_proxy_without_consumers

    active_proxy_with_consumers = Mock()
    active_proxy_with_consumers.num_consumers.return_value = 99
    active_proxy_with_consumers.last_used = time.monotonic()
    instance.proxies["active_proxy_with_consumers"] = active_proxy_with_consumers

    unused_proxy_without_consumers = Mock()
    unused_proxy_without_consumers.num_consumers.return_value = 0
    unused_proxy_without_consumers.last_used = time.monotonic() - 600
    close_future = asyncio.Future()
    close_future.set_result(True)
    unused_proxy_without_consumers.aclose.return_value = close_future
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
    unused_proxy_with_consumers = Mock()
    unused_proxy_with_consumers.num_consumers.return_value = 99
    unused_proxy_with_consumers.last_used = time.monotonic() - 600
    close_future = asyncio.Future()
    close_future.set_result(True)
    unused_proxy_with_consumers.aclose.return_value = close_future
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
