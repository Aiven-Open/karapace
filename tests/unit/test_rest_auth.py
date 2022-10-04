# pylint: disable=protected-access
from karapace.config import set_config_defaults
from karapace.kafka_rest_apis import KafkaRest

import time


class MockUserRestProxy:
    def __init__(self, num_consumers=0, last_used=0):
        self._last_used = last_used
        self._num_consumers = num_consumers

    @property
    def last_used(self) -> int:
        return self._last_used

    def num_consumers(self) -> int:
        return self._num_consumers

    async def aclose(self):
        pass


async def test_rest_proxy_janitor_default():
    config = set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
        }
    )
    instance = KafkaRest(config=config)

    instance.proxies["active_proxy_without_consumers"] = MockUserRestProxy(num_consumers=0, last_used=time.monotonic())
    instance.proxies["active_proxy_with_consumers"] = MockUserRestProxy(num_consumers=99, last_used=time.monotonic())

    instance.proxies["unused_proxy_without_consumers"] = MockUserRestProxy(num_consumers=0, last_used=time.monotonic() - 600)
    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_without_consumers") is None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 2

    # Proxy with consumers is not deleted without explicit config
    instance.proxies["unused_proxy_with_consumers"] = MockUserRestProxy(num_consumers=99, last_used=time.monotonic() - 600)
    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 3


async def test_rest_proxy_janitor_destructive():
    config = set_config_defaults(
        {
            "rest_authorization": True,
            "sasl_bootstrap_uri": "localhost:9094",
            "consumer_idle_disconnect_timeout": 120,
        }
    )
    instance = KafkaRest(config=config)

    instance.proxies["active_proxy_without_consumers"] = MockUserRestProxy(num_consumers=0, last_used=time.monotonic())
    instance.proxies["active_proxy_with_consumers"] = MockUserRestProxy(num_consumers=99, last_used=time.monotonic())

    instance.proxies["unused_proxy_without_consumers"] = MockUserRestProxy(num_consumers=0, last_used=time.monotonic() - 600)
    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_without_consumers") is None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 2

    # Proxy with consumers gets deleted after enough time has passed
    instance.proxies["unused_proxy_with_consumers"] = MockUserRestProxy(num_consumers=99, last_used=time.monotonic() - 600)
    await instance._disconnect_idle_proxy_if_any()
    assert instance.proxies.get("unused_proxy_with_consumers") is None
    assert instance.proxies.get("active_proxy_with_consumers") is not None
    assert instance.proxies.get("active_proxy_without_consumers") is not None
    assert len(instance.proxies) == 2
