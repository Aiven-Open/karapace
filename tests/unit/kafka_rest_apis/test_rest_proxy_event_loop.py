"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import asyncio
import pytest
import threading
import time

from karapace.core.container import KarapaceContainer
from karapace.core.serialization import SchemaRegistrySerializer
from karapace.kafka_rest_apis import UserRestProxy

# Generous margins: the checks below tell "blocked for ~10s" from "finished at once", so they stay reliable on a
# heavily loaded CI machine.
BLOCKING_CALL_TIMEOUT_SECONDS = 10.0
RELEASE_AFTER_SECONDS = 0.1
MAX_EXPECTED_DURATION_SECONDS = 5.0


def _user_rest_proxy(karapace_container: KarapaceContainer) -> UserRestProxy:
    config = karapace_container.config()
    serializer = SchemaRegistrySerializer(config=config)
    return UserRestProxy(config, 1, serializer, auth_expiry=None, verify_connection=False)


def _blocking_until(release: threading.Event, result: object) -> Callable[..., object]:
    def call(*_args: object, **_kwargs: object) -> object:
        # Stands in for a Kafka admin request waiting on a slow broker.
        release.wait(timeout=BLOCKING_CALL_TIMEOUT_SECONDS)
        return result

    return call


@pytest.mark.parametrize(
    ("admin_method", "result", "call"),
    [
        ("cluster_metadata", {"topics": {}, "brokers": []}, lambda proxy: proxy.cluster_metadata()),
        ("cluster_metadata", {"topics": {}, "brokers": []}, lambda proxy: proxy.cluster_metadata(["topic"])),
        ("get_offsets", {"beginning_offset": 0, "end_offset": 1}, lambda proxy: proxy.get_offsets("topic", 0)),
        ("get_topic_config", {}, lambda proxy: proxy.get_topic_config("topic")),
    ],
    ids=["cluster_metadata-all", "cluster_metadata-topics", "get_offsets", "get_topic_config"],
)
async def test_slow_admin_call_does_not_block_event_loop(
    karapace_container: KarapaceContainer,
    admin_method: str,
    result: object,
    call: Callable[[UserRestProxy], Awaitable[object]],
) -> None:
    release = threading.Event()
    proxy = _user_rest_proxy(karapace_container)
    proxy.admin_client = MagicMock()
    getattr(proxy.admin_client, admin_method).side_effect = _blocking_until(release, result)
    # Only an event loop that keeps running can get to this callback and let the admin call finish. If the call
    # blocks the loop itself, nothing releases it until its own timeout.
    asyncio.get_running_loop().call_later(RELEASE_AFTER_SECONDS, release.set)

    started = time.monotonic()
    await call(proxy)

    duration = time.monotonic() - started
    assert duration < MAX_EXPECTED_DURATION_SECONDS, f"the admin call blocked the event loop for {duration:.1f}s"


async def test_admin_calls_do_not_depend_on_the_shared_default_executor(karapace_container: KarapaceContainer) -> None:
    """Consumers use the loop's default executor; a saturated one must not starve (or be starved by) admin calls."""
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    release = threading.Event()
    proxy = _user_rest_proxy(karapace_container)
    proxy.admin_client = MagicMock()
    proxy.admin_client.get_topic_config.return_value = {}
    busy = loop.run_in_executor(None, release.wait, BLOCKING_CALL_TIMEOUT_SECONDS)  # occupies the only default worker

    try:
        assert await asyncio.wait_for(proxy.get_topic_config("topic"), timeout=MAX_EXPECTED_DURATION_SECONDS * 2) == {}
    finally:
        release.set()
        await busy
        await proxy.aclose()


async def test_admin_executor_is_shut_down_with_the_proxy(karapace_container: KarapaceContainer) -> None:
    proxy = _user_rest_proxy(karapace_container)

    await proxy.aclose()

    with pytest.raises(RuntimeError):
        proxy.admin_executor.submit(lambda: None)
