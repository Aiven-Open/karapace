"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import http

from tenacity import retry, stop_after_delay, wait_fixed

from karapace.core.client import Client
from karapace.core.kafka.admin import KafkaAdminClient
from tests.integration.utils.cluster import RegistryDescription


async def test_health_check(
    registry_cluster: RegistryDescription, registry_async_client: Client, admin_client: KafkaAdminClient
) -> None:
    res = await registry_async_client.get("/_health")
    assert res.ok

    admin_client.delete_topic(registry_cluster.schemas_topic)

    @retry(stop=stop_after_delay(10), wait=wait_fixed(1), reraise=True)
    async def check_health():
        res = await registry_async_client.get("/_health")
        assert res.status_code == http.HTTPStatus.SERVICE_UNAVAILABLE, "should report unhealthy after topic has been deleted"

    await check_health()
