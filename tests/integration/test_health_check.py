"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import http

from tenacity import retry, stop_after_delay, wait_fixed

from karapace.core.client import Client
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.version import __version__
from tests.integration.utils.cluster import RegistryDescription


async def test_health_check_with_non_schema_accept_header(registry_async_client: Client) -> None:
    """Regression test: /_health must be exempt from schema-registry content negotiation.

    Monitoring tools (e.g. Kubernetes probes) may send Accept headers that don't match
    schema-registry content types. This must not result in HTTP 406.
    """
    res = await registry_async_client.get("/_health", headers={"Accept": "text/plain, */*;q=0.1"})
    assert res.status_code in (
        http.HTTPStatus.OK,
        http.HTTPStatus.SERVICE_UNAVAILABLE,
    ), f"Expected 200 or 503, got {res.status_code}"


async def test_root_with_non_schema_accept_header(registry_async_client: Client) -> None:
    """Regression test: / must be exempt from schema-registry content negotiation."""
    res = await registry_async_client.get("/", headers={"Accept": "text/html"})
    assert res.ok, f"Expected 200, got {res.status_code}"


async def test_master_available_with_non_schema_accept_header(registry_async_client: Client) -> None:
    """Regression test: /master_available must be exempt from schema-registry content negotiation."""
    res = await registry_async_client.get("/master_available", headers={"Accept": "text/plain"})
    assert res.ok, f"Expected 200, got {res.status_code}"


async def test_health_check(
    registry_cluster: RegistryDescription, registry_async_client: Client, admin_client: KafkaAdminClient
) -> None:
    res = await registry_async_client.get("/_health")
    assert res.ok
    response = res.json()
    assert "karapace_version" in response
    assert response["karapace_version"] == __version__
    response_status = response.get("status")
    assert "schema_registry_startup_time_sec" in response_status
    assert response_status["schema_registry_startup_time_sec"] > 0

    admin_client.delete_topic(registry_cluster.schemas_topic)

    @retry(stop=stop_after_delay(10), wait=wait_fixed(1), reraise=True)
    async def check_health():
        res = await registry_async_client.get("/_health")
        assert res.status_code == http.HTTPStatus.SERVICE_UNAVAILABLE, "should report unhealthy after topic has been deleted"

    await check_health()
