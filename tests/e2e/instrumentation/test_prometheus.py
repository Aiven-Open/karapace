"""
karapace - prometheus instrumentation tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from http import HTTPStatus

from prometheus_client.parser import text_string_to_metric_families

from karapace.core.client import Client, Result

METRICS_ENDPOINT_PATH = "/metrics"


async def test_metrics_endpoint(registry_async_client: Client) -> None:
    result: Result = await registry_async_client.get(
        METRICS_ENDPOINT_PATH,
        json_response=False,
    )
    assert result.status_code == HTTPStatus.OK.value


async def test_metrics_endpoint_with_prometheus_accept_header(registry_async_client: Client) -> None:
    """Regression test: Prometheus sends Accept headers that don't match schema-registry content types.

    The /metrics endpoint must be excluded from the schema-registry content negotiation
    middleware, otherwise it returns HTTP 406 Not Acceptable.
    """
    prometheus_accept = (
        "application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,"
        "text/plain;version=0.0.4;q=0.5,*/*;q=0.1"
    )
    result: Result = await registry_async_client.get(
        METRICS_ENDPOINT_PATH,
        headers={"Accept": prometheus_accept},
        json_response=False,
    )
    assert result.status_code == HTTPStatus.OK.value


async def test_metrics_endpoint_parsed_response(registry_async_client: Client) -> None:
    result: Result = await registry_async_client.get(
        METRICS_ENDPOINT_PATH,
        json_response=False,
    )
    metrics = [family.name for family in text_string_to_metric_families(result.json_result)]

    # Standard metrics (from prometheus-fastapi-instrumentator)
    assert "http_requests" in metrics
    assert "http_request_duration_seconds" in metrics
    assert "http_requests_inprogress" in metrics

    # Backwards-compatible karapace_* metrics
    assert "karapace_http_requests" in metrics
    assert "karapace_http_requests_duration_seconds" in metrics
