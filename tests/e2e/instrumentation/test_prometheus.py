"""
karapace - prometheus instrumentation tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from http import HTTPStatus
from karapace.core.client import Client, Result
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation
from prometheus_client.parser import text_string_to_metric_families


async def test_metrics_endpoint(registry_async_client: Client) -> None:
    result: Result = await registry_async_client.get(
        PrometheusInstrumentation.METRICS_ENDPOINT_PATH,
        json_response=False,
    )
    assert result.status_code == HTTPStatus.OK.value


async def test_metrics_endpoint_parsed_response(registry_async_client: Client) -> None:
    result: Result = await registry_async_client.get(
        PrometheusInstrumentation.METRICS_ENDPOINT_PATH,
        json_response=False,
    )
    metrics = [family.name for family in text_string_to_metric_families(result.json_result)]
    assert "karapace_http_requests" in metrics
    assert "karapace_http_requests_duration_seconds" in metrics
    assert "karapace_http_requests_in_progress" in metrics
