from typing import Callable, AsyncContextManager

from karapace.client import Client
from karapace.config import ConfigDefaults
from tests.integration.conftest import fixture_registry_async_client_custom_config, fixture_registry_cluster_with_custom_config  # noqa
from prometheus_client.parser import text_string_to_metric_families
import requests


async def test_metrics_are_produced(
    registry_async_client_from_config: Callable[[ConfigDefaults], AsyncContextManager[Client]]) -> None:
    async with registry_async_client_from_config(
        {
            "stats_service": "prometheus",
            "prometheus_host": "127.0.0.1",
            "prometheus_port": 9090,
        }
    ) as client:
        metrics_available = requests.get("http://127.0.0.1:9090/metrics").text
        print(text_string_to_metric_families(metrics_available))
        assert "karapace_schema_reader_records_processed_total" in requests.get("http://localhost:9090/api/v1/label/__name__/values").text
