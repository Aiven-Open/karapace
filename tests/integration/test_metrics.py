import json
from typing import Callable, AsyncContextManager

from karapace.client import Client
from karapace.config import ConfigDefaults
from tests.utils import create_subject_name_factory, create_schema_name_factory
from tests.integration.conftest import fixture_registry_async_client_custom_config, fixture_registry_cluster_with_custom_config  # noqa
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
        assert "karapace_schema_reader_records_processed_total" in requests.get("http://localhost:9090/api/v1/label/__name__/values").text
