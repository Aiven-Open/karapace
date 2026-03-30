"""
schema_registry - Prometheus metrics tests (standard + backwards-compatible karapace_* metrics)

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import pytest
from fastapi import FastAPI, HTTPException, Response
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import default as default_metrics

from karapace.api.middlewares import _karapace_requests_total, _karapace_requests_duration


@pytest.fixture(autouse=True)
def _clean_registries():
    """Prevent duplicate metric registration errors across tests."""
    yield
    collectors = list(REGISTRY._names_to_collectors.values())
    for c in set(collectors):
        try:
            REGISTRY.unregister(c)
        except Exception:
            pass


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()

    instrumentator = Instrumentator(
        should_group_status_codes=False,
        should_instrument_requests_inprogress=True,
        excluded_handlers=["/metrics"],
        inprogress_labels=True,
    )
    instrumentator.add(
        default_metrics(),
        _karapace_requests_total(),
        _karapace_requests_duration(),
    )
    instrumentator.instrument(app)

    @app.get("/subjects")
    async def subjects():
        return []

    @app.get("/config")
    async def config():
        return {"compatibilityLevel": "NONE"}

    @app.get("/fail")
    async def fail():
        raise HTTPException(status_code=404, detail="Not found")

    @app.get("/metrics")
    async def metrics():
        return Response(
            content=generate_latest(REGISTRY),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


class TestStandardMetrics:
    """Tests for prometheus-fastapi-instrumentator (standard metric names)."""

    def test_standard_metrics_populated_after_requests(self, client: TestClient) -> None:
        client.get("/subjects")

        response = client.get("/metrics")
        body = response.text

        assert "http_requests_total{" in body
        assert "http_request_duration_seconds" in body

    def test_standard_metrics_record_error_status(self, client: TestClient) -> None:
        client.get("/fail")

        response = client.get("/metrics")
        body = response.text
        assert 'status="404"' in body

    def test_standard_metrics_not_grouped(self, client: TestClient) -> None:
        """Status codes should be exact (404), not grouped (4xx)."""
        client.get("/fail")

        response = client.get("/metrics")
        body = response.text
        assert 'status="4xx"' not in body

    def test_metrics_endpoint_not_instrumented(self, client: TestClient) -> None:
        """The /metrics endpoint itself should be excluded from instrumentation."""
        client.get("/metrics")
        client.get("/metrics")

        response = client.get("/metrics")
        body = response.text
        assert 'handler="/metrics"' not in body


class TestKarapaceMetrics:
    """Tests for backwards-compatible karapace_* metrics (via instrumentator .add())."""

    def test_karapace_metrics_populated_after_requests(self, client: TestClient) -> None:
        client.get("/subjects")
        client.get("/config")

        response = client.get("/metrics")
        body = response.text

        assert "karapace_http_requests_total{" in body
        assert "karapace_http_requests_duration_seconds" in body

    def test_karapace_metrics_record_error_status(self, client: TestClient) -> None:
        client.get("/fail")

        response = client.get("/metrics")
        body = response.text
        assert 'path="/fail"' in body

    def test_karapace_metrics_use_path_label(self, client: TestClient) -> None:
        """Karapace metrics use 'path' label (not 'handler') for backwards compat."""
        client.get("/subjects")

        response = client.get("/metrics")
        body = response.text
        assert 'path="/subjects"' in body


class TestMetricsEndpoint:
    """Tests for the /metrics endpoint."""

    def test_metrics_endpoint_exists(self, client: TestClient) -> None:
        response = client.get("/metrics")
        assert response.status_code == 200

    def test_both_metric_systems_present(self, client: TestClient) -> None:
        client.get("/subjects")

        response = client.get("/metrics")
        body = response.text

        assert "http_requests_total{" in body
        assert "karapace_http_requests_total{" in body

    def test_metrics_with_prometheus_accept_header(self, client: TestClient) -> None:
        """Prometheus-style Accept header should not cause 406."""
        prometheus_accept = (
            "application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,"
            "text/plain;version=0.0.4;q=0.5,*/*;q=0.1"
        )
        response = client.get("/metrics", headers={"Accept": prometheus_accept})
        assert response.status_code == 200
