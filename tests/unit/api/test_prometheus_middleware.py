"""
schema_registry - FastAPI prometheus metrics middleware tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from unittest.mock import MagicMock, patch

from fastapi import FastAPI

from karapace.api.middlewares import _setup_prometheus_middleware
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation


class TestPrometheusMetricsMiddleware:
    def test_setup_registers_middleware(self) -> None:
        app = MagicMock(spec=FastAPI)
        _setup_prometheus_middleware(app=app)
        app.middleware.assert_called_once_with("http")


class TestPrometheusMetricsMiddlewareIntegration:
    """Integration test using a real FastAPI app and TestClient."""

    def test_metrics_populated_after_requests(self) -> None:
        from fastapi.testclient import TestClient
        from prometheus_client import CollectorRegistry

        app = FastAPI()
        _setup_prometheus_middleware(app=app)

        @app.get("/subjects")
        async def subjects():
            return []

        @app.get("/config")
        async def config():
            return {"compatibilityLevel": "NONE"}

        # Use a fresh registry to avoid cross-test pollution
        test_registry = CollectorRegistry()
        with (
            patch.object(
                PrometheusInstrumentation,
                "karapace_http_requests_total",
                PrometheusInstrumentation.karapace_http_requests_total.__class__(
                    registry=test_registry,
                    name="karapace_http_requests_total_test",
                    documentation="test",
                    labelnames=("method", "path", "status"),
                ),
            ),
            patch.object(
                PrometheusInstrumentation,
                "karapace_http_requests_duration_seconds",
                PrometheusInstrumentation.karapace_http_requests_duration_seconds.__class__(
                    registry=test_registry,
                    name="karapace_http_requests_duration_seconds_test",
                    documentation="test",
                    labelnames=("method", "path"),
                ),
            ),
            patch.object(
                PrometheusInstrumentation,
                "karapace_http_requests_in_progress",
                PrometheusInstrumentation.karapace_http_requests_in_progress.__class__(
                    registry=test_registry,
                    name="karapace_http_requests_in_progress_test",
                    documentation="test",
                    labelnames=("method", "path"),
                ),
            ),
        ):
            client = TestClient(app)

            client.get("/subjects")
            client.get("/subjects")
            client.get("/config")

            total = PrometheusInstrumentation.karapace_http_requests_total
            assert total.labels("GET", "/subjects", 200)._value.get() == 2.0
            assert total.labels("GET", "/config", 200)._value.get() == 1.0

            in_progress = PrometheusInstrumentation.karapace_http_requests_in_progress
            assert in_progress.labels("GET", "/subjects")._value.get() == 0.0
            assert in_progress.labels("GET", "/config")._value.get() == 0.0

            duration = PrometheusInstrumentation.karapace_http_requests_duration_seconds
            assert duration.labels("GET", "/subjects")._sum.get() > 0
            assert duration.labels("GET", "/config")._sum.get() > 0

    def test_metrics_record_error_status(self) -> None:
        from fastapi import HTTPException
        from fastapi.testclient import TestClient
        from prometheus_client import CollectorRegistry

        app = FastAPI()
        _setup_prometheus_middleware(app=app)

        @app.get("/fail")
        async def fail():
            raise HTTPException(status_code=404, detail="Not found")

        test_registry = CollectorRegistry()
        with patch.object(
            PrometheusInstrumentation,
            "karapace_http_requests_total",
            PrometheusInstrumentation.karapace_http_requests_total.__class__(
                registry=test_registry,
                name="karapace_http_requests_total_err",
                documentation="test",
                labelnames=("method", "path", "status"),
            ),
        ):
            client = TestClient(app, raise_server_exceptions=False)
            client.get("/fail")

            total = PrometheusInstrumentation.karapace_http_requests_total
            assert total.labels("GET", "/fail", 404)._value.get() == 1.0

    def test_metrics_normalize_paths(self) -> None:
        from fastapi.testclient import TestClient
        from prometheus_client import CollectorRegistry

        app = FastAPI()
        _setup_prometheus_middleware(app=app)

        @app.get("/subjects/{subject}/versions/{version}")
        async def subject_version(subject: str, version: str):
            return {}

        test_registry = CollectorRegistry()
        with patch.object(
            PrometheusInstrumentation,
            "karapace_http_requests_total",
            PrometheusInstrumentation.karapace_http_requests_total.__class__(
                registry=test_registry,
                name="karapace_http_requests_total_norm",
                documentation="test",
                labelnames=("method", "path", "status"),
            ),
        ):
            client = TestClient(app)
            client.get("/subjects/my-subject/versions/3")
            client.get("/subjects/other-subject/versions/1")

            total = PrometheusInstrumentation.karapace_http_requests_total
            assert total.labels("GET", "/subjects/{subject}/versions/{version}", 200)._value.get() == 2.0
