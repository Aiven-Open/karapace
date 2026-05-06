"""
karapace - prometheus instrumentation tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import logging
from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock, call, patch

import aiohttp.web
import pytest
from _pytest.logging import LogCaptureFixture
from prometheus_client import CollectorRegistry, Counter, Gauge

from karapace.core.instrumentation.prometheus import PrometheusInstrumentation
from karapace.rapu import HTTPResponse, RestApp


class DummyRequest:
    def __init__(self, path: str, method: str, app: dict):
        self.path = path
        self.method = method
        self.app = app


def _make_metric_mocks(prometheus: PrometheusInstrumentation):
    """Create isolated metric mocks to avoid cross-test leakage.

    Returns a tuple: (app_metrics, in_progress_metric, in_progress_instance,
    total_metric, total_instance)
    """
    in_progress_instance = MagicMock()
    in_progress_instance.inc = MagicMock()
    in_progress_instance.dec = MagicMock()
    in_progress_metric = MagicMock()
    in_progress_metric.labels = MagicMock(return_value=in_progress_instance)

    total_instance = MagicMock()
    total_instance.inc = MagicMock()
    total_metric = MagicMock()
    total_metric.labels = MagicMock(return_value=total_instance)

    app_metrics = {
        prometheus.karapace_http_requests_in_progress: in_progress_metric,
        prometheus.karapace_http_requests_total: total_metric,
    }

    return (
        app_metrics,
        in_progress_metric,
        in_progress_instance,
        total_metric,
        total_instance,
    )


class TestPrometheusInstrumentation:
    @pytest.fixture
    def prometheus(self) -> PrometheusInstrumentation:
        return PrometheusInstrumentation()

    def test_constants(self, prometheus: PrometheusInstrumentation) -> None:
        assert isinstance(prometheus.registry, CollectorRegistry)

    def test_metric_types(self, prometheus: PrometheusInstrumentation) -> None:
        assert isinstance(prometheus.karapace_http_requests_total, Counter)
        assert isinstance(prometheus.karapace_http_requests_in_progress, Gauge)

    def test_metric_values(self, prometheus: PrometheusInstrumentation) -> None:
        # `_total` suffix is stripped off the metric name for `Counters`, but needed for clarity.
        assert repr(prometheus.karapace_http_requests_total) == "prometheus_client.metrics.Counter(karapace_http_requests)"
        assert (
            repr(prometheus.karapace_http_requests_in_progress)
            == "prometheus_client.metrics.Gauge(karapace_http_requests_in_progress)"
        )

    def test_setup_metrics(self, caplog: LogCaptureFixture, prometheus: PrometheusInstrumentation) -> None:
        app = AsyncMock(spec=RestApp, app=AsyncMock(spec=aiohttp.web.Application))

        with caplog.at_level(logging.INFO, logger="karapace.core.instrumentation.prometheus"):
            prometheus.setup_metrics(app=app)

            app.route.assert_called_once_with(
                prometheus.METRICS_ENDPOINT_PATH,
                callback=prometheus.serve_metrics,
                method="GET",
                schema_request=False,
                with_request=False,
                json_body=False,
                auth=None,
            )
            app.app.middlewares.insert.assert_called_once_with(0, prometheus.http_request_metrics_middleware)
            app.app.__setitem__.assert_has_calls(
                [
                    call(prometheus.karapace_http_requests_total, prometheus.karapace_http_requests_total),
                    call(prometheus.karapace_http_requests_in_progress, prometheus.karapace_http_requests_in_progress),
                ]
            )
            for log in caplog.records:
                assert log.name == "karapace.core.instrumentation.prometheus"
                assert log.levelname == "INFO"
                assert log.message == "Setting up prometheus metrics"

    @patch("karapace.core.instrumentation.prometheus.generate_latest")
    async def test_serve_metrics(self, generate_latest: MagicMock, prometheus: PrometheusInstrumentation) -> None:
        mock_metrics_data = b"# HELP test_metric Test metric\n# TYPE test_metric counter\ntest_metric 1\n"
        generate_latest.return_value = mock_metrics_data

        with pytest.raises(HTTPResponse) as exc_info:
            await prometheus.serve_metrics()

        generate_latest.assert_called_once_with(prometheus.registry)

        # Verify HTTPResponse has correct attributes
        assert exc_info.value.status == HTTPStatus.OK
        assert exc_info.value.headers.get("Content-Type") == prometheus.CONTENT_TYPE_LATEST
        assert exc_info.value.body == mock_metrics_data

    async def test_http_request_metrics_middleware(self, prometheus: PrometheusInstrumentation) -> None:
        (
            app_metrics,
            in_progress_metric,
            in_progress_instance,
            total_metric,
            total_instance,
        ) = _make_metric_mocks(prometheus)

        request = DummyRequest(path="/path", method="GET", app=app_metrics)

        response = MagicMock(status=200)

        called: list = []

        async def handler(req):
            called.append(req)
            return response

        await prometheus.http_request_metrics_middleware(request=request, handler=handler)

        # Handler was invoked with the request
        assert called == [request]

        # In-progress gauge incremented and then decremented
        in_progress_metric.labels.assert_called_with("GET", "/path")
        in_progress_instance.inc.assert_called_once()
        in_progress_instance.dec.assert_called_once()

        # Total should be incremented for successful response
        total_metric.labels.assert_called_with("GET", "/path", response.status)
        total_instance.inc.assert_called_once()

    async def test_http_request_metrics_middleware_exception(self, prometheus: PrometheusInstrumentation) -> None:
        (
            app_metrics,
            in_progress_metric,
            in_progress_instance,
            total_metric,
            total_instance,
        ) = _make_metric_mocks(prometheus)

        request = DummyRequest(path="/error", method="POST", app=app_metrics)

        async def handler(req):
            raise Exception("boom")

        with pytest.raises(Exception):
            await prometheus.http_request_metrics_middleware(request=request, handler=handler)

        # In-progress gauge should have been incremented and eventually decremented
        in_progress_metric.labels.assert_called_with("POST", "/error")
        in_progress_instance.inc.assert_called_once()
        in_progress_instance.dec.assert_called_once()

        # Total should NOT be incremented when handler raises a non-HTTP exception
        total_metric.labels.assert_not_called()

    async def test_http_request_metrics_middleware_normalizes_path(self, prometheus: PrometheusInstrumentation) -> None:
        """Verify that the middleware normalizes dynamic path segments to prevent unbounded metric cardinality."""
        (
            app_metrics,
            in_progress_metric,
            in_progress_instance,
            total_metric,
            total_instance,
        ) = _make_metric_mocks(prometheus)

        request = DummyRequest(path="/schemas/ids/878916964", method="GET", app=app_metrics)

        response = MagicMock(status=200)

        async def handler(req):
            return response

        await prometheus.http_request_metrics_middleware(request=request, handler=handler)

        in_progress_metric.labels.assert_called_with("GET", "/schemas/ids/{id}")
        total_metric.labels.assert_called_with("GET", "/schemas/ids/{id}", response.status)

    async def test_http_request_metrics_middleware_normalizes_subject_version_path(
        self, prometheus: PrometheusInstrumentation
    ) -> None:
        (
            app_metrics,
            in_progress_metric,
            in_progress_instance,
            total_metric,
            total_instance,
        ) = _make_metric_mocks(prometheus)

        request = DummyRequest(path="/subjects/my-subject/versions/3", method="GET", app=app_metrics)

        response = MagicMock(status=200)

        async def handler(req):
            return response

        await prometheus.http_request_metrics_middleware(request=request, handler=handler)

        in_progress_metric.labels.assert_called_with("GET", "/subjects/{subject}/versions/{version}")
        total_metric.labels.assert_called_with("GET", "/subjects/{subject}/versions/{version}", response.status)

    async def test_http_request_metrics_middleware_normalizes_config_subject_path(
        self, prometheus: PrometheusInstrumentation
    ) -> None:
        (
            app_metrics,
            in_progress_metric,
            in_progress_instance,
            total_metric,
            total_instance,
        ) = _make_metric_mocks(prometheus)

        request = DummyRequest(path="/config/journeys_v1-dbx", method="GET", app=app_metrics)

        response = MagicMock(status=200)

        async def handler(req):
            return response

        await prometheus.http_request_metrics_middleware(request=request, handler=handler)

        in_progress_metric.labels.assert_called_with("GET", "/config/{subject}")
        total_metric.labels.assert_called_with("GET", "/config/{subject}", response.status)
