"""
karapace - prometheus instrumentation tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from karapace.rapu import RestApp
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from unittest.mock import AsyncMock, call, MagicMock, patch

import aiohttp.web
import logging
import pytest


class TestPrometheusInstrumentation:
    @pytest.fixture
    def prometheus(self) -> PrometheusInstrumentation:
        return PrometheusInstrumentation()

    def test_constants(self, prometheus: PrometheusInstrumentation) -> None:
        assert prometheus.START_TIME_REQUEST_KEY == "start_time"
        assert isinstance(prometheus.registry, CollectorRegistry)

    def test_metric_types(self, prometheus: PrometheusInstrumentation) -> None:
        assert isinstance(prometheus.karapace_http_requests_total, Counter)
        assert isinstance(prometheus.karapace_http_requests_duration_seconds, Histogram)
        assert isinstance(prometheus.karapace_http_requests_in_progress, Gauge)

    def test_metric_values(self, prometheus: PrometheusInstrumentation) -> None:
        # `_total` suffix is stripped off the metric name for `Counters`, but needed for clarity.
        assert repr(prometheus.karapace_http_requests_total) == "prometheus_client.metrics.Counter(karapace_http_requests)"
        assert (
            repr(prometheus.karapace_http_requests_duration_seconds)
            == "prometheus_client.metrics.Histogram(karapace_http_requests_duration_seconds)"
        )
        assert (
            repr(prometheus.karapace_http_requests_in_progress)
            == "prometheus_client.metrics.Gauge(karapace_http_requests_in_progress)"
        )

    def test_setup_metrics(self, caplog: LogCaptureFixture, prometheus: PrometheusInstrumentation) -> None:
        app = AsyncMock(spec=RestApp, app=AsyncMock(spec=aiohttp.web.Application))

        with caplog.at_level(logging.INFO, logger="karapace.instrumentation.prometheus"):
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
                    call(
                        prometheus.karapace_http_requests_duration_seconds,
                        prometheus.karapace_http_requests_duration_seconds,
                    ),
                    call(prometheus.karapace_http_requests_in_progress, prometheus.karapace_http_requests_in_progress),
                ]
            )
            for log in caplog.records:
                assert log.name == "karapace.instrumentation.prometheus"
                assert log.levelname == "INFO"
                assert log.message == "Setting up prometheus metrics"

    @patch("karapace.instrumentation.prometheus.generate_latest")
    async def test_serve_metrics(self, generate_latest: MagicMock, prometheus: PrometheusInstrumentation) -> None:
        await prometheus.serve_metrics()
        generate_latest.assert_called_once_with(prometheus.registry)

    @patch("karapace.instrumentation.prometheus.time")
    async def test_http_request_metrics_middleware(
        self,
        mock_time: MagicMock,
        prometheus: PrometheusInstrumentation,
    ) -> None:
        mock_time.time.return_value = 10
        request = AsyncMock(
            spec=aiohttp.web.Request, app=AsyncMock(spec=aiohttp.web.Application), path="/path", method="GET"
        )
        handler = AsyncMock(spec=aiohttp.web.RequestHandler, return_value=MagicMock(status=200))

        await prometheus.http_request_metrics_middleware(request=request, handler=handler)

        assert handler.assert_awaited_once  # extra assert is to ignore pylint [pointless-statement]
        request.__setitem__.assert_called_once_with(prometheus.START_TIME_REQUEST_KEY, 10)
        request.app[prometheus.karapace_http_requests_in_progress].labels.assert_has_calls(
            [
                call("GET", "/path"),
                call().inc(),
            ]
        )
        request.app[prometheus.karapace_http_requests_duration_seconds].labels.assert_has_calls(
            [
                call("GET", "/path"),
                call().observe(request.__getitem__.return_value.__rsub__.return_value),
            ]
        )
        request.app[prometheus.karapace_http_requests_total].labels.assert_has_calls(
            [
                call("GET", "/path", 200),
                call().inc(),
            ]
        )
        request.app[prometheus.karapace_http_requests_in_progress].labels.assert_has_calls(
            [
                call("GET", "/path"),
                call().dec(),
            ]
        )
