"""
schema_registry - prometheus instrumentation middleware tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from fastapi import FastAPI, Request, Response
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from schema_registry.middlewares.prometheus import prometheus_middleware, setup_prometheus_middleware
from starlette.datastructures import State
from unittest.mock import AsyncMock, call, MagicMock, patch

import logging


def test_setup_prometheus_middleware(caplog: LogCaptureFixture) -> None:
    app = AsyncMock(spec=FastAPI)
    with caplog.at_level(logging.INFO, logger="schema_registry.middlewares.prometheus"):
        setup_prometheus_middleware(app=app)

        for log in caplog.records:
            assert log.name == "schema_registry.middlewares.prometheus"
            assert log.levelname == "INFO"
            assert log.message == "Setting up prometheus middleware for metrics"

        app.middleware.assert_called_once_with("http")
        app.middleware.return_value.assert_called_once_with(prometheus_middleware)


async def test_prometheus_middleware() -> None:
    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 200

    call_next = AsyncMock()
    call_next.return_value = response_mock

    request = AsyncMock(spec=Request)
    request.state = MagicMock(spec=State)

    prometheus = MagicMock(spec=PrometheusInstrumentation, START_TIME_REQUEST_KEY="start_time")

    with patch("schema_registry.middlewares.prometheus.time.monotonic", return_value=1):
        response = await prometheus_middleware(request=request, call_next=call_next, prometheus=prometheus)

        # Check that the `start_time` for the request is set
        assert hasattr(request.state, "start_time")
        assert getattr(request.state, "start_time") == 1

        # Check that `karapace_http_requests_in_progress` metric is incremented/decremented
        prometheus.karapace_http_requests_in_progress.labels.assert_has_calls(
            [
                call(method=request.method, path=request.url.path),
                call().inc(),
                call(method=request.method, path=request.url.path),
                call().dec(),
            ]
        )

        # Check that `karapace_http_requests_duration_seconds` metric is observed
        prometheus.karapace_http_requests_duration_seconds.labels.assert_has_calls(
            [
                call(method=request.method, path=request.url.path),
                call().observe(0),
            ]
        )

        # Check that the request handler is called
        call_next.assert_awaited_once_with(request)

        # Check that `karapace_http_requests_total` metric is incremented/decremented
        prometheus.karapace_http_requests_total.labels.assert_has_calls(
            [
                call(method=request.method, path=request.url.path, status=response.status_code),
                call().inc(),
            ]
        )

        assert response == response_mock
