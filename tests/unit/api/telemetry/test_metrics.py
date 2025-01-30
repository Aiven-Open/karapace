"""
schema_registry - telemetry metrics tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from fastapi import HTTPException, Request, Response

from karapace.api.telemetry.meter import Meter
from karapace.api.telemetry.metrics import HTTPRequestMetrics


@pytest.fixture
def http_request_metrics() -> HTTPRequestMetrics:
    meter = MagicMock(spec=Meter)
    return HTTPRequestMetrics(meter=meter)


@pytest.fixture
def request_mock() -> AsyncMock:
    request_mock = AsyncMock(spec=Request)
    request_mock.method = "GET"
    request_mock.url.path = "/test/inner-path"
    return request_mock


def test_http_request_metrics_objects(http_request_metrics: HTTPRequestMetrics):
    assert hasattr(http_request_metrics, "karapace_http_requests_in_progress")
    assert hasattr(http_request_metrics, "karapace_http_requests_duration_seconds")
    assert hasattr(http_request_metrics, "karapace_http_requests_total")
    assert hasattr(http_request_metrics, "START_TIME_KEY")
    assert http_request_metrics.START_TIME_KEY == "start_time"

    http_request_metrics.meter.assert_has_calls(
        [
            call.get_meter(),
            call.get_meter().create_up_down_counter(
                name="karapace_http_requests_in_progress", description="In-progress requests for HTTP/TCP Protocol"
            ),
            call.get_meter(),
            call.get_meter().create_histogram(
                unit="seconds",
                name="karapace_http_requests_duration_seconds",
                description="Request Duration for HTTP/TCP Protocol",
            ),
            call.get_meter(),
            call.get_meter().create_counter(
                name="karapace_http_requests_total", description="Total Request Count for HTTP/TCP Protocol"
            ),
        ]
    )


def test_get_resource_from_request(http_request_metrics: HTTPRequestMetrics, request_mock: AsyncMock) -> None:
    assert http_request_metrics.get_resource_from_request(request=request_mock) == "test"


def test_start_request(http_request_metrics: HTTPRequestMetrics, request_mock: AsyncMock) -> None:
    http_request_metrics.karapace_http_requests_in_progress = MagicMock()

    with patch("karapace.api.telemetry.metrics.time.monotonic", return_value=1):
        ATTRIBUTES = http_request_metrics.start_request(request=request_mock)
        http_request_metrics.karapace_http_requests_in_progress.add.assert_called_with(amount=1, attributes=ATTRIBUTES)
        assert ATTRIBUTES == {"method": "GET", "path": "/test/inner-path", "resource": "test"}


def test_finish_request(http_request_metrics: HTTPRequestMetrics, request_mock: AsyncMock) -> None:
    ATTRIBUTES = {"method": "GET", "path": "/test/inner-path", "resource": "test"}
    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 200
    http_request_metrics.karapace_http_requests_duration_seconds = MagicMock()
    http_request_metrics.karapace_http_requests_in_progress = MagicMock()
    http_request_metrics.karapace_http_requests_total = MagicMock()

    with patch("karapace.api.telemetry.metrics.time.monotonic", return_value=3):
        http_request_metrics.finish_request(ATTRIBUTES=ATTRIBUTES, request=request_mock, response=response_mock)
        http_request_metrics.karapace_http_requests_duration_seconds.record.assert_called_with(
            amount=3 - request_mock.state.start_time, attributes=ATTRIBUTES
        )
        http_request_metrics.karapace_http_requests_in_progress.add.assert_called_with(amount=-1, attributes=ATTRIBUTES)
        http_request_metrics.karapace_http_requests_total.add.assert_called_with(
            amount=1, attributes={**ATTRIBUTES, "status": 200}
        )


def test_finish_request_without_response(http_request_metrics: HTTPRequestMetrics, request_mock: AsyncMock) -> None:
    ATTRIBUTES = {"method": "GET", "path": "/test/inner-path", "resource": "test"}
    http_request_metrics.karapace_http_requests_duration_seconds = MagicMock()
    http_request_metrics.karapace_http_requests_in_progress = MagicMock()
    http_request_metrics.karapace_http_requests_total = MagicMock()

    with patch("karapace.api.telemetry.metrics.time.monotonic", return_value=3):
        http_request_metrics.finish_request(ATTRIBUTES=ATTRIBUTES, request=request_mock, response=None)
        http_request_metrics.karapace_http_requests_duration_seconds.record.assert_called_with(
            amount=3 - request_mock.state.start_time, attributes=ATTRIBUTES
        )
        http_request_metrics.karapace_http_requests_in_progress.add.assert_called_with(amount=-1, attributes=ATTRIBUTES)
        http_request_metrics.karapace_http_requests_total.add.assert_called_with(
            amount=1, attributes={**ATTRIBUTES, "status": 0}
        )


def test_record_request_exception_http_exception(http_request_metrics: HTTPRequestMetrics) -> None:
    exception = HTTPException(status_code=404)
    ATTRIBUTES = {"method": "GET", "path": "/test/inner-path", "resource": "test"}
    http_request_metrics.karapace_http_requests_total = MagicMock()

    http_request_metrics.record_request_exception(ATTRIBUTES=ATTRIBUTES, exc=exception)
    http_request_metrics.karapace_http_requests_total.add.assert_called_with(
        amount=1, attributes={**ATTRIBUTES, "status": 404}
    )


def test_record_request_exception_uncaught_exception(http_request_metrics: HTTPRequestMetrics) -> None:
    ATTRIBUTES = {"method": "GET", "path": "/test/inner-path", "resource": "test"}
    http_request_metrics.karapace_http_requests_total = MagicMock()

    http_request_metrics.record_request_exception(ATTRIBUTES=ATTRIBUTES, exc=Exception())
    http_request_metrics.karapace_http_requests_total.add.assert_called_with(
        amount=1, attributes={**ATTRIBUTES, "status": 0}
    )
