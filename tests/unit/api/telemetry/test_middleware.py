"""
schema_registry - telemetry middleware tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import logging
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from _pytest.logging import LogCaptureFixture
from fastapi import FastAPI, Request, Response
from opentelemetry.trace import SpanKind, Status, StatusCode

from karapace.core.instrumentation.meter import Meter
from karapace.api.telemetry.metrics import HTTPRequestMetrics
from karapace.api.telemetry.middleware import setup_telemetry_middleware, telemetry_middleware
from karapace.api.telemetry.tracer import Tracer


@pytest.fixture
def http_request_metrics() -> MagicMock:
    return MagicMock(
        spec=HTTPRequestMetrics,
        START_TIME_KEY="start_time",
        meter=MagicMock(spec=Meter),
        karapace_http_requests_in_progress=MagicMock(),
        karapace_http_requests_duration_seconds=MagicMock(),
        karapace_http_requests_total=MagicMock(),
        start_request=MagicMock(),
        get_resource_from_request=MagicMock(return_value="test"),
        record_request_exception=MagicMock(),
        finish_request=MagicMock(),
    )


def test_setup_telemetry_middleware(caplog: LogCaptureFixture) -> None:
    app = AsyncMock(spec=FastAPI)
    with caplog.at_level(logging.INFO, logger="karapace.api.telemetry.middleware"):
        setup_telemetry_middleware(app=app)

        for log in caplog.records:
            assert log.name == "karapace.api.telemetry.middleware"
            assert log.levelname == "INFO"
            assert log.message == "Setting OTel tracing middleware"

        app.middleware.assert_called_once_with("http")
        app.middleware.return_value.assert_called_once_with(telemetry_middleware)


async def test_telemetry_middleware(http_request_metrics: MagicMock) -> None:
    tracer = MagicMock(spec=Tracer)

    request_mock = AsyncMock(spec=Request)
    request_mock.method = "GET"
    request_mock.url.path = "/test/inner-path"

    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 200

    call_next = AsyncMock()
    call_next.return_value = response_mock

    SpanStatus = MagicMock(spec=Status, status_code=StatusCode.OK)

    with patch("karapace.api.telemetry.middleware.Status", return_value=SpanStatus):
        response = await telemetry_middleware(
            request=request_mock, call_next=call_next, tracer=tracer, http_request_metrics=http_request_metrics
        )
        span = tracer.get_tracer.return_value.start_as_current_span.return_value.__enter__.return_value

        tracer.get_tracer.assert_called_once()
        tracer.get_tracer.return_value.start_as_current_span.assert_called_once_with(name="GET: /test", kind=SpanKind.SERVER)
        tracer.update_span_with_request.assert_called_once_with(request=request_mock, span=span)
        tracer.update_span_with_response.assert_called_once_with(response=response_mock, span=span)

        # Check that the request handler is called
        call_next.assert_awaited_once_with(request_mock)
        span.set_status.assert_called_once_with(SpanStatus)

        http_request_metrics.assert_has_calls(
            [
                call.get_resource_from_request(request=request_mock),
                call.start_request(request=request_mock),
                call.finish_request(
                    ATTRIBUTES=http_request_metrics.start_request.return_value, request=request_mock, response=response_mock
                ),
            ]
        )

        assert response == response_mock


async def test_telemetry_middleware_call_next_exception(http_request_metrics: MagicMock) -> None:
    tracer = MagicMock(spec=Tracer)

    request_mock = AsyncMock(spec=Request)
    request_mock.method = "GET"
    request_mock.url.path = "/test/inner-path"

    exception = Exception("Test exception")
    call_next = AsyncMock()
    call_next.side_effect = exception

    SpanStatus = MagicMock(spec=Status, status_code=StatusCode.ERROR)

    response = None
    with patch("karapace.api.telemetry.middleware.Status", return_value=SpanStatus):
        with pytest.raises(Exception) as excinfo:
            response = await telemetry_middleware(
                request=request_mock, call_next=call_next, tracer=tracer, http_request_metrics=http_request_metrics
            )

    assert "Test exception" in str(excinfo.value)
    assert response is None
    span = tracer.get_tracer.return_value.start_as_current_span.return_value.__enter__.return_value

    tracer.get_tracer.assert_called_once()
    tracer.get_tracer.return_value.start_as_current_span.assert_called_once_with(name="GET: /test", kind=SpanKind.SERVER)
    tracer.update_span_with_request.assert_called_once_with(request=request_mock, span=span)

    # Check that the request handler is called
    call_next.assert_awaited_once_with(request_mock)
    span.set_status.assert_called_once_with(SpanStatus)

    http_request_metrics.assert_has_calls(
        [
            call.get_resource_from_request(request=request_mock),
            call.start_request(request=request_mock),
            call.record_request_exception(ATTRIBUTES=http_request_metrics.start_request.return_value, exc=exception),
            call.finish_request(
                ATTRIBUTES=http_request_metrics.start_request.return_value, request=request_mock, response=None
            ),
        ]
    )
