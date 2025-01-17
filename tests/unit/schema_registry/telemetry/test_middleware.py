"""
schema_registry - telemetry middleware tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from fastapi import FastAPI, Request, Response, HTTPException
from opentelemetry.trace import SpanKind, Status, StatusCode
from schema_registry.telemetry.metrics import Metrics
from schema_registry.telemetry.meter import Meter
from schema_registry.telemetry.middleware import setup_telemetry_middleware, telemetry_middleware
from schema_registry.telemetry.tracer import Tracer
from unittest.mock import AsyncMock, call, MagicMock, patch

import logging
import pytest


@pytest.fixture
def metrics() -> MagicMock:
    return MagicMock(
        spec=Metrics,
        START_TIME_KEY="start_time",
        meter=MagicMock(spec=Meter),
        karapace_http_requests_in_progress=MagicMock(),
        karapace_http_requests_duration_seconds=MagicMock(),
        karapace_http_requests_total=MagicMock(),
    )


def test_setup_telemetry_middleware(caplog: LogCaptureFixture) -> None:
    app = AsyncMock(spec=FastAPI)
    with caplog.at_level(logging.INFO, logger="schema_registry.telemetry.middleware"):
        setup_telemetry_middleware(app=app)

        for log in caplog.records:
            assert log.name == "schema_registry.telemetry.middleware"
            assert log.levelname == "INFO"
            assert log.message == "Setting OTel tracing middleware"

        app.middleware.assert_called_once_with("http")
        app.middleware.return_value.assert_called_once_with(telemetry_middleware)


async def test_telemetry_middleware(metrics: MagicMock) -> None:
    tracer = MagicMock(spec=Tracer)

    request_mock = AsyncMock(spec=Request)
    request_mock.method = "GET"
    request_mock.url.path = "/test/inner-path"

    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 200

    call_next = AsyncMock()
    call_next.return_value = response_mock

    SpanStatus = MagicMock(spec=Status, status_code=StatusCode.OK)

    with (
        patch("schema_registry.telemetry.middleware.time.monotonic", return_value=1),
        patch("schema_registry.telemetry.middleware.Status", return_value=SpanStatus),
    ):
        response = await telemetry_middleware(request=request_mock, call_next=call_next, tracer=tracer, metrics=metrics)
        span = tracer.get_tracer.return_value.start_as_current_span.return_value.__enter__.return_value

        tracer.get_tracer.assert_called_once()
        tracer.get_tracer.return_value.start_as_current_span.assert_called_once_with(name="GET: /test", kind=SpanKind.SERVER)
        tracer.update_span_with_request.assert_called_once_with(request=request_mock, span=span)
        tracer.update_span_with_response.assert_called_once_with(response=response_mock, span=span)

        # Check that the request handler is called
        call_next.assert_awaited_once_with(request_mock)
        span.set_status.assert_called_once_with(SpanStatus)

        span.add_event.assert_has_calls(
            [
                call("Creating metering resources"),
                call("Metering requests in progress (increase)"),
                call("Calling request handler"),
                call("Metering total requests"),
                call("Update span with response details"),
                call("Metering request duration"),
                call("Metering requests in progress (decrease)"),
            ]
        )

        metrics.assert_has_calls(
            [
                call.karapace_http_requests_in_progress.add(
                    amount=1, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
                call.karapace_http_requests_total.add(
                    amount=1, attributes={"method": "GET", "path": "/test/inner-path", "status": 200, "resource": "test"}
                ),
                call.karapace_http_requests_duration_seconds.record(
                    amount=0, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
                call.karapace_http_requests_in_progress.add(
                    amount=-1, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
            ]
        )

        assert response == response_mock


async def test_telemetry_middleware_when_call_next_raises_unknown_error(metrics: MagicMock) -> None:
    tracer = MagicMock(spec=Tracer)

    request_mock = AsyncMock(spec=Request)
    request_mock.method = "GET"
    request_mock.url.path = "/test/inner-path"

    call_next = AsyncMock()
    call_next.side_effect = Exception("Something went wrong")

    SpanStatus = MagicMock(spec=Status, status_code=StatusCode.ERROR)

    with (
        patch("schema_registry.telemetry.middleware.time.monotonic", return_value=1),
        patch("schema_registry.telemetry.middleware.Status", return_value=SpanStatus),
    ):
        response = await telemetry_middleware(request=request_mock, call_next=call_next, tracer=tracer, metrics=metrics)
        span = tracer.get_tracer.return_value.start_as_current_span.return_value.__enter__.return_value

        tracer.get_tracer.assert_called_once()
        tracer.get_tracer.return_value.start_as_current_span.assert_called_once_with(name="GET: /test", kind=SpanKind.SERVER)
        tracer.update_span_with_request.assert_called_once_with(request=request_mock, span=span)

        # Check that the request handler is called, the exception is caught and the span
        # is not updated with response as there will be no response due to the exception
        call_next.assert_awaited_once_with(request_mock)
        span.set_status.assert_called_once_with(SpanStatus)
        span.record_exception.assert_called_once_with(call_next.side_effect)
        assert not tracer.update_span_with_response.called

        span.add_event.assert_has_calls(
            [
                call("Creating metering resources"),
                call("Metering requests in progress (increase)"),
                call("Calling request handler"),
                call("Metering total requests on exception"),
                call("Metering request duration"),
                call("Metering requests in progress (decrease)"),
            ]
        )

        metrics.assert_has_calls(
            [
                call.karapace_http_requests_in_progress.add(
                    amount=1, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
                call.karapace_http_requests_total.add(
                    amount=1, attributes={"method": "GET", "path": "/test/inner-path", "status": 0, "resource": "test"}
                ),
                call.karapace_http_requests_duration_seconds.record(
                    amount=0, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
                call.karapace_http_requests_in_progress.add(
                    amount=-1, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
            ]
        )

        assert not response


async def test_telemetry_middleware_when_call_next_raises_http_error(metrics: MagicMock) -> None:
    tracer = MagicMock(spec=Tracer)

    request_mock = AsyncMock(spec=Request)
    request_mock.method = "GET"
    request_mock.url.path = "/test/inner-path"

    call_next = AsyncMock()
    call_next.side_effect = HTTPException(status_code=401, detail="Unauthorized")

    SpanStatus = MagicMock(spec=Status, status_code=StatusCode.ERROR)

    with (
        patch("schema_registry.telemetry.middleware.time.monotonic", return_value=1),
        patch("schema_registry.telemetry.middleware.Status", return_value=SpanStatus),
    ):
        response = await telemetry_middleware(request=request_mock, call_next=call_next, tracer=tracer, metrics=metrics)
        span = tracer.get_tracer.return_value.start_as_current_span.return_value.__enter__.return_value

        tracer.get_tracer.assert_called_once()
        tracer.get_tracer.return_value.start_as_current_span.assert_called_once_with(name="GET: /test", kind=SpanKind.SERVER)
        tracer.update_span_with_request.assert_called_once_with(request=request_mock, span=span)

        # Check that the request handler is called, the exception is caught and the span
        # is not updated with response as there will be no response due to the exception
        call_next.assert_awaited_once_with(request_mock)
        span.set_status.assert_called_once_with(SpanStatus)
        span.record_exception.assert_called_once_with(call_next.side_effect)
        assert not tracer.update_span_with_response.called

        span.add_event.assert_has_calls(
            [
                call("Creating metering resources"),
                call("Metering requests in progress (increase)"),
                call("Calling request handler"),
                call("Metering total requests on exception"),
                call("Metering request duration"),
                call("Metering requests in progress (decrease)"),
            ]
        )

        metrics.assert_has_calls(
            [
                call.karapace_http_requests_in_progress.add(
                    amount=1, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
                call.karapace_http_requests_total.add(
                    amount=1, attributes={"method": "GET", "path": "/test/inner-path", "status": 401, "resource": "test"}
                ),
                call.karapace_http_requests_duration_seconds.record(
                    amount=0, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
                call.karapace_http_requests_in_progress.add(
                    amount=-1, attributes={"method": "GET", "path": "/test/inner-path", "resource": "test"}
                ),
            ]
        )

        assert not response
