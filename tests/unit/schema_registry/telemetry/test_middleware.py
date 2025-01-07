"""
schema_registry - telemetry middleware tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from fastapi import FastAPI, Request, Response
from opentelemetry.trace import SpanKind
from karapace.config import Config
from schema_registry.telemetry.middleware import setup_telemetry_middleware, telemetry_middleware
from schema_registry.telemetry.tracer import Tracer
from unittest.mock import AsyncMock, MagicMock

import logging


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


async def test_telemetry_middleware() -> None:
    tracer = MagicMock(spec=Tracer)

    request_mock = AsyncMock(spec=Request)
    request_mock.method = "GET"
    request_mock.url.path = "/test"

    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 200

    config_mock = MagicMock(spec=Config)

    call_next = AsyncMock()
    call_next.return_value = response_mock

    response = await telemetry_middleware(request=request_mock, call_next=call_next, tracer=tracer, config=config_mock)
    span = tracer.get_tracer.return_value.start_as_current_span.return_value.__enter__.return_value

    tracer.get_tracer.assert_called_once()
    tracer.get_tracer.return_value.start_as_current_span.assert_called_once_with(name="GET: /test", kind=SpanKind.SERVER)
    tracer.update_span_with_request.assert_called_once_with(request=request_mock, span=span, config=config_mock)
    tracer.update_span_with_response.assert_called_once_with(response=response_mock, span=span)

    # Check that the request handler is called
    call_next.assert_awaited_once_with(request_mock)

    assert response == response_mock
