"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, Request, Response
from opentelemetry.trace import SpanKind, Status, StatusCode
from schema_registry.telemetry.container import TelemetryContainer
from schema_registry.telemetry.metrics import HTTPRequestMetrics
from schema_registry.telemetry.tracer import Tracer

import logging

LOG = logging.getLogger(__name__)


@inject
async def telemetry_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    tracer: Tracer = Provide[TelemetryContainer.tracer],
    http_request_metrics: HTTPRequestMetrics = Provide[TelemetryContainer.http_request_metrics],
) -> Response | None:
    RESOURCE = http_request_metrics.get_resource_from_request(request=request)
    with tracer.get_tracer().start_as_current_span(name=f"{request.method}: /{RESOURCE}", kind=SpanKind.SERVER) as SPAN:
        ATTRIBUTES = http_request_metrics.start_request(request=request)
        tracer.update_span_with_request(request=request, span=SPAN)

        try:
            response: Response = await call_next(request)
            SPAN.set_status(Status(StatusCode.OK))
        except Exception as exc:
            http_request_metrics.record_request_exception(ATTRIBUTES=ATTRIBUTES, exc=exc)
            SPAN.set_status(Status(StatusCode.ERROR))
            SPAN.record_exception(exc)
            http_request_metrics.finish_request(ATTRIBUTES=ATTRIBUTES, request=request, response=None)
            raise exc
        else:
            tracer.update_span_with_response(response=response, span=SPAN)
            http_request_metrics.finish_request(
                ATTRIBUTES=ATTRIBUTES, request=request, response=response if "response" in locals() else None
            )
        return response


def setup_telemetry_middleware(app: FastAPI) -> None:
    LOG.info("Setting OTel tracing middleware")
    app.middleware("http")(telemetry_middleware)
