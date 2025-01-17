"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, Request, Response, HTTPException
from opentelemetry.trace import SpanKind, Status, StatusCode
from schema_registry.telemetry.container import TelemetryContainer
from schema_registry.telemetry.metrics import Metrics
from schema_registry.telemetry.tracer import Tracer

import logging
import time

LOG = logging.getLogger(__name__)


@inject
async def telemetry_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    tracer: Tracer = Provide[TelemetryContainer.tracer],
    metrics: Metrics = Provide[TelemetryContainer.metrics],
) -> Response | None:
    RESOURCE = request.url.path.split("/")[1]
    with tracer.get_tracer().start_as_current_span(name=f"{request.method}: /{RESOURCE}", kind=SpanKind.SERVER) as SPAN:
        SPAN.add_event("Creating metering resources")

        # Set start time for request
        setattr(request.state, metrics.START_TIME_KEY, time.monotonic())
        tracer.update_span_with_request(request=request, span=SPAN)

        PATH = request.url.path
        METHOD = request.method
        ATTRIBUTES = {"method": METHOD, "path": PATH, "resource": RESOURCE}

        SPAN.add_event("Metering requests in progress (increase)")
        metrics.karapace_http_requests_in_progress.add(amount=1, attributes=ATTRIBUTES)

        try:
            SPAN.add_event("Calling request handler")
            response: Response = await call_next(request)
            SPAN.set_status(Status(StatusCode.OK))
        except Exception as exc:
            status = exc.status_code if isinstance(exc, HTTPException) else 0
            SPAN.add_event("Metering total requests on exception")
            metrics.karapace_http_requests_total.add(amount=1, attributes={**ATTRIBUTES, "status": status})
            SPAN.set_status(Status(StatusCode.ERROR))
            SPAN.record_exception(exc)
        else:
            SPAN.add_event("Metering total requests")
            metrics.karapace_http_requests_total.add(amount=1, attributes={**ATTRIBUTES, "status": response.status_code})
            SPAN.add_event("Update span with response details")
            tracer.update_span_with_response(response=response, span=SPAN)
            return response
        finally:
            SPAN.add_event("Metering request duration")
            metrics.karapace_http_requests_duration_seconds.record(
                amount=(time.monotonic() - getattr(request.state, metrics.START_TIME_KEY)),
                attributes=ATTRIBUTES,
            )
            SPAN.add_event("Metering requests in progress (decrease)")
            metrics.karapace_http_requests_in_progress.add(amount=-1, attributes=ATTRIBUTES)
    return None


def setup_telemetry_middleware(app: FastAPI) -> None:
    LOG.info("Setting OTel tracing middleware")
    app.middleware("http")(telemetry_middleware)
