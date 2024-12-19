"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, Request, Response
from opentelemetry.metrics import Counter, Histogram, UpDownCounter
from opentelemetry.trace import SpanKind
from karapace.config import Config
from karapace.container import KarapaceContainer
from schema_registry.telemetry.container import TelemetryContainer
from schema_registry.telemetry.meter import Meter
from schema_registry.telemetry.tracer import Tracer

import logging
import time

LOG = logging.getLogger(__name__)


@inject
async def telemetry_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    tracer: Tracer = Provide[TelemetryContainer.tracer],
    meter: Meter = Provide[TelemetryContainer.meter],
    config: Config = Provide[KarapaceContainer.config],
) -> Response:
    resource = request.url.path.split("/")[1]
    with tracer.get_tracer().start_as_current_span(name=f"{request.method}: /{resource}", kind=SpanKind.SERVER) as span:
        span.add_event("Creating metering resources")
        karapace_http_requests_in_progress: UpDownCounter = meter.get_meter().create_up_down_counter(
            name="karapace_http_requests_in_progress",
            description="In-progress requests for HTTP/TCP Protocol",
        )
        karapace_http_requests_duration_seconds: Histogram = meter.get_meter().create_histogram(
            unit="seconds",
            name="karapace_http_requests_duration_seconds",
            description="Request Duration for HTTP/TCP Protocol",
        )
        karapace_http_requests_total: Counter = meter.get_meter().create_counter(
            name="karapace_http_requests_total",
            description="Total Request Count for HTTP/TCP Protocol",
        )

        # Set start time for request
        setattr(request.state, meter.START_TIME_KEY, time.monotonic())

        # Extract request labels
        path = request.url.path
        method = request.method

        # Increment requests in progress before response handler
        span.add_event("Metering requests in progress (increase)")
        karapace_http_requests_in_progress.add(amount=1, attributes={"method": method, "path": path})

        # Call request handler
        tracer.update_span_with_request(request=request, span=span)
        span.add_event("Calling request handler")
        response: Response = await call_next(request)
        tracer.update_span_with_response(response=response, span=span)

        # Instrument request duration
        span.add_event("Metering request duration")
        karapace_http_requests_duration_seconds.record(
            amount=(time.monotonic() - getattr(request.state, meter.START_TIME_KEY)),
            attributes={"method": method, "path": path},
        )

        # Instrument total requests
        span.add_event("Metering total requests")
        karapace_http_requests_total.add(
            amount=1, attributes={"method": method, "path": path, "status": response.status_code}
        )

        # Decrement requests in progress after response handler
        span.add_event("Metering requests in progress (decrease)")
        karapace_http_requests_in_progress.add(amount=-1, attributes={"method": method, "path": path})

        return response


def setup_telemetry_middleware(app: FastAPI) -> None:
    LOG.info("Setting OTel tracing middleware")
    app.middleware("http")(telemetry_middleware)
