"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, Request, Response
from opentelemetry.trace import SpanKind
from karapace.config import Config
from karapace.container import KarapaceContainer
from schema_registry.telemetry.container import TelemetryContainer
from schema_registry.telemetry.tracer import Tracer

import logging

LOG = logging.getLogger(__name__)


@inject
async def telemetry_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    tracer: Tracer = Provide[TelemetryContainer.tracer],
    config: Config = Provide[KarapaceContainer.config],
) -> Response:
    resource = request.url.path.split("/")[1]
    with tracer.get_tracer().start_as_current_span(name=f"{request.method}: /{resource}", kind=SpanKind.SERVER) as span:
        tracer.update_span_with_request(request=request, span=span, config=config)
        response: Response = await call_next(request)
        tracer.update_span_with_response(response=response, span=span)
        return response


def setup_telemetry_middleware(app: FastAPI) -> None:
    LOG.info("Setting OTel tracing middleware")
    app.middleware("http")(telemetry_middleware)
