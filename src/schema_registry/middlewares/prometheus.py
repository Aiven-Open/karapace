"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from dependency_injector.wiring import inject, Provide
from fastapi import Depends, FastAPI, Request, Response
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from schema_registry.container import SchemaRegistryContainer

import logging
import time

LOG = logging.getLogger(__name__)


@inject
async def prometheus_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    prometheus: PrometheusInstrumentation = Depends(Provide[SchemaRegistryContainer.karapace_container.prometheus]),
) -> Response:
    # Set start time for request
    setattr(request.state, prometheus.START_TIME_REQUEST_KEY, time.monotonic())

    # Extract request labels
    path = request.url.path
    method = request.method

    # Increment requests in progress before response handler
    prometheus.karapace_http_requests_in_progress.labels(method=method, path=path).inc()

    # Call request handler
    response: Response = await call_next(request)

    # Instrument request duration
    prometheus.karapace_http_requests_duration_seconds.labels(method=method, path=path).observe(
        time.monotonic() - getattr(request.state, prometheus.START_TIME_REQUEST_KEY)
    )

    # Instrument total requests
    prometheus.karapace_http_requests_total.labels(method=method, path=path, status=response.status_code).inc()

    # Decrement requests in progress after response handler
    prometheus.karapace_http_requests_in_progress.labels(method=method, path=path).dec()

    return response


def setup_prometheus_middleware(app: FastAPI) -> None:
    LOG.info("Setting up prometheus middleware for metrics")
    app.middleware("http")(prometheus_middleware)
