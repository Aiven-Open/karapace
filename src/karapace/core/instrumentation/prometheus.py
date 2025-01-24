"""
karapace - prometheus instrumentation

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
# mypy: disable-error-code="call-overload"

from __future__ import annotations

from aiohttp.web import middleware, Request, Response
from collections.abc import Awaitable, Callable
from karapace.core.rapu import RestApp
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest, Histogram
from typing import Final

import logging
import time

LOG = logging.getLogger(__name__)


class PrometheusInstrumentation:
    METRICS_ENDPOINT_PATH: Final[str] = "/metrics"
    CONTENT_TYPE_LATEST: Final[str] = "text/plain; version=0.0.4; charset=utf-8"
    START_TIME_REQUEST_KEY: Final[str] = "start_time"

    registry: Final[CollectorRegistry] = CollectorRegistry()

    karapace_http_requests_total: Final[Counter] = Counter(
        registry=registry,
        name="karapace_http_requests_total",
        documentation="Total Request Count for HTTP/TCP Protocol",
        labelnames=("method", "path", "status"),
    )

    karapace_http_requests_duration_seconds: Final[Histogram] = Histogram(
        registry=registry,
        name="karapace_http_requests_duration_seconds",
        documentation="Request Duration for HTTP/TCP Protocol",
        labelnames=("method", "path"),
    )

    karapace_http_requests_in_progress: Final[Gauge] = Gauge(
        registry=registry,
        name="karapace_http_requests_in_progress",
        documentation="In-progress requests for HTTP/TCP Protocol",
        labelnames=("method", "path"),
    )

    @classmethod
    def setup_metrics(cls, *, app: RestApp) -> None:
        LOG.info("Setting up prometheus metrics")
        app.route(
            cls.METRICS_ENDPOINT_PATH,
            callback=cls.serve_metrics,
            method="GET",
            schema_request=False,
            with_request=False,
            json_body=False,
            auth=None,
        )
        app.app.middlewares.insert(0, cls.http_request_metrics_middleware)  # type: ignore[arg-type]

        # disable-error-code="call-overload" is used at the top of this file to allow mypy checks.
        # the issue is in the type difference (Counter, Gauge, etc) of the arguments which we are
        # passing to `__setitem__()`, but we need to keep these objects in the `app.app` dict.
        app.app[cls.karapace_http_requests_total] = cls.karapace_http_requests_total
        app.app[cls.karapace_http_requests_duration_seconds] = cls.karapace_http_requests_duration_seconds
        app.app[cls.karapace_http_requests_in_progress] = cls.karapace_http_requests_in_progress

    @classmethod
    async def serve_metrics(cls) -> bytes:
        return generate_latest(cls.registry)

    @classmethod
    @middleware
    async def http_request_metrics_middleware(
        cls,
        request: Request,
        handler: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request[cls.START_TIME_REQUEST_KEY] = time.time()

        # Extract request labels
        path = request.path
        method = request.method

        # Increment requests in progress before handler
        request.app[cls.karapace_http_requests_in_progress].labels(method, path).inc()

        # Call request handler
        response: Response = await handler(request)

        # Instrument request duration
        request.app[cls.karapace_http_requests_duration_seconds].labels(method, path).observe(
            time.time() - request[cls.START_TIME_REQUEST_KEY]
        )

        # Instrument total requests
        request.app[cls.karapace_http_requests_total].labels(method, path, response.status).inc()

        # Decrement requests in progress after handler
        request.app[cls.karapace_http_requests_in_progress].labels(method, path).dec()

        return response
