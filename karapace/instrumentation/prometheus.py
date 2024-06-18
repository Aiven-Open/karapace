"""
karapace - prometheus instrumentation

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from aiohttp.web import middleware, Request, RequestHandler, Response
from karapace.rapu import RestApp
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest, Histogram
from typing import ClassVar

import logging
import time

LOG = logging.getLogger(__name__)


class PrometheusInstrumentation:
    METRICS_ENDPOINT_PATH: ClassVar[str] = "/metrics"
    START_TIME_REQUEST_KEY: ClassVar[str] = "start_time"

    registry: ClassVar[CollectorRegistry] = CollectorRegistry()

    karapace_http_requests_total: ClassVar[Counter] = Counter(
        registry=registry,
        name="karapace_http_requests_total",
        documentation="Total Request Count for HTTP/TCP Protocol",
        labelnames=("method", "path", "status"),
    )

    karapace_http_requests_latency_seconds: ClassVar[Histogram] = Histogram(
        registry=registry,
        name="karapace_http_requests_latency_seconds",
        documentation="Request Duration for HTTP/TCP Protocol",
        labelnames=("method", "path"),
    )

    karapace_http_requests_in_progress: ClassVar[Gauge] = Gauge(
        registry=registry,
        name="karapace_http_requests_in_progress",
        documentation="Request Duration for HTTP/TCP Protocol",
        labelnames=("method", "path"),
    )

    @classmethod
    def setup_metrics(cls: PrometheusInstrumentation, *, app: RestApp) -> None:
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
        app.app.middlewares.insert(0, cls.http_request_metrics_middleware)
        app.app[cls.karapace_http_requests_total] = cls.karapace_http_requests_total
        app.app[cls.karapace_http_requests_latency_seconds] = cls.karapace_http_requests_latency_seconds
        app.app[cls.karapace_http_requests_in_progress] = cls.karapace_http_requests_in_progress

    @classmethod
    async def serve_metrics(cls: PrometheusInstrumentation) -> bytes:
        return generate_latest(cls.registry)

    @classmethod
    @middleware
    async def http_request_metrics_middleware(
        cls: PrometheusInstrumentation,
        request: Request,
        handler: RequestHandler,
    ) -> None:
        request[cls.START_TIME_REQUEST_KEY] = time.time()

        # Extract request labels
        path = request.path
        method = request.method

        # Increment requests in progress before handler
        request.app[cls.karapace_http_requests_in_progress].labels(method, path).inc()

        # Call request handler
        response: Response = await handler(request)

        # Instrument request latency
        request.app[cls.karapace_http_requests_latency_seconds].labels(method, path).observe(
            time.time() - request[cls.START_TIME_REQUEST_KEY]
        )

        # Instrument total requests
        request.app[cls.karapace_http_requests_total].labels(method, path, response.status).inc()

        # Decrement requests in progress after handler
        request.app[cls.karapace_http_requests_in_progress].labels(method, path).dec()

        return response
