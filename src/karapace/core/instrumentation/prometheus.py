"""
karapace - prometheus instrumentation

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
# mypy: disable-error-code="call-overload"

from __future__ import annotations

from aiohttp.web import middleware, Request, Response
from collections.abc import Awaitable, Callable
from http import HTTPStatus
from karapace.rapu import HTTPResponse, RestApp
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest, Histogram
from typing import Final, NoReturn

import logging
import re
import time

LOG = logging.getLogger(__name__)

# Regex patterns for normalizing paths to prevent unbounded label cardinality
# UUIDs: 8-4-4-4-12 hex format
UUID_PATTERN = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
# Topic names after /topics/ - match until next slash or end
TOPIC_PATTERN = re.compile(r"(/topics/)([^/]+)")
# Schema IDs (numeric) after /schemas/ids/
SCHEMA_ID_PATTERN = re.compile(r"(/schemas/ids/)(\d+)")
# Subject names after /subjects/
SUBJECT_PATTERN = re.compile(r"(/subjects/)([^/]+)")
# Version numbers after /versions/
VERSION_PATTERN = re.compile(r"(/versions/)(\d+)")


def normalize_path(path: str) -> str:
    """Normalize a request path by replacing dynamic segments with placeholders.

    This prevents unbounded label cardinality in prometheus metrics which would
    cause memory growth over time as each unique path creates a new time series.

    Examples:
        /topics/my-topic -> /topics/{topic}
        /consumers/abc-123-def/instances/xyz-456 -> /consumers/{consumer_group}/instances/{instance}
        /schemas/ids/42 -> /schemas/ids/{id}
        /subjects/my-subject/versions/3 -> /subjects/{subject}/versions/{version}
    """
    # Replace UUIDs first (consumer groups, instances)
    normalized = UUID_PATTERN.sub("{uuid}", path)
    # Replace topic names
    normalized = TOPIC_PATTERN.sub(r"\1{topic}", normalized)
    # Replace schema IDs
    normalized = SCHEMA_ID_PATTERN.sub(r"\1{id}", normalized)
    # Replace subject names
    normalized = SUBJECT_PATTERN.sub(r"\1{subject}", normalized)
    # Replace version numbers
    normalized = VERSION_PATTERN.sub(r"\1{version}", normalized)
    return normalized


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
    async def serve_metrics(cls) -> NoReturn:
        metrics_data = generate_latest(cls.registry)
        # Raise HTTPResponse for RestApp compatibility
        raise HTTPResponse(
            body=metrics_data,
            status=HTTPStatus.OK,
            content_type=cls.CONTENT_TYPE_LATEST,
        )

    @classmethod
    @middleware
    async def http_request_metrics_middleware(
        cls,
        request: Request,
        handler: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request[cls.START_TIME_REQUEST_KEY] = time.time()

        # Extract request labels - normalize path to prevent unbounded cardinality
        path = normalize_path(request.path)
        method = request.method

        in_progress = request.app[cls.karapace_http_requests_in_progress].labels(method, path)
        in_progress.inc()

        try:
            response: Response = await handler(request)
            request.app[cls.karapace_http_requests_total].labels(method, path, response.status).inc()
            return response
        finally:
            try:
                request.app[cls.karapace_http_requests_duration_seconds].labels(method, path).observe(
                    time.time() - request[cls.START_TIME_REQUEST_KEY]
                )
            finally:
                in_progress.dec()
