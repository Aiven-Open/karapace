"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from karapace.api.telemetry.middleware import setup_telemetry_middleware
from karapace.core.instrumentation.path_normalization import normalize_path
from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import Info
from prometheus_fastapi_instrumentator.metrics import default as default_metrics

from karapace.api.oidc.middleware import OIDCMiddleware
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config
import logging

log = logging.getLogger(__name__)


def setup_middlewares(app: FastAPI, config: Config) -> None:
    oidc_middleware = OIDCMiddleware(app=app, config=config)

    @app.middleware("http")
    async def set_content_types(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        # Skip schema-registry header checks and Content-Type override for docs (Swagger UI, ReDoc, OpenAPI JSON).
        if request.url.path in {"/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"}:
            return await call_next(request)

        # Check for skip paths like /_health and /metrics and bypass
        if request.url.path in config.sasl_oauthbearer_skip_auth_paths:
            return await call_next(request)

        # Check for bearer token in header
        auth_header = request.headers.get("Authorization")

        if config.sasl_oauthbearer_authorization_enabled:
            if not auth_header or not auth_header.startswith("Bearer "):
                # Fail fast if header is missing or invalid
                return JSONResponse(
                    status_code=401,
                    content={"error": "Unauthorized", "reason": "Missing or invalid Authorization header"},
                )

            # Header exists and starts with Bearer → validate JWT
            token = auth_header.split(" ", 1)[1]
            try:
                payload = oidc_middleware.validate_jwt(token)
                request.state.user = payload
                log.debug("Authenticated")
            except AuthenticationError:
                return JSONResponse(
                    status_code=401,
                    content={"error": "Unauthorized", "reason": "Invalid token/payload"},
                )

            try:
                oidc_middleware.authorize_request(payload, request.method)
            except HTTPException as e:
                return JSONResponse(
                    {"error": "Authorization error", "reason": e.detail},
                    status_code=e.status_code,
                )

        response = await call_next(request)

        content_type = getattr(request.state, "schema_response_content_type", None)
        if content_type:
            response.headers["Content-Type"] = content_type

        return response

    setup_telemetry_middleware(app=app)

    # Metrics via prometheus-fastapi-instrumentator; /metrics served by metrics_router.
    # .add() before .instrument(): Starlette defers middleware construction, so if the
    # instrumentations list is non-empty the middleware skips its built-in defaults.
    # We include default_metrics() explicitly to get both standard and karapace_* names.
    instrumentator = Instrumentator(
        should_group_status_codes=False,
        should_instrument_requests_inprogress=True,
        excluded_handlers=["/metrics"],
        inprogress_labels=True,
    )
    instrumentator.add(
        default_metrics(),
        _karapace_requests_total(),
        _karapace_requests_duration(),
    )
    instrumentator.instrument(app)


def _karapace_requests_total() -> Callable[[Info], None]:
    counter = Counter(
        "karapace_http_requests_total",
        "Total Request Count for HTTP/TCP Protocol",
        labelnames=("method", "path", "status"),
    )

    def instrumentation(info: Info) -> None:
        path = normalize_path(info.request.url.path)
        counter.labels(info.request.method, path, info.modified_status).inc()

    return instrumentation


def _karapace_requests_duration() -> Callable[[Info], None]:
    histogram = Histogram(
        "karapace_http_requests_duration_seconds",
        "Request Duration for HTTP/TCP Protocol",
        labelnames=("method", "path"),
    )

    def instrumentation(info: Info) -> None:
        path = normalize_path(info.request.url.path)
        histogram.labels(info.request.method, path).observe(info.modified_duration)

    return instrumentation
