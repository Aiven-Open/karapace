"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from karapace.api.telemetry.middleware import setup_telemetry_middleware
from karapace.core.instrumentation.path_normalization import normalize_path
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation

from karapace.api.oidc.middleware import OIDCMiddleware
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config
import logging
import time

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
    _setup_prometheus_middleware(app=app)


def _setup_prometheus_middleware(app: FastAPI) -> None:
    """FastAPI equivalent of PrometheusInstrumentation.http_request_metrics_middleware (aiohttp).

    Records request metrics into prometheus_client counters so the /metrics
    endpoint serves live data, mirroring what the REST proxy does via its
    aiohttp middleware.
    """

    @app.middleware("http")
    async def prometheus_metrics_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start_time = time.monotonic()
        path = normalize_path(request.url.path)
        method = request.method

        PrometheusInstrumentation.karapace_http_requests_in_progress.labels(method, path).inc()
        try:
            response = await call_next(request)
            PrometheusInstrumentation.karapace_http_requests_total.labels(method, path, response.status_code).inc()
            return response
        except Exception:
            PrometheusInstrumentation.karapace_http_requests_total.labels(method, path, 500).inc()
            raise
        finally:
            PrometheusInstrumentation.karapace_http_requests_duration_seconds.labels(method, path).observe(
                time.monotonic() - start_time
            )
            PrometheusInstrumentation.karapace_http_requests_in_progress.labels(method, path).dec()
