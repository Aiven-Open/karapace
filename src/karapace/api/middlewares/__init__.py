"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from karapace.api.telemetry.middleware import setup_telemetry_middleware
from karapace.core.instrumentation.path_normalization import normalize_path
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import Info, request_size, requests, response_size

from karapace.api.oidc.middleware import OIDCMiddleware, TokenExpiredError
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config
import logging

log = logging.getLogger(__name__)


def _authenticate_and_authorize(request: Request, config: Config, oidc_middleware: OIDCMiddleware) -> JSONResponse | None:
    """Run the OIDC auth gate. Return a JSONResponse on failure, or None to continue."""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized", "reason": "Missing or invalid Authorization header"},
        )

    token = auth_header[len("Bearer ") :].strip()
    if not token:
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized", "reason": "Missing or invalid Authorization header"},
        )

    try:
        payload = oidc_middleware.validate_jwt(token)
    except TokenExpiredError:
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized", "reason": "Token expired"},
        )
    except AuthenticationError:
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized", "reason": "Invalid token/payload"},
        )

    # Expose only the configured subject claim to handlers, never the full payload.
    # Prevents downstream code from picking up attacker-controlled claims (e.g. "roles")
    # and using them for authz decisions.
    request.state.user = payload.get(oidc_middleware.claim_name) if oidc_middleware.claim_name else None
    log.debug("Authenticated")

    if config.sasl_oauthbearer_authorization_enabled:
        try:
            oidc_middleware.authorize_request(payload, request.method)
        except HTTPException as e:
            return JSONResponse(
                {"error": "Authorization error", "reason": e.detail},
                status_code=e.status_code,
            )
    return None


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

        if config.sasl_oauthbearer_authentication_enabled:
            failure = _authenticate_and_authorize(request, config, oidc_middleware)
            if failure is not None:
                return failure

        response = await call_next(request)

        content_type = getattr(request.state, "schema_response_content_type", None)
        if content_type:
            response.headers["Content-Type"] = content_type

        return response

    setup_telemetry_middleware(app=app)

    # Metrics via prometheus-fastapi-instrumentator.
    # .add() before .instrument(): Starlette defers middleware construction, so if the
    # instrumentations list is non-empty the middleware skips its built-in defaults.
    # Latency histograms are intentionally omitted to limit Prometheus series cardinality.
    instrumentator = Instrumentator(
        should_group_status_codes=False,
        should_instrument_requests_inprogress=True,
        excluded_handlers=["/metrics"],
        inprogress_labels=True,
    )
    instrumentator.add(
        requests(),
        request_size(),
        response_size(),
        _karapace_requests_total(),
    )
    instrumentator.instrument(app).expose(app, include_in_schema=False)


def _karapace_requests_total() -> Callable[[Info], None]:
    """Deprecated: use http_requests_total instead. Subject to removal."""
    counter = Counter(
        "karapace_http_requests_total",
        "Deprecated: use http_requests_total. Total Request Count for HTTP/TCP Protocol",
        labelnames=("method", "path", "status"),
    )

    def instrumentation(info: Info) -> None:
        path = normalize_path(info.request.url.path)
        counter.labels(info.request.method, path, info.modified_status).inc()

    return instrumentation
