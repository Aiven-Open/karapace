"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from karapace.api.telemetry.middleware import setup_telemetry_middleware

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
