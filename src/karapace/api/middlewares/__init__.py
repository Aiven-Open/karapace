"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Awaitable, Callable
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from karapace.api.content_type import check_schema_headers
from karapace.api.telemetry.middleware import setup_telemetry_middleware

from karapace.api.oidc.middleware import OIDCMiddleware
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config
import logging

log = logging.getLogger(__name__)


def setup_middlewares(app: FastAPI, config: Config) -> None:
    oidc_middleware = OIDCMiddleware(config)

    @app.middleware("http")
    async def set_content_types(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        try:
            response_content_type = check_schema_headers(request)
        except HTTPException as exc:
            return JSONResponse(
                status_code=exc.status_code,
                headers=exc.headers,
                content=exc.detail,
            )

        # Schema registry supports application/octet-stream, assumption is JSON object body.
        # Force internally to use application/json in this case for compatibility.
        if request.headers.get("Content-Type") == "application/octet-stream":
            new_headers = request.headers.mutablecopy()
            new_headers["Content-Type"] = "application/json"
            request._headers = new_headers
            request.scope.update(headers=request.headers.raw)

        # Check for bearer token in header
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            try:
                payload = oidc_middleware.validate_jwt(auth_header.split(" ", 1)[1])
                request.state.user = payload
            except AuthenticationError as e:
                return JSONResponse(
                    status_code=401,
                    content={"error": "Unauthorized", "reason": str(e)},
                )

        response = await call_next(request)
        response.headers["Content-Type"] = response_content_type
        return response

    setup_telemetry_middleware(app=app)
