"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import logging

from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from http import HTTPStatus
from karapace.api.routers.errors import KarapaceValidationError, SchemaErrorCodes
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request as StarletteHTTPRequest

log = logging.getLogger(__name__)


def setup_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(_: StarletteHTTPRequest, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.detail)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(_: StarletteHTTPRequest, exc: RequestValidationError) -> JSONResponse:
        error_code = HTTPStatus.UNPROCESSABLE_ENTITY.value
        if isinstance(exc, KarapaceValidationError):
            error_code = exc.error_code
            message = exc.body
        else:
            message = exc.errors()
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error_code": error_code,
                "message": message,
            },
        )

    @app.exception_handler(Exception)
    async def global_exception_handler(_: StarletteHTTPRequest, exc: Exception) -> JSONResponse:
        """Handle all unhandled exceptions and return JSON responses instead of plain text."""
        log.exception("Unhandled exception: %s", exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                "message": "Internal server error",
            },
        )
