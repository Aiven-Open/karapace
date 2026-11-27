"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from http import HTTPStatus
from karapace.api.content_type import SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE
from karapace.api.routers.errors import KarapaceValidationError, SchemaErrorCodes
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request as StarletteHTTPRequest


def setup_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: StarletteHTTPRequest, exc: StarletteHTTPException) -> JSONResponse:
        if (
            exc.status_code == status.HTTP_404_NOT_FOUND
            and exc.detail == "Not Found"
            and request.scope.get("route") is None
            and request.scope.get("endpoint") is None
        ):
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "Not Found",
                },
                media_type=SCHEMA_RESPONSE_DEFAULT_CONTENT_TYPE,
            )
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
