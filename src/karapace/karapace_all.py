"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

# from aiohttp.web_log import AccessLogger
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from http import HTTPStatus
from karapace import version as karapace_version
from karapace.auth.auth import AuthenticatorAndAuthorizer
from karapace.auth.dependencies import AuthorizationDependencyManager
from karapace.config import Config
from karapace.content_type import check_schema_headers
from karapace.dependencies.config_dependency import ConfigDependencyManager
from karapace.dependencies.schema_registry_dependency import SchemaRegistryDependencyManager
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from karapace.routers.errors import KarapaceValidationError
from karapace.schema_registry import KarapaceSchemaRegistry
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request as StarletteHTTPRequest

import logging
import sys

# from karapace.kafka_rest_apis import KafkaRest
# from karapace.utils import DebugAccessLogger


# class KarapaceAll(KafkaRest, KarapaceSchemaRegistryController):
#    pass


def _configure_logging(*, config: Config) -> None:
    log_handler = config.log_handler

    root_handler: logging.Handler | None = None
    if "systemd" == log_handler:
        from systemd import journal

        root_handler = journal.JournalHandler(SYSLOG_IDENTIFIER="karapace")
    elif "stdout" == log_handler or log_handler is None:
        root_handler = logging.StreamHandler(stream=sys.stdout)
    else:
        logging.basicConfig(level=config.log_level, format=config.log_format)
        logging.getLogger().setLevel(config.log_level)
        logging.warning("Log handler %s not recognized, root handler not set.", log_handler)

    if root_handler is not None:
        root_handler.setFormatter(logging.Formatter(config.log_format))
        root_handler.setLevel(config.log_level)
        root_handler.set_name(name="karapace")
        logging.root.addHandler(root_handler)

    logging.root.setLevel(config.log_level)
    logging.getLogger("uvicorn.error").setLevel(config.log_level)


#    if config.access_logs_debug is True:
#        config["access_log_class"] = DebugAccessLogger
#        logging.getLogger("aiohttp.access").setLevel(logging.DEBUG)
#    else:
#        config["access_log_class"] = AccessLogger

CONFIG = ConfigDependencyManager.get_config()
_configure_logging(config=CONFIG)

# config_without_secrets = {}
# for key, value in config.items():
#    if "password" in key:
#        value = "****"
#        config_without_secrets[key] = value
# logging.log(logging.DEBUG, "Config %r", config_without_secrets)
logging.log(logging.INFO, "Karapace version %s", karapace_version)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    schema_registry: KarapaceSchemaRegistry | None = None
    authorizer: AuthenticatorAndAuthorizer | None = None
    try:
        schema_registry = await SchemaRegistryDependencyManager.get_schema_registry()
        await schema_registry.start()
        await schema_registry.get_master()
        authorizer = AuthorizationDependencyManager.get_authorizer()
        # if authorizer is not None:
        #    await authorizer.start(StatsDependencyManager.get_stats())
        yield
    finally:
        if schema_registry:
            await schema_registry.close()
        if authorizer:
            await authorizer.close()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(_: StarletteHTTPRequest, exc: StarletteHTTPException):
    return JSONResponse(status_code=exc.status_code, content=exc.detail)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: StarletteHTTPRequest, exc: RequestValidationError):
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


@app.middleware("http")
async def set_content_types(request: Request, call_next):
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

    response = await call_next(request)
    response.headers["Content-Type"] = response_content_type
    return response


if CONFIG.karapace_registry:
    from karapace.routers.compatibility_router import compatibility_router
    from karapace.routers.config_router import config_router
    from karapace.routers.health_router import health_router
    from karapace.routers.master_available_router import master_availability_router
    from karapace.routers.mode_router import mode_router
    from karapace.routers.root_router import root_router
    from karapace.routers.schemas_router import schemas_router
    from karapace.routers.subjects_router import subjects_router

    app.include_router(compatibility_router)
    app.include_router(config_router)
    app.include_router(health_router)
    app.include_router(master_availability_router)
    app.include_router(mode_router)
    app.include_router(root_router)
    app.include_router(schemas_router)
    app.include_router(subjects_router)
if CONFIG.karapace_rest:
    # add rest router.
    pass


def __old_main() -> int:
    try:
        PrometheusInstrumentation.setup_metrics(app=app)
        app.run()  # `close` will be called by the callback `close_by_app` set by `KarapaceBase`
    except Exception as ex:  # pylint: disable-broad-except
        app.stats.unexpected_exception(ex=ex, where="karapace")
        raise
    return 0


# if __name__ == "__main__":
#    sys.exit(main())
