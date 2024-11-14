"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

# from aiohttp.web_log import AccessLogger
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from karapace import version as karapace_version
from karapace.config import Config
from karapace.instrumentation.prometheus import PrometheusInstrumentation
from karapace.schema_registry import KarapaceSchemaRegistry
from starlette.exceptions import HTTPException as StarletteHTTPException

import logging
import os
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
        logging.basicConfig(level=logging.INFO, format=config.log_format)
        logging.getLogger().setLevel(config.log_level)
        logging.warning("Log handler %s not recognized, root handler not set.", log_handler)

    if root_handler is not None:
        root_handler.setFormatter(logging.Formatter(config.log_format))
        root_handler.setLevel(config.log_level)
        root_handler.set_name(name="karapace")
        logging.root.addHandler(root_handler)

    logging.root.setLevel(config.log_level)


#    if config.access_logs_debug is True:
#        config["access_log_class"] = DebugAccessLogger
#        logging.getLogger("aiohttp.access").setLevel(logging.DEBUG)
#    else:
#        config["access_log_class"] = AccessLogger

env_file = os.environ.get("KARAPACE_DOTENV", None)
CONFIG = Config(_env_file=env_file, _env_file_encoding="utf-8")
_configure_logging(config=CONFIG)

# config_without_secrets = {}
# for key, value in config.items():
#    if "password" in key:
#        value = "****"
#        config_without_secrets[key] = value
# logging.log(logging.DEBUG, "Config %r", config_without_secrets)
logging.log(logging.INFO, "Karapace version %s", karapace_version)

SCHEMA_REGISTRY = KarapaceSchemaRegistry(config=CONFIG)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    try:
        await SCHEMA_REGISTRY.start()
        await SCHEMA_REGISTRY.get_master()
        yield
    finally:
        await SCHEMA_REGISTRY.close()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(status_code=exc.status_code, content=exc.detail)


if CONFIG.karapace_registry:
    from karapace.routers.compatibility_router import compatibility_router
    from karapace.routers.config_router import config_router
    from karapace.routers.health_router import health_router
    from karapace.routers.mode_router import mode_router
    from karapace.routers.schemas_router import schemas_router
    from karapace.routers.subjects_router import subjects_router

    app.include_router(compatibility_router)
    app.include_router(config_router)
    app.include_router(health_router)
    app.include_router(mode_router)
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
