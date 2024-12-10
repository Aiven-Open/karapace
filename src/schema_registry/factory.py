"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dependency_injector.wiring import inject, Provide
from fastapi import Depends, FastAPI
from karapace import version as karapace_version
from karapace.auth import AuthenticatorAndAuthorizer
from karapace.config import Config
from karapace.logging_setup import configure_logging, log_config_without_secrets
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.statsd import StatsClient
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from schema_registry.container import SchemaRegistryContainer
from schema_registry.http_handlers import setup_exception_handlers
from schema_registry.middlewares import setup_middlewares
from schema_registry.routers.setup import setup_routers
from typing import AsyncContextManager

import logging


@asynccontextmanager
@inject
async def karapace_schema_registry_lifespan(
    _: FastAPI,
    stastd: StatsClient = Depends(Provide[SchemaRegistryContainer.karapace_container.statsd]),
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.karapace_container.schema_registry]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
) -> AsyncGenerator[None, None]:
    try:
        await schema_registry.start()
        await schema_registry.get_master()
        await authorizer.start(stats=stastd)

        yield
    finally:
        if schema_registry:
            await schema_registry.close()
        if authorizer:
            await authorizer.close()
        if stastd:
            stastd.close()


def create_karapace_application(
    *,
    config: Config,
    lifespan: Callable[
        [FastAPI, StatsClient, KarapaceSchemaRegistry, AuthenticatorAndAuthorizer], AsyncContextManager[None]
    ],
) -> FastAPI:
    configure_logging(config=config)
    log_config_without_secrets(config=config)
    logging.info("Starting Karapace Schema Registry (%s)", karapace_version.__version__)

    app = FastAPI(lifespan=lifespan)  # type: ignore[arg-type]
    setup_routers(app=app)
    setup_exception_handlers(app=app)
    setup_middlewares(app=app)

    FastAPIInstrumentor.instrument_app(app)

    return app
