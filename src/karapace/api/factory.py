"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dependency_injector.wiring import inject, Provide
from fastapi import Depends, FastAPI
from karapace import version as karapace_version
from karapace.api.container import SchemaRegistryContainer
from karapace.api.forward_client import ForwardClient
from karapace.api.http_handlers import setup_exception_handlers
from karapace.api.middlewares import setup_middlewares
from karapace.api.routers.setup import setup_routers
from karapace.api.telemetry.setup import setup_metering, setup_tracing
from karapace.core.auth import AuthenticatorAndAuthorizer
from karapace.core.auth_container import AuthContainer
from karapace.core.config import Config
from karapace.core.logging_setup import configure_logging, log_config_without_secrets
from karapace.core.metrics_container import MetricsContainer
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.stats import StatsClient
from typing import AsyncContextManager

import logging


@asynccontextmanager
@inject
async def karapace_schema_registry_lifespan(
    _: FastAPI,
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    stats: StatsClient = Depends(Provide[MetricsContainer.stats]),
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
) -> AsyncGenerator[None, None]:
    try:
        await schema_registry.start()
        await authorizer.start(stats=stats)

        yield
    finally:
        await schema_registry.close()
        await authorizer.close()
        await forward_client.close()
        stats.close()


def create_karapace_application(
    *,
    config: Config,
    lifespan: Callable[
        [FastAPI, ForwardClient, StatsClient, KarapaceSchemaRegistry, AuthenticatorAndAuthorizer], AsyncContextManager[None]
    ],
) -> FastAPI:
    configure_logging(config=config)
    log_config_without_secrets(config=config)
    logging.info("Starting Karapace Schema Registry (%s)", karapace_version.__version__)

    app = FastAPI(lifespan=lifespan)  # type: ignore[arg-type]

    setup_tracing()
    setup_metering()
    setup_routers(app=app)
    setup_exception_handlers(app=app)
    setup_middlewares(app=app, config=config)

    return app
