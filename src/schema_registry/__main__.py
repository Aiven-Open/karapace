"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from fastapi import FastAPI
from karapace import version as karapace_version
from karapace.auth.auth import AuthenticatorAndAuthorizer
from karapace.auth.dependencies import AuthorizationDependencyManager
from karapace.config import Config
from karapace.dependencies.config_dependency import ConfigDependencyManager
from karapace.dependencies.schema_registry_dependency import SchemaRegistryDependencyManager
from karapace.dependencies.stats_dependeny import StatsDependencyManager
from karapace.logging import configure_logging, log_config_without_secrets
from karapace.schema_registry import KarapaceSchemaRegistry
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from schema_registry.http_handlers import setup_exception_handlers
from schema_registry.middlewares import setup_middlewares
from schema_registry.routers import setup_routers
from typing import Final

import logging
import uvicorn


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    schema_registry: KarapaceSchemaRegistry | None = None
    authorizer: AuthenticatorAndAuthorizer | None = None
    try:
        schema_registry = await SchemaRegistryDependencyManager.get_schema_registry()
        await schema_registry.start()
        await schema_registry.get_master()
        authorizer = AuthorizationDependencyManager.get_authorizer()
        if authorizer is not None:
            await authorizer.start(StatsDependencyManager.get_stats())
        yield
    finally:
        if schema_registry:
            await schema_registry.close()
        if authorizer:
            await authorizer.close()


def create_karapace_application(*, config: Config) -> FastAPI:
    configure_logging(config=config)
    log_config_without_secrets(config=config)
    logging.info("Starting Karapace Schema Registry (%s)", karapace_version.__version__)

    app = FastAPI(lifespan=lifespan)
    setup_routers(app=app)
    setup_exception_handlers(app=app)
    setup_middlewares(app=app)

    FastAPIInstrumentor.instrument_app(app)

    return app


CONFIG: Final = ConfigDependencyManager.get_config()

if __name__ == "__main__":
    app = create_karapace_application(config=CONFIG)
    uvicorn.run(app, host=CONFIG.host, port=CONFIG.port, log_level=CONFIG.log_level.lower())
