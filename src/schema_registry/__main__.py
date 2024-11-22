"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from karapace import version as karapace_version
from karapace.auth.auth import AuthenticatorAndAuthorizer
from karapace.auth.dependencies import AuthorizationDependencyManager
from karapace.config import Config
from schema_registry.dependencies.schema_registry_dependency import SchemaRegistryDependencyManager
from schema_registry.dependencies.stats_dependeny import StatsDependencyManager
from karapace.logging import configure_logging, log_config_without_secrets
from karapace.schema_registry import KarapaceSchemaRegistry
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from schema_registry.http_handlers import setup_exception_handlers
from schema_registry.middlewares import setup_middlewares
from schema_registry.routers import setup_routers
from typing import Final
from schema_registry.container import SchemaRegistryContainer
from dependency_injector.wiring import Provide, inject

import logging
import uvicorn
from pathlib import Path

SCHEMA_REGISTRY_ROOT: Final[Path] = Path(__file__).parent


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


@inject
def create_karapace_application(*, config: Config = Depends(Provide[SchemaRegistryContainer.config])) -> FastAPI:
    configure_logging(config=config)
    log_config_without_secrets(config=config)
    logging.info("Starting Karapace Schema Registry (%s)", karapace_version.__version__)

    app = FastAPI(lifespan=lifespan)
    setup_routers(app=app)
    setup_exception_handlers(app=app)
    setup_middlewares(app=app)

    FastAPIInstrumentor.instrument_app(app)

    return app


if __name__ == "__main__":
    container = SchemaRegistryContainer()
    container.base_config.from_yaml(f"{SCHEMA_REGISTRY_ROOT / 'base_config.yaml'}", envs_required=True, required=True)
    container.wire(modules=[__name__,])

    app = create_karapace_application()
    uvicorn.run(
        app,
        host=container.config().host,
        port=container.config().port,
        log_level=container.config().log_level.lower()
    )
