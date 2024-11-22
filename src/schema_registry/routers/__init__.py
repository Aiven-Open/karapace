"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import FastAPI
from schema_registry.routers.compatibility_router import compatibility_router
from schema_registry.routers.config_router import config_router
from schema_registry.routers.health_router import health_router
from schema_registry.routers.mode_router import mode_router
from schema_registry.routers.root_router import root_router
from schema_registry.routers.schemas_router import schemas_router
from schema_registry.routers.subjects_router import subjects_router


def setup_routers(app: FastAPI) -> None:
    app.include_router(compatibility_router)
    app.include_router(config_router)
    app.include_router(health_router)
    app.include_router(mode_router)
    app.include_router(root_router)
    app.include_router(schemas_router)
    app.include_router(subjects_router)
