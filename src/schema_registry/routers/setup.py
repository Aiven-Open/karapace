"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import FastAPI
from schema_registry.routers.compatibility import compatibility_router
from schema_registry.routers.config import config_router
from schema_registry.routers.health import health_router
from schema_registry.routers.metrics import metrics_router
from schema_registry.routers.mode import mode_router
from schema_registry.routers.root import root_router
from schema_registry.routers.schemas import schemas_router
from schema_registry.routers.subjects import subjects_router


def setup_routers(app: FastAPI) -> None:
    app.include_router(compatibility_router)
    app.include_router(config_router)
    app.include_router(health_router)
    app.include_router(mode_router)
    app.include_router(root_router)
    app.include_router(schemas_router)
    app.include_router(subjects_router)
    app.include_router(metrics_router)
