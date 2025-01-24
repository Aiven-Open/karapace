"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import FastAPI
from karapace.api.routers.compatibility import compatibility_router
from karapace.api.routers.config import config_router
from karapace.api.routers.health import health_router
from karapace.api.routers.master_availability import master_availability_router
from karapace.api.routers.metrics import metrics_router
from karapace.api.routers.mode import mode_router
from karapace.api.routers.root import root_router
from karapace.api.routers.schemas import schemas_router
from karapace.api.routers.subjects import subjects_router


def setup_routers(app: FastAPI) -> None:
    app.include_router(compatibility_router)
    app.include_router(config_router)
    app.include_router(health_router)
    app.include_router(mode_router)
    app.include_router(root_router)
    app.include_router(schemas_router)
    app.include_router(subjects_router)
    app.include_router(metrics_router)
    app.include_router(master_availability_router)
