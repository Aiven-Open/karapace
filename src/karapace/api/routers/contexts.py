"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends
from karapace.api.container import SchemaRegistryContainer
from karapace.api.routers.raw_path_router import SchemaRegistryRoute
from karapace.api.user import get_current_user
from karapace.core.auth import User
from karapace.core.schema_registry import KarapaceSchemaRegistry
from typing import Annotated

contexts_router = APIRouter(
    prefix="/contexts",
    tags=["contexts"],
    responses={404: {"description": "Not found"}},
    route_class=SchemaRegistryRoute,
)


@contexts_router.get("")
@inject
async def contexts_get(
    user: Annotated[User, Depends(get_current_user)],
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
) -> list[str]:
    return schema_registry.database.contexts()
