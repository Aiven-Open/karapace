"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, HTTPException, status
from karapace.schema_registry import KarapaceSchemaRegistry
from pydantic import BaseModel
from schema_registry.container import SchemaRegistryContainer


class HealthStatus(BaseModel):
    schema_registry_ready: bool
    schema_registry_startup_time_sec: float
    schema_registry_reader_current_offset: int
    schema_registry_reader_highest_offset: int
    schema_registry_is_primary: bool | None = None
    schema_registry_is_primary_eligible: bool
    schema_registry_primary_url: str | None = None
    schema_registry_coordinator_running: bool
    schema_registry_coordinator_generation_id: int


class HealthCheck(BaseModel):
    status: HealthStatus
    healthy: bool


health_router = APIRouter(
    prefix="/_health",
    tags=["health"],
    responses={404: {"description": "Not found"}},
)


@health_router.get("")
@inject
async def health(
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.karapace_container.schema_registry]),
) -> HealthCheck:
    starttime = 0.0
    if schema_registry.schema_reader.ready():
        starttime = schema_registry.schema_reader.last_check - schema_registry.schema_reader.start_time

    cs = schema_registry.mc.get_coordinator_status()

    health_status = HealthStatus(
        schema_registry_ready=schema_registry.schema_reader.ready(),
        schema_registry_startup_time_sec=starttime,
        schema_registry_reader_current_offset=schema_registry.schema_reader.offset,
        schema_registry_reader_highest_offset=schema_registry.schema_reader.highest_offset(),
        schema_registry_is_primary=cs.is_primary,
        schema_registry_is_primary_eligible=cs.is_primary_eligible,
        schema_registry_primary_url=cs.primary_url,
        schema_registry_coordinator_running=cs.is_running,
        schema_registry_coordinator_generation_id=cs.group_generation_id,
    )
    # if self._auth is not None:
    #    resp["schema_registry_authfile_timestamp"] = self._auth.authfile_last_modified

    if not await schema_registry.schema_reader.is_healthy():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    return HealthCheck(status=health_status, healthy=True)