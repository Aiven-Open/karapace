"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter
from karapace.dependencies import SchemaRegistryDep
from karapace.typing import JsonObject
from pydantic import BaseModel


class HealthCheck(BaseModel):
    status: JsonObject
    healthy: bool


health_router = APIRouter(
    prefix="/health",
    tags=["health"],
    responses={404: {"description": "Not found"}},
)


@health_router.get("")
async def health(
    schema_registry: SchemaRegistryDep,
) -> HealthCheck:
    resp = {}
    # if self._auth is not None:
    #    resp["schema_registry_authfile_timestamp"] = self._auth.authfile_last_modified
    resp["schema_registry_ready"] = schema_registry.schema_reader.ready
    if schema_registry.schema_reader.ready:
        resp["schema_registry_startup_time_sec"] = (
            schema_registry.schema_reader.last_check - schema_registry.schema_reader.start_time
        )
    resp["schema_registry_reader_current_offset"] = schema_registry.schema_reader.offset
    resp["schema_registry_reader_highest_offset"] = schema_registry.schema_reader.highest_offset()
    cs = schema_registry.mc.get_coordinator_status()
    resp["schema_registry_is_primary"] = cs.is_primary
    resp["schema_registry_is_primary_eligible"] = cs.is_primary_eligible
    resp["schema_registry_primary_url"] = cs.primary_url
    resp["schema_registry_coordinator_running"] = cs.is_running
    resp["schema_registry_coordinator_generation_id"] = cs.group_generation_id

    healthy = True
    if not await schema_registry.schema_reader.is_healthy():
        healthy = False

    return HealthCheck(status=resp, healthy=healthy)
