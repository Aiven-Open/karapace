"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, HTTPException, status
from karapace.api.container import SchemaRegistryContainer
from karapace.api.telemetry.tracer import Tracer
from karapace.core.schema_registry import KarapaceSchemaRegistry
from opentelemetry.trace import Span
from opentelemetry.trace.status import StatusCode
from pydantic import BaseModel


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


def set_health_status_tracing_attributes(health_check_span: Span, health_status: HealthStatus) -> None:
    health_check_span.set_attribute("schema_registry_ready", health_status.schema_registry_ready)
    health_check_span.set_attribute("schema_registry_startup_time_sec", health_status.schema_registry_startup_time_sec)
    health_check_span.set_attribute(
        "schema_registry_reader_current_offset", health_status.schema_registry_reader_current_offset
    )
    health_check_span.set_attribute(
        "schema_registry_reader_highest_offset", health_status.schema_registry_reader_highest_offset
    )
    health_check_span.set_attribute("schema_registry_is_primary", getattr(health_status, "schema_registry_is_primary", ""))


@health_router.get("")
@inject
async def health(
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    tracer: Tracer = Depends(Provide[SchemaRegistryContainer.telemetry_container.tracer]),
) -> HealthCheck:
    with tracer.get_tracer().start_as_current_span("APIRouter: health_check") as health_check_span:
        starttime = 0.0

        schema_reader_is_ready = schema_registry.schema_reader.ready()
        if schema_reader_is_ready:
            starttime = schema_registry.schema_reader.last_check - schema_registry.schema_reader.start_time

        cs = schema_registry.mc.get_coordinator_status()
        health_status = HealthStatus(
            schema_registry_ready=schema_reader_is_ready,
            schema_registry_startup_time_sec=starttime,
            schema_registry_reader_current_offset=schema_registry.schema_reader.offset,
            schema_registry_reader_highest_offset=schema_registry.schema_reader.highest_offset(),
            schema_registry_is_primary=cs.is_primary,
            schema_registry_is_primary_eligible=cs.is_primary_eligible,
            schema_registry_primary_url=cs.primary_url,
            schema_registry_coordinator_running=cs.is_running,
            schema_registry_coordinator_generation_id=cs.group_generation_id,
        )
        set_health_status_tracing_attributes(health_check_span=health_check_span, health_status=health_status)

        # if self._auth is not None:
        #    resp["schema_registry_authfile_timestamp"] = self._auth.authfile_last_modified

        if not await schema_registry.schema_reader.is_healthy():
            health_check_span.set_status(status=StatusCode.ERROR, description="Schema reader is not healthy")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

        return HealthCheck(status=health_status, healthy=True)
