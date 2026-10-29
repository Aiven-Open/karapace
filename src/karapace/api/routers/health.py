"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, HTTPException, status
from karapace import version as karapace_version
from karapace.api.container import SchemaRegistryContainer
from karapace.core.instrumentation.tracer import Tracer
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
    karapace_version: str
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
        health_check = await schema_registry.health_monitor.check()
        health_status = HealthStatus.model_validate(health_check.status, from_attributes=True)
        set_health_status_tracing_attributes(health_check_span=health_check_span, health_status=health_status)

        # if self._auth is not None:
        #    resp["schema_registry_authfile_timestamp"] = self._auth.authfile_last_modified

        if not health_check.healthy:
            health_check_span.set_status(status=StatusCode.ERROR, description="Schema reader is not healthy")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

        return HealthCheck(karapace_version=karapace_version.__version__, status=health_status, healthy=True)
