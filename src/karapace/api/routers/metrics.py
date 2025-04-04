"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, Response
from karapace.api.container import SchemaRegistryContainer
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation

metrics_router = APIRouter(
    prefix=PrometheusInstrumentation.METRICS_ENDPOINT_PATH,
    tags=["metrics"],
    responses={404: {"description": "Not found"}},
)


@metrics_router.get("")
@inject
async def metrics(
    prometheus: PrometheusInstrumentation = Depends(Provide[SchemaRegistryContainer.karapace_container.prometheus]),
) -> Response:
    return Response(content=await prometheus.serve_metrics(), media_type=prometheus.CONTENT_TYPE_LATEST)
