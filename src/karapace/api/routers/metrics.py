"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, Response
from karapace.api.container import SchemaRegistryContainer
from karapace.core.instrumentation.prometheus import PrometheusInstrumentation
from karapace.rapu import HTTPResponse

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
    try:
        await prometheus.serve_metrics()
    except HTTPResponse as ex:
        # FastAPI needs to extract the body and return a Response
        # generate_latest() returns bytes, so ex.body is already bytes
        content = ex.body if isinstance(ex.body, bytes) else ex.body.encode("utf-8")
        return Response(content=content, media_type=prometheus.CONTENT_TYPE_LATEST)
