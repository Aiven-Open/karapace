"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter
from karapace.dependencies import KarapaceSchemaRegistryControllerDep
from karapace.routers.requests import CompatibilityCheckResponse, SchemaRequest

compatibility_router = APIRouter(
    prefix="/compatibility",
    tags=["compatibility"],
    responses={404: {"description": "Not found"}},
)


@compatibility_router.post("/subjects/{subject}/versions/{version}", response_model_exclude_none=True)
async def compatibility_post(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    version: str,  # TODO support actual Version object
    schema_request: SchemaRequest,
) -> CompatibilityCheckResponse:
    return await controller.compatibility_check(subject=subject, schema_request=schema_request, version=version)