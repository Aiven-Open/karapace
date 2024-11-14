"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter
from karapace.dependencies import KarapaceSchemaRegistryControllerDep
from karapace.routers.requests import SchemasResponse

schemas_router = APIRouter(
    prefix="/schemas",
    tags=["schemas"],
    responses={404: {"description": "Not found"}},
)


# TODO is this needed? Is this actually the ids/schema/id/schema??
@schemas_router.get("")
async def schemas_get_list(
    controller: KarapaceSchemaRegistryControllerDep,
    deleted: bool = False,
    latestOnly: bool = False,
):
    return await controller.schemas_list(deleted=deleted, latest_only=latestOnly)


@schemas_router.get("/ids/{schema_id}", response_model_exclude_none=True)
async def schemas_get(
    controller: KarapaceSchemaRegistryControllerDep,
    schema_id: str,  # TODO: type to actual type
    includeSubjects: bool = False,  # TODO: include subjects?
    fetchMaxId: bool = False,  # TODO: fetch max id?
    format: str = "",
) -> SchemasResponse:
    return await controller.schemas_get(
        schema_id=schema_id,
        include_subjects=includeSubjects,
        fetch_max_id=fetchMaxId,
        format_serialized=format,
    )


@schemas_router.get("/ids/{schema_id}/schema")
async def schemas_get_only_id(
    controller: KarapaceSchemaRegistryControllerDep,
):
    # TODO retrieve by id only schema
    return await controller.schemas_get()


@schemas_router.get("/ids/{schema_id}/versions")
async def schemas_get_versions(
    controller: KarapaceSchemaRegistryControllerDep,
    schema_id: str,
    deleted: bool = False,
):
    return await controller.schemas_get_versions(schema_id=schema_id, deleted=deleted)


@schemas_router.get("/ids/{schema_id}/types")
async def schemas_get_subjects_list(
    controller: KarapaceSchemaRegistryControllerDep,
):
    return await controller.schemas_types()
