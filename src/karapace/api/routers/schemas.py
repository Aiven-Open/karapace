"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, Query
from karapace.core.auth import AuthenticatorAndAuthorizer, User
from karapace.api.container import SchemaRegistryContainer
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.routers.requests import SchemaListingItem, SchemasResponse, SubjectVersion
from karapace.api.user import get_current_user
from typing import Annotated

schemas_router = APIRouter(
    prefix="/schemas",
    tags=["schemas"],
    responses={404: {"description": "Not found"}},
)


# TODO is this needed? Is this actually the ids/schema/id/schema??
@schemas_router.get("")
@inject
async def schemas_get_list(
    user: Annotated[User, Depends(get_current_user)],
    deleted: bool = False,
    latestOnly: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> list[SchemaListingItem]:
    return await controller.schemas_list(
        deleted=deleted,
        latest_only=latestOnly,
        user=user,
        authorizer=authorizer,
    )


@schemas_router.get("/ids/{schema_id}", response_model_exclude_none=True)
@inject
async def schemas_get(
    user: Annotated[User, Depends(get_current_user)],
    schema_id: str,  # TODO: type to actual type
    includeSubjects: bool = False,  # TODO: include subjects?
    fetchMaxId: bool = False,  # TODO: fetch max id?
    format_serialized: str = Query("", alias="format"),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> SchemasResponse:
    return await controller.schemas_get(
        schema_id=schema_id,
        include_subjects=includeSubjects,
        fetch_max_id=fetchMaxId,
        format_serialized=format_serialized,
        user=user,
        authorizer=authorizer,
    )


# @schemas_router.get("/ids/{schema_id}/schema")
# async def schemas_get_only_id(
#    controller: KarapaceSchemaRegistryControllerDep,
# ) -> SchemasResponse:
#    # TODO retrieve by id only schema
#    return await controller.schemas_get()


@schemas_router.get("/ids/{schema_id}/versions")
@inject
async def schemas_get_versions(
    user: Annotated[User, Depends(get_current_user)],
    schema_id: str,
    deleted: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> list[SubjectVersion]:
    return await controller.schemas_get_versions(
        schema_id=schema_id,
        deleted=deleted,
        user=user,
        authorizer=authorizer,
    )


@schemas_router.get("/types")
@inject
async def schemas_get_subjects_list(
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> list[str]:
    return await controller.schemas_types()
