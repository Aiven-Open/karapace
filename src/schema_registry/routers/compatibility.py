"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends
from karapace.auth import AuthenticatorAndAuthorizer, Operation, User
from karapace.typing import Subject
from schema_registry.container import SchemaRegistryContainer
from schema_registry.routers.errors import unauthorized
from schema_registry.routers.requests import CompatibilityCheckResponse, SchemaRequest
from schema_registry.schema_registry_apis import KarapaceSchemaRegistryController
from schema_registry.user import get_current_user
from typing import Annotated

compatibility_router = APIRouter(
    prefix="/compatibility",
    tags=["compatibility"],
    responses={404: {"description": "Not found"}},
)


@compatibility_router.post("/subjects/{subject}/versions/{version}", response_model_exclude_none=True)
@inject
async def compatibility_post(
    subject: Subject,
    version: str,  # TODO support actual Version object
    schema_request: SchemaRequest,
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> CompatibilityCheckResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.compatibility_check(subject=subject, schema_request=schema_request, version=version)
