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
from schema_registry.schema_registry_apis import KarapaceSchemaRegistryController
from schema_registry.user import get_current_user
from typing import Annotated

mode_router = APIRouter(
    prefix="/mode",
    tags=["mode"],
    responses={404: {"description": "Not found"}},
)


@mode_router.get("")
@inject
async def mode_get(
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
):
    if authorizer and not authorizer.check_authorization(user, Operation.Read, "Config:"):
        raise unauthorized()

    return await controller.get_global_mode()


@mode_router.get("/{subject}")
@inject
async def mode_get_subject(
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
):
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.get_subject_mode(subject=subject)
