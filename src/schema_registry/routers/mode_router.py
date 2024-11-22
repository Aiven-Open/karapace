"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter
from karapace.auth.auth import Operation
from karapace.auth.dependencies import AuthenticatorAndAuthorizerDep, CurrentUserDep
from karapace.dependencies.controller_dependency import KarapaceSchemaRegistryControllerDep
from karapace.typing import Subject
from schema_registry.routers.errors import unauthorized

mode_router = APIRouter(
    prefix="/mode",
    tags=["mode"],
    responses={404: {"description": "Not found"}},
)


@mode_router.get("")
async def mode_get(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
):
    if authorizer and not authorizer.check_authorization(user, Operation.Read, "Config:"):
        raise unauthorized()

    return await controller.get_global_mode()


@mode_router.get("/{subject}")
async def mode_get_subject(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
):
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.get_subject_mode(subject=subject)
