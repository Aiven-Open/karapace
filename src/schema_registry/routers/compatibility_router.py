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
from schema_registry.routers.requests import CompatibilityCheckResponse, SchemaRequest

compatibility_router = APIRouter(
    prefix="/compatibility",
    tags=["compatibility"],
    responses={404: {"description": "Not found"}},
)


@compatibility_router.post("/subjects/{subject}/versions/{version}", response_model_exclude_none=True)
async def compatibility_post(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    version: str,  # TODO support actual Version object
    schema_request: SchemaRequest,
) -> CompatibilityCheckResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.compatibility_check(subject=subject, schema_request=schema_request, version=version)
