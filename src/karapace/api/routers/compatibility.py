"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends
from karapace.api.container import SchemaRegistryContainer
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.routers.errors import unauthorized
from karapace.api.routers.raw_path_router import RawPathRoute
from karapace.api.routers.requests import CompatibilityCheckResponse, SchemaRequest
from karapace.api.user import get_current_user
from karapace.core.auth import AuthenticatorAndAuthorizer, Operation, User
from karapace.core.auth_container import AuthContainer
from karapace.core.typing import Subject
from typing import Annotated
from urllib.parse import unquote_plus

compatibility_router = APIRouter(
    prefix="/compatibility",
    tags=["compatibility"],
    responses={404: {"description": "Not found"}},
    route_class=RawPathRoute,
)


@compatibility_router.post("/subjects/{subject}/versions/{version}", response_model_exclude_none=True)
@inject
async def compatibility_post(
    subject: Subject,
    version: str,  # TODO support actual Version object
    schema_request: SchemaRequest,
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> CompatibilityCheckResponse:
    subject = Subject(unquote_plus(subject))
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.compatibility_check(subject=subject, schema_request=schema_request, version=version)
