"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, Request
from karapace.api.container import SchemaRegistryContainer
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.forward_client import ForwardClient
from karapace.api.routers.errors import no_primary_url_error, subject_not_found, unauthorized
from karapace.api.routers.raw_path_router import SchemaRegistryRoute
from karapace.api.routers.requests import ModeResponse, ModeUpdateRequest
from karapace.api.user import get_current_user
from karapace.core.auth import AuthenticatorAndAuthorizer, Operation, User
from karapace.core.auth_container import AuthContainer
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.typing import Subject
from typing import Annotated
from urllib.parse import unquote_plus

mode_router = APIRouter(
    prefix="/mode",
    tags=["mode"],
    responses={404: {"description": "Not found"}},
    route_class=SchemaRegistryRoute,
)


@mode_router.get("")
@inject
async def mode_get(
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> ModeResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, "Config:"):
        raise unauthorized()

    return await controller.get_global_mode()


@mode_router.put("")
@inject
async def mode_put(
    request: Request,
    mode_request: ModeUpdateRequest,
    user: Annotated[User, Depends(get_current_user)],
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> ModeResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, "Config:"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.set_global_mode(mode_request=mode_request)
    if not primary_info.primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(
        request=request, primary_url=primary_info.primary_url, response_type=ModeResponse
    )


@mode_router.get("/{subject}")
@inject
async def mode_get_subject(
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    defaultToGlobal: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> ModeResponse:
    subject = Subject(unquote_plus(subject))
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise subject_not_found(subject)

    return await controller.get_subject_mode(subject=subject, default_to_global=defaultToGlobal)


@mode_router.put("/{subject}")
@inject
async def mode_put_subject(
    request: Request,
    subject: Subject,
    mode_request: ModeUpdateRequest,
    user: Annotated[User, Depends(get_current_user)],
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
    force: bool = False,
) -> ModeResponse:
    subject = Subject(unquote_plus(subject))
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.set_subject_mode(subject=subject, mode_request=mode_request, force=force)
    if not primary_info.primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(
        request=request, primary_url=primary_info.primary_url, response_type=ModeResponse
    )


@mode_router.delete("/{subject}")
@inject
async def mode_delete_subject(
    request: Request,
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[AuthContainer.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> ModeResponse:
    subject = Subject(unquote_plus(subject))
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.delete_subject_mode(subject=subject)
    if not primary_info.primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(
        request=request, primary_url=primary_info.primary_url, response_type=ModeResponse
    )
