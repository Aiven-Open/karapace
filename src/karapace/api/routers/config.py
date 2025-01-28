"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, Request
from karapace.core.auth import AuthenticatorAndAuthorizer, Operation, User
from karapace.api.forward_client import ForwardClient
from karapace.core.typing import Subject
from karapace.api.container import SchemaRegistryContainer
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.api.routers.errors import no_primary_url_error, unauthorized
from karapace.api.routers.raw_path_router import RawPathRoute
from karapace.api.routers.requests import CompatibilityLevelResponse, CompatibilityRequest, CompatibilityResponse
from karapace.api.user import get_current_user
from typing import Annotated
from urllib.parse import unquote_plus


config_router = APIRouter(
    prefix="/config",
    tags=["config"],
    responses={404: {"description": "Not found"}},
    route_class=RawPathRoute,
)


@config_router.get("")
@inject
async def config_get(
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> CompatibilityLevelResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, "Config:"):
        raise unauthorized()

    return await controller.config_get()


@config_router.put("")
@inject
async def config_put(
    request: Request,
    compatibility_level_request: CompatibilityRequest,
    user: Annotated[User, Depends(get_current_user)],
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> CompatibilityResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, "Config:"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.config_set(compatibility_level_request=compatibility_level_request)
    if not primary_info.primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(
        request=request, primary_url=primary_info.primary_url, response_type=CompatibilityResponse
    )


@config_router.get("/{subject}")
@inject
async def config_get_subject(
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    defaultToGlobal: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> CompatibilityLevelResponse:
    subject = Subject(unquote_plus(subject))
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.config_subject_get(subject=subject, default_to_global=defaultToGlobal)


@config_router.put("/{subject}")
@inject
async def config_set_subject(
    request: Request,
    subject: Subject,
    compatibility_level_request: CompatibilityRequest,
    user: Annotated[User, Depends(get_current_user)],
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> CompatibilityResponse:
    subject = Subject(unquote_plus(subject))
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.config_subject_set(subject=subject, compatibility_level_request=compatibility_level_request)
    if not primary_info.primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(
        request=request, primary_url=primary_info.primary_url, response_type=CompatibilityResponse
    )


@config_router.delete("/{subject}")
@inject
async def config_delete_subject(
    request: Request,
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> CompatibilityResponse:
    subject = Subject(unquote_plus(subject))
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.config_subject_delete(subject=subject)
    if not primary_info.primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(
        request=request, primary_url=primary_info.primary_url, response_type=CompatibilityResponse
    )
