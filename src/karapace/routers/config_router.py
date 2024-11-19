"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter, Request
from karapace.auth.auth import Operation
from karapace.auth.dependencies import AuthenticatorAndAuthorizerDep, CurrentUserDep
from karapace.dependencies.controller_dependency import KarapaceSchemaRegistryControllerDep
from karapace.dependencies.forward_client_dependency import ForwardClientDep
from karapace.dependencies.schema_registry_dependency import SchemaRegistryDep
from karapace.routers.errors import no_primary_url_error, unauthorized
from karapace.routers.requests import CompatibilityLevelResponse, CompatibilityRequest, CompatibilityResponse
from karapace.typing import Subject

config_router = APIRouter(
    prefix="/config",
    tags=["config"],
    responses={404: {"description": "Not found"}},
)


@config_router.get("")
async def config_get(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
) -> CompatibilityLevelResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, "Config:"):
        raise unauthorized()

    return await controller.config_get()


@config_router.put("")
async def config_put(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    schema_registry: SchemaRegistryDep,
    forward_client: ForwardClientDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    compatibility_level_request: CompatibilityRequest,
) -> CompatibilityResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, "Config:"):
        raise unauthorized()

    i_am_primary, primary_url = await schema_registry.get_master()
    if i_am_primary:
        return await controller.config_set(compatibility_level_request=compatibility_level_request)
    elif not primary_url:
        raise no_primary_url_error()
    else:
        return await forward_client.forward_request_remote(request=request, primary_url=primary_url)


@config_router.get("/{subject}")
async def config_get_subject(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    defaultToGlobal: bool = False,
) -> CompatibilityLevelResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.config_subject_get(subject=subject, default_to_global=defaultToGlobal)


@config_router.put("/{subject}")
async def config_set_subject(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    schema_registry: SchemaRegistryDep,
    forward_client: ForwardClientDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    compatibility_level_request: CompatibilityRequest,
) -> CompatibilityResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    i_am_primary, primary_url = await schema_registry.get_master()
    if i_am_primary:
        return await controller.config_subject_set(subject=subject, compatibility_level_request=compatibility_level_request)
    elif not primary_url:
        raise no_primary_url_error()
    else:
        return await forward_client.forward_request_remote(request=request, primary_url=primary_url)


@config_router.delete("/{subject}")
async def config_delete_subject(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    schema_registry: SchemaRegistryDep,
    forward_client: ForwardClientDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
) -> CompatibilityResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    i_am_primary, primary_url = await schema_registry.get_master()
    if i_am_primary:
        return await controller.config_subject_delete(subject=subject)
    elif not primary_url:
        raise no_primary_url_error()
    else:
        return await forward_client.forward_request_remote(request=request, primary_url=primary_url)
