"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter, Request
from karapace.dependencies import ForwardClientDep, KarapaceSchemaRegistryControllerDep, SchemaRegistryDep
from karapace.routers.errors import no_primary_url_error
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
) -> CompatibilityLevelResponse:
    return await controller.config_get()


@config_router.put("")
async def config_put(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    schema_registry: SchemaRegistryDep,
    forward_client: ForwardClientDep,
    compatibility_level_request: CompatibilityRequest,
) -> CompatibilityResponse:
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
    subject: Subject,
    defaultToGlobal: bool = False,
) -> CompatibilityLevelResponse:
    return await controller.config_subject_get(subject=subject, default_to_global=defaultToGlobal)


@config_router.put("/{subject}")
async def config_set_subject(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    schema_registry: SchemaRegistryDep,
    forward_client: ForwardClientDep,
    subject: Subject,
    compatibility_level_request: CompatibilityRequest,
) -> CompatibilityResponse:
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
    subject: Subject,
) -> CompatibilityResponse:
    i_am_primary, primary_url = await schema_registry.get_master()
    if i_am_primary:
        return await controller.config_subject_delete(subject=subject)
    elif not primary_url:
        raise no_primary_url_error()
    else:
        return await forward_client.forward_request_remote(request=request, primary_url=primary_url)
