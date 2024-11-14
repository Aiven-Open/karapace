"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter
from karapace.dependencies import KarapaceSchemaRegistryControllerDep
from karapace.routers.requests import CompatibilityRequest

config_router = APIRouter(
    prefix="/config",
    tags=["config"],
    responses={404: {"description": "Not found"}},
)


@config_router.get("")
async def config_get(
    controller: KarapaceSchemaRegistryControllerDep,
):
    return await controller.config_get()


@config_router.put("")
async def config_put(controller: KarapaceSchemaRegistryControllerDep, compatibility_level_request: CompatibilityRequest):
    return await controller.config_set(compatibility_level_request=compatibility_level_request)


@config_router.get("/{subject}")
async def config_get_subject(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    defaultToGlobal: bool = False,
):
    return await controller.config_subject_get(subject=subject, default_to_global=defaultToGlobal)


@config_router.put("/{subject}")
async def config_set_subject(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    compatibility_level_request: CompatibilityRequest,
):
    return await controller.config_subject_set(subject=subject, compatibility_level_request=compatibility_level_request)


@config_router.delete("/{subject}")
async def config_delete_subject(controller: KarapaceSchemaRegistryControllerDep, subject: str):
    return await controller.config_subject_delete(subject=subject)
