"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter
from karapace.dependencies import KarapaceSchemaRegistryControllerDep
from karapace.typing import Subject

mode_router = APIRouter(
    prefix="/mode",
    tags=["mode"],
    responses={404: {"description": "Not found"}},
)


@mode_router.get("")
async def mode_get(
    controller: KarapaceSchemaRegistryControllerDep,
):
    return await controller.get_global_mode()


@mode_router.get("/{subject}")
async def mode_get_subject(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: Subject,
):
    return await controller.get_subject_mode(subject=subject)
