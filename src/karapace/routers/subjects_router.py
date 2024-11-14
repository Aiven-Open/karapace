"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import APIRouter
from karapace.dependencies import KarapaceSchemaRegistryControllerDep
from karapace.routers.requests import SchemaIdResponse, SchemaRequest, SchemaResponse

subjects_router = APIRouter(
    prefix="/subjects",
    tags=["subjects"],
    responses={404: {"description": "Not found"}},
)


@subjects_router.get("")
async def subjects_get(
    controller: KarapaceSchemaRegistryControllerDep,
    deleted: bool = False,
):
    return await controller.subjects_list(deleted=deleted)


@subjects_router.post("/{subject}")
async def subjects_subject_post(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    schema_request: SchemaRequest,
    deleted: bool = False,
    normalize: bool = False,
) -> SchemaResponse:
    return await controller.subjects_schema_post(
        subject=subject, schema_request=schema_request, deleted=deleted, normalize=normalize
    )


@subjects_router.delete("/{subject}")
async def subjects_subject_delete(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    permanent: bool = False,
):
    return await controller.subject_delete(subject=subject, permanent=permanent)


@subjects_router.post("/{subject}/versions")
async def subjects_subject_versions_post(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    schema_request: SchemaRequest,
    normalize: bool = False,
) -> SchemaIdResponse:
    return await controller.subject_post(subject=subject, schema_request=schema_request, normalize=normalize)


@subjects_router.get("/{subject}/versions")
async def subjects_subject_versions_list(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    deleted: bool = False,
):
    return await controller.subject_versions_list(subject=subject, deleted=deleted)


@subjects_router.get("/{subject}/versions/{version}")
async def subjects_subject_version_get(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    version: str,
    deleted: bool = False,
):
    return await controller.subject_version_get(subject=subject, version=version, deleted=deleted)


@subjects_router.delete("/{subject}/versions/{version}")
async def subjects_subject_version_delete(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    version: str,
    permanent: bool = False,
):
    return await controller.subject_version_delete(subject=subject, version=version, permanent=permanent)


@subjects_router.get("/{subject}/versions/{version}/schema")
async def subjects_subject_version_schema_get(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    version: str,
):
    return await controller.subject_version_schema_get(subject=subject, version=version)


@subjects_router.get("/{subject}/versions/{version}/referencedby")
async def subjects_subject_version_referenced_by(
    controller: KarapaceSchemaRegistryControllerDep,
    subject: str,
    version: str,
):
    return await controller.subject_version_referencedby_get(subject=subject, version=version)
