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
from karapace.routers.requests import SchemaIdResponse, SchemaRequest, SchemaResponse, SubjectSchemaVersionResponse
from karapace.typing import Subject

import logging

LOG = logging.getLogger(__name__)


subjects_router = APIRouter(
    prefix="/subjects",
    tags=["subjects"],
    responses={404: {"description": "Not found"}},
)


@subjects_router.get("")
async def subjects_get(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    deleted: bool = False,
) -> list[str]:
    return await controller.subjects_list(
        deleted=deleted,
        user=user,
        authorizer=authorizer,
    )


@subjects_router.post("/{subject}", response_model_exclude_none=True)
async def subjects_subject_post(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    schema_request: SchemaRequest,
    deleted: bool = False,
    normalize: bool = False,
) -> SchemaResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subjects_schema_post(
        subject=subject,
        schema_request=schema_request,
        deleted=deleted,
        normalize=normalize,
    )


@subjects_router.delete("/{subject}")
async def subjects_subject_delete(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    schema_registry: SchemaRegistryDep,
    forward_client: ForwardClientDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    permanent: bool = False,
) -> list[int]:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.subject_delete(subject=subject, permanent=permanent)
    elif not primary_info.primary_url:
        raise no_primary_url_error()
    else:
        return await forward_client.forward_request_remote(request=request, primary_url=primary_url)


@subjects_router.post("/{subject}/versions")
async def subjects_subject_versions_post(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    forward_client: ForwardClientDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    schema_request: SchemaRequest,
    normalize: bool = False,
) -> SchemaIdResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    # TODO: split the functionality so primary error and forwarding can be handled here
    # and local/primary write is in controller.
    return await controller.subject_post(
        subject=subject,
        schema_request=schema_request,
        normalize=normalize,
        forward_client=forward_client,
        request=request,
    )


@subjects_router.get("/{subject}/versions")
async def subjects_subject_versions_list(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    deleted: bool = False,
) -> list[int]:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_versions_list(subject=subject, deleted=deleted)


@subjects_router.get("/{subject}/versions/{version}", response_model_exclude_none=True)
async def subjects_subject_version_get(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    version: str,
    deleted: bool = False,
) -> SubjectSchemaVersionResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_version_get(subject=subject, version=version, deleted=deleted)


@subjects_router.delete("/{subject}/versions/{version}")
async def subjects_subject_version_delete(
    request: Request,
    controller: KarapaceSchemaRegistryControllerDep,
    schema_registry: SchemaRegistryDep,
    forward_client: ForwardClientDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    version: str,
    permanent: bool = False,
) -> int:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    primary_info = await schema_registry.get_master()
    if primary_info.primary:
        return await controller.subject_version_delete(subject=subject, version=version, permanent=permanent)
    elif not primary_info.primary_url:
        raise no_primary_url_error()
    else:
        return await forward_client.forward_request_remote(request=request, primary_url=primary_url)


@subjects_router.get("/{subject}/versions/{version}/schema")
async def subjects_subject_version_schema_get(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    version: str,
) -> dict:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_version_schema_get(subject=subject, version=version)


@subjects_router.get("/{subject}/versions/{version}/referencedby")
async def subjects_subject_version_referenced_by(
    controller: KarapaceSchemaRegistryControllerDep,
    user: CurrentUserDep,
    authorizer: AuthenticatorAndAuthorizerDep,
    subject: Subject,
    version: str,
) -> list[int]:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_version_referencedby_get(subject=subject, version=version)
