"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, Depends, Request
from karapace.auth import AuthenticatorAndAuthorizer, Operation, User
from karapace.forward_client import ForwardClient
from karapace.typing import Subject
from schema_registry.container import SchemaRegistryContainer
from schema_registry.controller import KarapaceSchemaRegistryController
from schema_registry.registry import KarapaceSchemaRegistry
from schema_registry.routers.errors import no_primary_url_error, unauthorized
from schema_registry.routers.requests import SchemaIdResponse, SchemaRequest, SchemaResponse, SubjectSchemaVersionResponse
from schema_registry.user import get_current_user
from typing import Annotated

import logging

LOG = logging.getLogger(__name__)


subjects_router = APIRouter(
    prefix="/subjects",
    tags=["subjects"],
    responses={404: {"description": "Not found"}},
)


@subjects_router.get("")
@inject
async def subjects_get(
    user: Annotated[User, Depends(get_current_user)],
    deleted: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> list[str]:
    return await controller.subjects_list(
        deleted=deleted,
        user=user,
        authorizer=authorizer,
    )


@subjects_router.post("/{subject}", response_model_exclude_none=True)
@inject
async def subjects_subject_post(
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    schema_request: SchemaRequest,
    deleted: bool = False,
    normalize: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
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
@inject
async def subjects_subject_delete(
    request: Request,
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    permanent: bool = False,
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> list[int]:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    i_am_primary, primary_url = await schema_registry.get_master()
    if i_am_primary:
        return await controller.subject_delete(subject=subject, permanent=permanent)
    if not primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(request=request, primary_url=primary_url, response_type=list[int])


@subjects_router.post("/{subject}/versions")
@inject
async def subjects_subject_versions_post(
    request: Request,
    subject: Subject,
    schema_request: SchemaRequest,
    user: Annotated[User, Depends(get_current_user)],
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    normalize: bool = False,
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
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
@inject
async def subjects_subject_versions_list(
    subject: Subject,
    user: Annotated[User, Depends(get_current_user)],
    deleted: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> list[int]:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_versions_list(subject=subject, deleted=deleted)


@subjects_router.get("/{subject}/versions/{version}", response_model_exclude_none=True)
@inject
async def subjects_subject_version_get(
    subject: Subject,
    version: str,
    user: Annotated[User, Depends(get_current_user)],
    deleted: bool = False,
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> SubjectSchemaVersionResponse:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_version_get(subject=subject, version=version, deleted=deleted)


@subjects_router.delete("/{subject}/versions/{version}")
@inject
async def subjects_subject_version_delete(
    request: Request,
    subject: Subject,
    version: str,
    user: Annotated[User, Depends(get_current_user)],
    permanent: bool = False,
    forward_client: ForwardClient = Depends(Provide[SchemaRegistryContainer.karapace_container.forward_client]),
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    schema_registry: KarapaceSchemaRegistry = Depends(Provide[SchemaRegistryContainer.schema_registry]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> int:
    if authorizer and not authorizer.check_authorization(user, Operation.Write, f"Subject:{subject}"):
        raise unauthorized()

    i_am_primary, primary_url = await schema_registry.get_master()
    if i_am_primary:
        return await controller.subject_version_delete(subject=subject, version=version, permanent=permanent)
    if not primary_url:
        raise no_primary_url_error()
    return await forward_client.forward_request_remote(request=request, primary_url=primary_url, response_type=int)


@subjects_router.get("/{subject}/versions/{version}/schema")
@inject
async def subjects_subject_version_schema_get(
    subject: Subject,
    version: str,
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> dict:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_version_schema_get(subject=subject, version=version)


@subjects_router.get("/{subject}/versions/{version}/referencedby")
@inject
async def subjects_subject_version_referenced_by(
    subject: Subject,
    version: str,
    user: Annotated[User, Depends(get_current_user)],
    authorizer: AuthenticatorAndAuthorizer = Depends(Provide[SchemaRegistryContainer.karapace_container.authorizer]),
    controller: KarapaceSchemaRegistryController = Depends(Provide[SchemaRegistryContainer.schema_registry_controller]),
) -> list[int]:
    if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
        raise unauthorized()

    return await controller.subject_version_referencedby_get(subject=subject, version=version)
