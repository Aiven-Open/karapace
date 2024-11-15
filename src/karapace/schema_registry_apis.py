"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from avro.errors import SchemaParseException
from contextlib import AsyncExitStack
from enum import Enum, unique
from fastapi import HTTPException, status
from http import HTTPStatus
from karapace.auth import HTTPAuthorizer, Operation, User
from karapace.compatibility import CompatibilityModes
from karapace.compatibility.jsonschema.checks import is_incompatible
from karapace.compatibility.schema_compatibility import SchemaCompatibility
from karapace.config import Config
from karapace.errors import (
    IncompatibleSchema,
    InvalidReferences,
    InvalidSchema,
    InvalidSchemaType,
    InvalidVersion,
    ReferenceExistsException,
    SchemasNotFoundException,
    SchemaTooLargeException,
    SchemaVersionNotSoftDeletedException,
    SchemaVersionSoftDeletedException,
    SubjectNotFoundException,
    SubjectNotSoftDeletedException,
    SubjectSoftDeletedException,
    VersionNotFoundException,
)
from karapace.protobuf.exception import ProtobufUnresolvedDependencyException
from karapace.rapu import HTTPRequest, HTTPResponse, JSON_CONTENT_TYPE
from karapace.routers.requests import (
    CompatibilityCheckResponse,
    CompatibilityLevelResponse,
    CompatibilityRequest,
    CompatibilityResponse,
    ModeResponse,
    SchemaIdResponse,
    SchemaListingItem,
    SchemaRequest,
    SchemaResponse,
    SchemasResponse,
    SubjectSchemaVersionResponse,
    SubjectVersion,
)
from karapace.schema_models import ParsedTypedSchema, SchemaType, SchemaVersion, TypedSchema, ValidatedTypedSchema, Versioner
from karapace.schema_references import LatestVersionReference, Reference, reference_from_mapping
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.statsd import StatsClient
from karapace.typing import JsonData, JsonObject, SchemaId, Subject, Version
from karapace.utils import JSONDecodeError
from typing import Any, cast

import async_timeout
import json
import logging
import time

LOG = logging.getLogger(__name__)


@unique
class SchemaErrorCodes(Enum):
    HTTP_BAD_REQUEST = HTTPStatus.BAD_REQUEST.value
    HTTP_NOT_FOUND = HTTPStatus.NOT_FOUND.value
    HTTP_CONFLICT = HTTPStatus.CONFLICT.value
    HTTP_UNPROCESSABLE_ENTITY = HTTPStatus.UNPROCESSABLE_ENTITY.value
    HTTP_INTERNAL_SERVER_ERROR = HTTPStatus.INTERNAL_SERVER_ERROR.value
    SUBJECT_NOT_FOUND = 40401
    VERSION_NOT_FOUND = 40402
    SCHEMA_NOT_FOUND = 40403
    SUBJECT_SOFT_DELETED = 40404
    SUBJECT_NOT_SOFT_DELETED = 40405
    SCHEMAVERSION_SOFT_DELETED = 40406
    SCHEMAVERSION_NOT_SOFT_DELETED = 40407
    SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE = 40408
    INVALID_VERSION_ID = 42202
    INVALID_COMPATIBILITY_LEVEL = 42203
    INVALID_SCHEMA = 42201
    INVALID_SUBJECT = 42208
    SCHEMA_TOO_LARGE_ERROR_CODE = 42209
    REFERENCES_SUPPORT_NOT_IMPLEMENTED = 44302
    REFERENCE_EXISTS = 42206
    NO_MASTER_ERROR = 50003


@unique
class SchemaErrorMessages(Enum):
    SUBJECT_NOT_FOUND_FMT = "Subject '{subject}' not found."
    INVALID_COMPATIBILITY_LEVEL = (
        "Invalid compatibility level. Valid values are none, backward, "
        "forward, full, backward_transitive, forward_transitive, and "
        "full_transitive"
    )
    SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_FMT = (
        "Subject '{subject}' does not have subject-level compatibility configured"
    )
    REFERENCES_SUPPORT_NOT_IMPLEMENTED = "Schema references are not supported for '{schema_type}' schema type"


def _validate_subject(subject: str) -> str:
    """Subject may not contain control characters."""
    if bool([c for c in subject if (ord(c) <= 31 or (ord(c) >= 127 and ord(c) <= 159))]):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error_code": SchemaErrorCodes.INVALID_SUBJECT.value,
                "message": f"The specified subject '{subject}' is not a valid.",
            },
        )
    return subject


# def _validate_schema_key(schema_request: SchemaRequest) -> None:
#    if "schema" not in body:
#        self.r(
#            body={
#                "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
#                "message": "Empty schema",
#            },
#            content_type=content_type,
#            status=HTTPStatus.UNPROCESSABLE_ENTITY,
#        )


class KarapaceSchemaRegistryController:
    def __init__(self, config: Config, schema_registry: KarapaceSchemaRegistry, stats: StatsClient) -> None:
        # super().__init__(config=config, not_ready_handler=self._forward_if_not_ready_to_serve)
        # TODO fix
        global LOG
        self.log = LOG

        self.config = config
        self._process_start_time = time.monotonic()
        self.stats = stats

        self._auth: HTTPAuthorizer | None = None
        if self.config.registry_authfile is not None:
            self._auth = HTTPAuthorizer(str(self.config.registry_authfile))
            # self.app.on_startup.append(self._start_authorizer)

        self.schema_registry = schema_registry
        # self.schema_registry = KarapaceSchemaRegistry(config)
        # self._add_schema_registry_routes()

        self._forward_client = None
        # self.app.on_startup.append(self._start_schema_registry)
        # self.app.on_startup.append(self._create_forward_client)
        # self.health_hooks.append(self.schema_registry_health)

    #    async def _start_schema_registry(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
    #        """Callback for aiohttp.Application.on_startup"""
    #        await self.schema_registry.start()

    #    async def _create_forward_client(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
    #        """Callback for aiohttp.Application.on_startup"""
    #        self._forward_client = aiohttp.ClientSession(headers={"User-Agent": SERVER_NAME})

    #    async def _start_authorizer(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
    #        """Callback for aiohttp.Application.on_startup"""
    #        await self._auth.start_refresh_task(self.stats)

    def _check_authorization(self, user: User | None, operation: Operation, resource: str) -> None:
        if self._auth:
            if not self._auth.check_authorization(user, operation, resource):
                self.r(body={"message": "Forbidden"}, content_type=JSON_CONTENT_TYPE, status=HTTPStatus.FORBIDDEN)

    async def _forward_if_not_ready_to_serve(self, request: HTTPRequest, content_type: str | None = None) -> None:
        if self.schema_registry.schema_reader.ready():
            pass
        else:
            # Not ready, still loading the state.
            primary_info = await self.schema_registry.get_master()
            returned_content_type = request.get_header("Content-Type") if content_type is None else content_type
            if not primary_info.primary_url:
                self.no_master_error(request.content_type)
            else:
                url = f"{primary_info.primary_url}{request.url.path}"
                await self._forward_request_remote(
                    request=request,
                    body=request.json,
                    url=url,
                    content_type=returned_content_type,
                    method=request.method,
                )

    def _add_schema_registry_routes(self) -> None:
        pass

    async def close(self) -> None:
        self.log.info("Closing karapace_schema_registry_controller")
        async with AsyncExitStack() as stack:
            stack.push_async_callback(super().close)
            stack.push_async_callback(self.schema_registry.close)
            if self._forward_client:
                stack.push_async_callback(self._forward_client.close)
            if self._auth is not None:
                stack.push_async_callback(self._auth.close)

    def _subject_get(
        self, subject: Subject, content_type: str, include_deleted: bool = False
    ) -> dict[Version, SchemaVersion]:
        try:
            schema_versions = self.schema_registry.subject_get(subject, include_deleted)
        except SubjectNotFoundException:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )
        except SchemasNotFoundException:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )
        return schema_versions

    def _invalid_version(self, version: str | int):
        """Shall be called when InvalidVersion is raised"""
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error_code": SchemaErrorCodes.INVALID_VERSION_ID.value,
                "message": (
                    f"The specified version '{version}' is not a valid version id. "
                    'Allowed values are between [1, 2^31-1] and the string "latest"'
                ),
            },
        )

    async def compatibility_check(
        #        self, content_type: str, *, subject: Subject, version: str, request: HTTPRequest, user: User | None = None
        self,
        *,
        subject: Subject,
        schema_request: SchemaRequest,
        version: str,
        user: User | None = None,
    ) -> CompatibilityCheckResponse:
        """Check for schema compatibility"""

        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        try:
            compatibility_mode = self.schema_registry.get_compatibility_mode(subject=subject)
        except ValueError as ex:
            # Using INTERNAL_SERVER_ERROR because the subject and configuration
            # should have been validated before.
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": str(ex),
                },
            )

        new_schema = self.get_new_schema(schema_request=schema_request)
        old_schema = self.get_old_schema(subject, Versioner.V(version))  # , content_type)
        if compatibility_mode.is_transitive():
            # Ignore the schema version provided in the rest api call (`version`)
            # Instead check against all previous versions (including `version` if existing)
            result = self.schema_registry.check_schema_compatibility(new_schema, subject)
        else:
            # Check against the schema version provided in the rest api call (`version`)
            result = SchemaCompatibility.check_compatibility(old_schema, new_schema, compatibility_mode)

        if is_incompatible(result):
            return CompatibilityCheckResponse(is_compatible=False, messages=list(result.messages))
        return CompatibilityCheckResponse(is_compatible=True)

    #    async def schemas_list(self, content_type: str, *, request: HTTPRequest, user: User | None = None):
    async def schemas_list(self, *, deleted: bool, latest_only: bool, user: User | None = None) -> list[SchemaListingItem]:
        schemas = await self.schema_registry.schemas_list(include_deleted=deleted, latest_only=latest_only)
        response_schemas: list[SchemaListingItem] = []
        for subject, schema_versions in schemas.items():
            if self._auth and not self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"):
                continue
            for schema_version in schema_versions:
                references: list[Any] | None = None
                if schema_version.references:
                    references = [r.to_dict() for r in schema_version.references]
                response_schemas.append(
                    SchemaListingItem(
                        subject=schema_version.subject,
                        schema=schema_version.schema.schema_str,
                        version=schema_version.version.value,
                        id=schema_version.schema_id,
                        schemaType=schema_version.schema.schema_type,
                        references=references,
                    )
                )

        return response_schemas

    async def schemas_get(
        #        self, content_type: str, *, request: HTTPRequest, user: User | None = None, schema_id: str
        self,
        *,
        schema_id: str,
        fetch_max_id: bool,
        include_subjects: bool,
        format_serialized: str,
        user: User | None = None,
    ) -> SchemasResponse:
        try:
            parsed_schema_id = SchemaId(int(schema_id))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
            )

        def _has_subject_with_id() -> bool:
            # Fast path
            if self._auth is None or self._auth.check_authorization(user, Operation.Read, "Subject:*"):
                return True

            subjects = self.schema_registry.database.subjects_for_schema(schema_id=parsed_schema_id)
            resources = [f"Subject:{subject}" for subject in subjects]
            return self._auth.check_authorization_any(user=user, operation=Operation.Read, resources=resources)

        if self._auth:
            has_subject = _has_subject_with_id()
            if not has_subject:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                        "message": "Schema not found",
                    },
                )

        schema = self.schema_registry.schemas_get(parsed_schema_id, fetch_max_id=fetch_max_id)
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                    "message": "Schema not found",
                },
            )

        schema_str = schema.schema_str
        if format_serialized and schema.schema_type == SchemaType.PROTOBUF:
            parsed_schema = ParsedTypedSchema.parse(schema_type=schema.schema_type, schema_str=schema_str)
            schema_str = parsed_schema.serialize()

        subjects: list[Subject] | None = None
        schema_type: SchemaType | None = None
        references: list[Any] | None = None  # TODO: typing
        maxId: int | None = None

        if include_subjects:
            subjects = self.schema_registry.database.subjects_for_schema(parsed_schema_id)
        if schema.schema_type is not SchemaType.AVRO:
            schema_type = schema.schema_type
        if schema.references:
            references = [r.to_dict() for r in schema.references]
        if fetch_max_id:
            maxId = schema.max_id

        return SchemasResponse(
            schema=schema_str,
            subjects=subjects,
            schema_type=schema_type,
            references=references,
            maxId=maxId,
        )

    async def schemas_get_versions(
        #        self, content_type: str, *, schema_id: str, deleted: bool, user: User | None = None
        self,
        *,
        schema_id: str,
        deleted: bool,
        user: User | None = None,
    ) -> list[SubjectVersion]:
        try:
            schema_id_int = SchemaId(int(schema_id))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
            )

        subject_versions = []
        for subject_version in self.schema_registry.get_subject_versions_for_schema(schema_id_int, include_deleted=deleted):
            subject = subject_version["subject"]
            if self._auth and not self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"):
                continue
            subject_versions.append(
                # TODO correct typing
                SubjectVersion(
                    subject=subject_version["subject"],
                    version=subject_version["version"].value,
                ),
            )
        return subject_versions

    async def schemas_types(self) -> list[str]:
        return ["JSON", "AVRO", "PROTOBUF"]

    #    async def config_get(self, content_type: str, *, user: User | None = None) -> None:
    async def config_get(self, *, user: User | None = None) -> CompatibilityLevelResponse:
        self._check_authorization(user, Operation.Read, "Config:")

        # Note: The format sent by the user differs from the return value, this
        # is for compatibility reasons.
        return CompatibilityLevelResponse(compatibilityLevel=self.schema_registry.schema_reader.config.compatibility)

    #    async def config_set(self, content_type: str, *, request: HTTPRequest, user: User | None = None) -> None:
    async def config_set(
        self, *, compatibility_level_request: CompatibilityRequest, user: User | None = None
    ) -> CompatibilityResponse:
        self._check_authorization(user, Operation.Write, "Config:")

        try:
            compatibility_level = CompatibilityModes(compatibility_level_request.compatibility)
        except (ValueError, KeyError):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": SchemaErrorMessages.INVALID_COMPATIBILITY_LEVEL.value,
                },
            )

        primary_info = await self.schema_registry.get_master()
        if primary_info.primary:
            self.schema_registry.send_config_message(compatibility_level=compatibility_level, subject=None)
        elif not primary_info.primary_url:
            self.no_master_error(content_type)
        else:
            url = f"{primary_info.primary_url}/config"
            await self._forward_request_remote(request=request, body=body, url=url, content_type=content_type, method="PUT")

        return CompatibilityResponse(compatibility=self.schema_registry.schema_reader.config.compatibility)

    async def config_subject_get(
        #        self, content_type: str, subject: str, *, request: HTTPRequest, user: User | None = None
        self,
        *,
        subject: str,
        default_to_global: bool,
        user: User | None = None,
    ) -> CompatibilityLevelResponse:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        # Config for a subject can exist without schemas so no need to check for their existence
        assert self.schema_registry.schema_reader, "KarapaceSchemaRegistry not initialized. Missing call to _init"
        if self.schema_registry.database.find_subject(subject=Subject(subject)) is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )

        compatibility = self.schema_registry.database.get_subject_compatibility(subject=Subject(subject))
        if not compatibility and default_to_global:
            compatibility = self.schema_registry.compatibility
        if compatibility:
            # Note: The format sent by the user differs from the return
            # value, this is for compatibility reasons.
            return CompatibilityLevelResponse(compatibilityLevel=compatibility)

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error_code": SchemaErrorCodes.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE.value,
                "message": SchemaErrorMessages.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_FMT.value.format(subject=subject),
            },
        )

    async def config_subject_set(
        self,
        #        content_type: str,
        *,
        subject: str,
        compatibility_level_request: CompatibilityRequest,
        user: User | None = None,
    ) -> CompatibilityResponse:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        try:
            compatibility_level = CompatibilityModes(compatibility_level_request.compatibility)
        except (ValueError, KeyError):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": "Invalid compatibility level",
                },
            )

        primary_info = await self.schema_registry.get_master()
        if primary_info.primary:
            self.schema_registry.send_config_message(compatibility_level=compatibility_level, subject=Subject(subject))
        elif not primary_info.primary_url:
            self.no_master_error(content_type)
        else:
            url = f"{primary_info.primary_url}/config/{subject}"
            await self._forward_request_remote(
                request=request, body=request.json, url=url, content_type=content_type, method="PUT"
            )

        return CompatibilityResponse(compatibility=compatibility_level.value)

    async def config_subject_delete(
        self,
        #        content_type,
        *,
        subject: str,
        user: User | None = None,
    ) -> CompatibilityResponse:
        if self._auth:
            if not self._auth.check_authorization(user, Operation.Write, f"Subject:{subject}"):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"message": "Forbidden"},
                )

        primary_info = await self.schema_registry.get_master()
        if primary_info.primary:
            self.schema_registry.send_config_subject_delete_message(subject=Subject(subject))
        elif not primary_info.primary_url:
            self.no_master_error(content_type)
        else:
            url = f"{primary_info.primary_url}/config/{subject}"
            await self._forward_request_remote(
                request=request, body=request.json, url=url, content_type=content_type, method="PUT"
            )
        return CompatibilityResponse(compatibility=self.schema_registry.schema_reader.config.compatibility)

    # async def subjects_list(self, content_type: str, *, request: HTTPRequest, user: User | None = None) -> None:
    async def subjects_list(self, deleted: bool, user: User | None = None) -> list[str]:
        subjects = [str(subject) for subject in self.schema_registry.database.find_subjects(include_deleted=deleted)]
        if self._auth is not None:
            subjects = list(
                filter(
                    lambda subject: self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"),
                    subjects,
                )
            )
        return subjects

    async def subject_delete(
        #        self, content_type: str, *, subject: str, request: HTTPRequest, user: User | None = None
        self,
        *,
        subject: str,
        permanent: bool,
        user: User | None = None,
    ) -> list[int]:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        primary_info = await self.schema_registry.get_master()
        if primary_info.primary:
            try:
                version_list = await self.schema_registry.subject_delete_local(subject=Subject(subject), permanent=permanent)
                return [version.value for version in version_list]
            except (SubjectNotFoundException, SchemasNotFoundException):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                        "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                    },
                )
            except SubjectNotSoftDeletedException:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.SUBJECT_NOT_SOFT_DELETED.value,
                        "message": f"Subject '{subject}' was not deleted first before being permanently deleted",
                    },
                )
            except SubjectSoftDeletedException:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.SUBJECT_SOFT_DELETED.value,
                        "message": f"Subject '{subject}' was soft deleted.Set permanent=true to delete permanently",
                    },
                )

            except ReferenceExistsException as arg:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "error_code": SchemaErrorCodes.REFERENCE_EXISTS.value,
                        "message": (
                            f"One or more references exist to the schema "
                            f"{{magic=1,keytype=SCHEMA,subject={subject},version={arg.version}}}."
                        ),
                    },
                )
        elif not primary_info.primary_url:
            self.no_master_error(content_type)
        else:
            url = f"{primary_info.primary_url}/subjects/{subject}?permanent={permanent}"
            await self._forward_request_remote(request=request, body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_get(
        #        self, content_type: str, *, subject: str, version: str, request: HTTPRequest, user: User | None = None
        self,
        subject: str,
        version: str,
        deleted: bool,
        user: User | None = None,
    ) -> SubjectSchemaVersionResponse:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        try:
            subject_data = self.schema_registry.subject_version_get(
                Subject(subject), Versioner.V(version), include_deleted=deleted
            )
            return SubjectSchemaVersionResponse(
                subject=subject_data["subject"],
                version=subject_data["version"],
                id=subject_data["id"],
                schema=subject_data["schema"],
                references=subject_data.get("references", None),
                schema_type=subject_data.get("schemaType", None),
                compatibility=None,  # Do not return compatibility from this endpoint.
            )
        except (SubjectNotFoundException, SchemasNotFoundException):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )
        except VersionNotFoundException:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            )
        except InvalidVersion:
            self._invalid_version(version)

    async def subject_version_delete(
        #        self, content_type: str, *, subject: str, version: str, request: HTTPRequest, user: User | None = None
        self,
        *,
        subject: str,
        version: str,
        permanent: bool,
        user: User | None = None,
    ) -> int:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        primary_info = await self.schema_registry.get_master()
        if primary_info.primary:
            try:
                resolved_version = await self.schema_registry.subject_version_delete_local(
                    Subject(subject), Versioner.V(version), permanent
                )
                return resolved_version.value
            except (SubjectNotFoundException, SchemasNotFoundException):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                        "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                    },
                )
            except VersionNotFoundException:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                        "message": f"Version {version} not found.",
                    },
                )
            except SchemaVersionSoftDeletedException:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.SCHEMAVERSION_SOFT_DELETED.value,
                        "message": (
                            f"Subject '{subject}' Version {version} was soft deleted. "
                            "Set permanent=true to delete permanently"
                        ),
                    },
                )
            except SchemaVersionNotSoftDeletedException:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error_code": SchemaErrorCodes.SCHEMAVERSION_NOT_SOFT_DELETED.value,
                        "message": (
                            f"Subject '{subject}' Version {version} was not deleted "
                            "first before being permanently deleted"
                        ),
                    },
                )
            except ReferenceExistsException as arg:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "error_code": SchemaErrorCodes.REFERENCE_EXISTS.value,
                        "message": (
                            f"One or more references exist to the schema "
                            f"{{magic=1,keytype=SCHEMA,subject={subject},version={arg.version}}}."
                        ),
                    },
                )
            except InvalidVersion:
                self._invalid_version(version)
        elif not primary_info.primary_url:
            self.no_master_error(content_type)
        else:
            url = f"{primary_info.primary_url}/subjects/{subject}/versions/{version}?permanent={permanent}"
            await self._forward_request_remote(request=request, body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_schema_get(
        #        self, content_type: str, *, subject: str, version: str, user: User | None = None
        self,
        *,
        subject: str,
        version: str,
        user: User | None = None,
    ) -> dict:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        try:
            subject_data = self.schema_registry.subject_version_get(Subject(subject), Versioner.V(version))
            return json.loads(cast(str, subject_data["schema"]))  # TODO typing
        except InvalidVersion:
            self._invalid_version(version)
        except VersionNotFoundException:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            )
        except (SchemasNotFoundException, SubjectNotFoundException):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )

    #    async def subject_version_referencedby_get(self, content_type, *, subject, version, user: User | None = None):
    async def subject_version_referencedby_get(self, *, subject: str, version, user: User | None = None) -> list[int]:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        referenced_by: list[int] = []
        try:
            referenced_by = await self.schema_registry.subject_version_referencedby_get(
                Subject(subject), Versioner.V(version)
            )
        except (SubjectNotFoundException, SchemasNotFoundException):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )
        except VersionNotFoundException:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            )
        except InvalidVersion:
            self._invalid_version(version)

        return referenced_by

    async def subject_versions_list(
        #        self, content_type: str, *, subject: str, request: HTTPRequest, user: User | None = None
        self,
        *,
        subject: str,
        deleted: bool,
        user: User | None = None,
    ) -> list[int]:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")
        try:
            schema_versions = self.schema_registry.subject_get(Subject(subject), include_deleted=deleted)
            version_list = [version.value for version in schema_versions]
            return version_list
        except (SubjectNotFoundException, SchemasNotFoundException):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )

    def _validate_schema_request_body(self, content_type: str, body: dict | Any) -> None:
        if not isinstance(body, dict):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_BAD_REQUEST.value,
                    "message": "Malformed request",
                },
                content_type=content_type,
                status=HTTPStatus.BAD_REQUEST,
            )
        for field in body:
            if field not in {"schema", "schemaType", "references", "metadata", "ruleSet"}:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                        "message": f"Unrecognized field: {field}",
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )

    def _validate_schema_type(self, content_type: str, data: JsonData) -> SchemaType:
        if not isinstance(data, dict):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_BAD_REQUEST.value,
                    "message": "Malformed request",
                },
                content_type=content_type,
                status=HTTPStatus.BAD_REQUEST,
            )
        schema_type_unparsed = data.get("schemaType", SchemaType.AVRO.value)
        try:
            schema_type = SchemaType(schema_type_unparsed)
        except ValueError:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                    "message": f"Invalid schemaType {schema_type_unparsed}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        return schema_type

    def _validate_references(
        self,
        content_type: str,
        schema_type: SchemaType,
        body: JsonData,
    ) -> list[Reference | LatestVersionReference] | None:
        references = body.get("references", [])
        # Allow passing `null` as value for compatibility
        if references is None:
            return None
        if not isinstance(references, list):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_BAD_REQUEST.value,
                    "message": "Expected array of `references`",
                },
                content_type=content_type,
                status=HTTPStatus.BAD_REQUEST,
            )
        if references and schema_type != SchemaType.PROTOBUF:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.REFERENCES_SUPPORT_NOT_IMPLEMENTED.value,
                    "message": SchemaErrorMessages.REFERENCES_SUPPORT_NOT_IMPLEMENTED.value.format(
                        schema_type=schema_type.value
                    ),
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        validated_references = []
        for reference_data in references:
            try:
                reference = reference_from_mapping(reference_data)
            except (TypeError, KeyError) as exc:
                raise InvalidReferences from exc
            validated_references.append(reference)
        if validated_references:
            return validated_references
        return None

    async def subjects_schema_post(
        self, *, subject: Subject, schema_request: SchemaRequest, deleted: bool, normalize: bool, user: User | None = None
    ) -> SchemaResponse:
        content_type = "application/json"
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")
        try:
            subject_data = self._subject_get(subject, content_type, include_deleted=deleted)
        except (SchemasNotFoundException, SubjectNotFoundException):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )
        # TODO
        references = None
        new_schema_dependencies = None
        # references = self._validate_references(content_type, schema_type, body)
        # references, new_schema_dependencies = self.schema_registry.resolve_references(references)

        new_schema: ParsedTypedSchema | None = None
        try:
            # When checking if schema is already registered, allow unvalidated schema in as
            # there might be stored schemas that are non-compliant from the past.
            new_schema = ParsedTypedSchema.parse(
                schema_type=schema_request.schema_type,
                schema_str=schema_request.schema_str,
                references=references,
                dependencies=new_schema_dependencies,
                normalize=normalize,
                use_protobuf_formatter=self.config.use_protobuf_formatter,
            )
        except InvalidSchema:
            self.log.warning("Invalid schema: %r", schema_request.schema_str)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Error while looking up schema under subject {subject}",
                },
            )
        except InvalidReferences:
            human_error = "Provided references is not valid"
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_request.schema_type} references. Error: {human_error}",
                },
            )

        # Match schemas based on version from latest to oldest
        for schema_version in sorted(subject_data.values(), key=lambda item: item.version, reverse=True):
            other_references, other_dependencies = self.schema_registry.resolve_references(schema_version.references)
            try:
                parsed_typed_schema = ParsedTypedSchema.parse(
                    schema_version.schema.schema_type,
                    schema_version.schema.schema_str,
                    references=other_references,
                    dependencies=other_dependencies,
                    normalize=normalize,
                )
            except InvalidSchema as e:
                failed_schema_id = schema_version.schema_id
                self.log.exception("Existing schema failed to parse. Id: %s", failed_schema_id)
                self.stats.unexpected_exception(
                    ex=e, where="Matching existing schemas to posted. Failed schema id: {failed_schema_id}"
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={
                        "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                        "message": f"Error while looking up schema under subject {subject}",
                    },
                )

            if schema_request.schema_type is SchemaType.JSONSCHEMA:
                schema_valid = parsed_typed_schema.to_dict() == new_schema.to_dict()
            else:
                schema_valid = new_schema.match(parsed_typed_schema)
            if parsed_typed_schema.schema_type == new_schema.schema_type and schema_valid:
                schema_type: SchemaType | None = None
                if schema_request.schema_type is not SchemaType.AVRO:
                    schema_type = schema_request.schema_type
                return SchemaResponse(
                    subject=subject,
                    version=schema_version.version.value,
                    id=schema_version.schema_id,
                    schema=parsed_typed_schema.schema_str,
                    schemaType=schema_type,
                )
            else:
                self.log.debug("Schema %r did not match %r", schema_version, parsed_typed_schema)

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                "message": "Schema not found",
            },
        )

    async def subject_post(
        self,
        *,
        subject: str,
        schema_request: SchemaRequest,
        normalize: bool,
        user: User | None = None,
    ) -> SchemaIdResponse:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        self.log.debug("POST with subject: %r, request: %r", subject, schema_request)
        _validate_subject(subject=subject)

        # TODO
        # self._validate_schema_request_body(content_type, body)
        # self._validate_schema_key(schema_request)
        # references = self._validate_references(content_type, schema_type, body)
        references = None

        try:
            # references, resolved_dependencies = self.schema_registry.resolve_references(references)
            resolved_dependencies = {}
            new_schema = ValidatedTypedSchema.parse(
                schema_type=schema_request.schema_type,
                schema_str=schema_request.schema_str,
                references=references,
                dependencies=resolved_dependencies,
                normalize=normalize,
                use_protobuf_formatter=self.config.use_protobuf_formatter,
            )
        except (InvalidReferences, InvalidSchema, InvalidSchemaType) as e:
            self.log.warning("Invalid schema: %r", schema_request.schema_str, exc_info=True)
            if isinstance(e.__cause__, (SchemaParseException, JSONDecodeError, ProtobufUnresolvedDependencyException)):
                human_error = f"{e.__cause__.args[0]}"  # pylint: disable=no-member
            else:
                from_body_schema_str = schema_request.schema_str
                human_error = (
                    f"Invalid schema {from_body_schema_str} with refs {references} of type {schema_request.schema_type}"
                )
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_request.schema_type.value} schema. Error: {human_error}",
                },
            )

        schema_id = self.get_schema_id_if_exists(subject=Subject(subject), schema=new_schema, include_deleted=False)
        if schema_id is not None:
            return SchemaIdResponse(id=schema_id)

        primary_info = await self.schema_registry.get_master()
        if primary_info.primary:
            try:
                schema_id = await self.schema_registry.write_new_schema_local(Subject(subject), new_schema, references)
                return SchemaIdResponse(id=schema_id)
            except InvalidSchema as ex:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                        "message": f"Invalid {schema_request.schema_type.value} schema. Error: {str(ex)}",
                    },
                )
            except IncompatibleSchema as ex:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error_code": SchemaErrorCodes.HTTP_CONFLICT.value,
                        "message": str(ex),
                    },
                )
            except SchemaTooLargeException:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "error_code": SchemaErrorCodes.SCHEMA_TOO_LARGE_ERROR_CODE.value,
                        "message": "Schema is too large",
                    },
                )
            except Exception as xx:
                raise xx

        elif not primary_info.primary_url:
            self.no_master_error("application/json")  # forward content type
        else:
            # TODO
            url = f"{primary_info.primary_url}/subjects/{subject}/versions"
            return await self._forward_request_remote(
                request=request, body=body, url=url, content_type=content_type, method="POST"
            )

    async def get_global_mode(
        self,
        *,
        user: User | None = None,
    ) -> ModeResponse:
        self._check_authorization(user, Operation.Read, "Config:")
        return ModeResponse(mode=str(self.schema_registry.get_global_mode()))

    async def get_subject_mode(
        self,
        *,
        subject: str,
        user: User | None = None,
    ) -> ModeResponse:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        if self.schema_registry.database.find_subject(subject=Subject(subject)) is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            )
        return ModeResponse(mode=str(self.schema_registry.get_global_mode()))

    def get_schema_id_if_exists(self, *, subject: Subject, schema: TypedSchema, include_deleted: bool) -> SchemaId | None:
        schema_id = self.schema_registry.database.get_schema_id_if_exists(
            subject=subject, schema=schema, include_deleted=include_deleted
        )
        return schema_id

    async def _forward_request_remote(
        self, *, request: HTTPRequest, body: dict | None, url: str, content_type: str, method: str = "POST"
    ) -> None:
        assert self._forward_client is not None, "Server must be initialized"

        self.log.info("Forwarding %s request to remote url: %r since we're not the master", method, url)
        timeout = 60.0
        func = getattr(self._forward_client, method.lower())
        auth_header = request.headers.get("Authorization")
        headers = {}
        if auth_header is not None:
            headers["Authorization"] = auth_header

        async with async_timeout.timeout(timeout):
            async with func(url, headers=headers, json=body) as response:
                if response.headers.get("content-type", "").startswith(JSON_CONTENT_TYPE):
                    resp_content = await response.json()
                else:
                    resp_content = await response.text()

        raise HTTPResponse(
            body=resp_content, content_type=content_type, status=HTTPStatus(response.status), headers=response.headers
        )

    def no_master_error(self, content_type: str) -> None:
        self.r(
            body={
                "error_code": SchemaErrorCodes.NO_MASTER_ERROR.value,
                "message": "Error while forwarding the request to the master.",
            },
            content_type=content_type,
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    def get_new_schema(self, schema_request: SchemaRequest) -> ValidatedTypedSchema:
        #        schema_type = self._validate_schema_type(content_type=content_type, data=body)
        #        references = self._validate_references(content_type, schema_type, body)
        references = None
        try:
            # references, new_schema_dependencies = self.schema_registry.resolve_references(references)
            new_schema_dependencies = {}
            return ValidatedTypedSchema.parse(
                schema_type=schema_request.schema_type,
                schema_str=schema_request.schema_str,
                references=references,
                dependencies=new_schema_dependencies,
                use_protobuf_formatter=self.config.use_protobuf_formatter,
            )
        except InvalidSchema:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_request.schema_type} schema",
                },
            )

    #    def get_old_schema(self, subject: Subject, version: Version, content_type: str) -> ParsedTypedSchema:
    def get_old_schema(self, subject: Subject, version: Version) -> ParsedTypedSchema:
        old: JsonObject | None = None
        try:
            old = self.schema_registry.subject_version_get(subject=subject, version=version)
        except InvalidVersion:
            # TODO remove content type
            self._invalid_version(version.value)
        except (VersionNotFoundException, SchemasNotFoundException, SubjectNotFoundException):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            )
        assert old is not None
        # TODO: remove content type
        old_schema_type = self._validate_schema_type(content_type="application/json", data=old)
        try:
            old_references = old.get("references", None)
            old_dependencies = None
            if old_references:
                old_references, old_dependencies = self.schema_registry.resolve_references(old_references)
            old_schema = ParsedTypedSchema.parse(old_schema_type, old["schema"], old_references, old_dependencies)
            return old_schema
        except InvalidSchema:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Found an invalid {old_schema_type} schema registered",
                },
            )
