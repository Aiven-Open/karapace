"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from avro.errors import SchemaParseException
from contextlib import AsyncExitStack
from enum import Enum, unique
from http import HTTPStatus
from karapace.auth import HTTPAuthorizer, Operation, User
from karapace.compatibility import check_compatibility, CompatibilityModes
from karapace.compatibility.jsonschema.checks import is_incompatible
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
from karapace.karapace import KarapaceBase
from karapace.protobuf.exception import ProtobufUnresolvedDependencyException
from karapace.rapu import HTTPRequest, JSON_CONTENT_TYPE, SERVER_NAME
from karapace.schema_models import ParsedTypedSchema, SchemaType, SchemaVersion, TypedSchema, ValidatedTypedSchema, Versioner
from karapace.schema_references import LatestVersionReference, Reference, reference_from_mapping
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.typing import JsonData, JsonObject, SchemaId, Subject, Version
from karapace.utils import JSONDecodeError
from typing import Any

import aiohttp
import async_timeout


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


class KarapaceSchemaRegistryController(KarapaceBase):
    def __init__(self, config: Config) -> None:
        super().__init__(config=config, not_ready_handler=self._forward_if_not_ready_to_serve)

        self._auth: HTTPAuthorizer | None = None
        if self.config["registry_authfile"] is not None:
            self._auth = HTTPAuthorizer(str(self.config["registry_authfile"]))
            self.app.on_startup.append(self._start_authorizer)

        self.schema_registry = KarapaceSchemaRegistry(config)
        self._add_schema_registry_routes()

        self._forward_client = None
        self.app.on_startup.append(self._start_schema_registry)
        self.app.on_startup.append(self._create_forward_client)
        self.health_hooks.append(self.schema_registry_health)

    async def schema_registry_health(self) -> JsonObject:
        resp = {}
        if self._auth is not None:
            resp["schema_registry_authfile_timestamp"] = self._auth.authfile_last_modified
        resp["schema_registry_ready"] = self.schema_registry.schema_reader.ready
        if self.schema_registry.schema_reader.ready:
            resp["schema_registry_startup_time_sec"] = (
                self.schema_registry.schema_reader.last_check - self._process_start_time
            )
        resp["schema_registry_reader_current_offset"] = self.schema_registry.schema_reader.offset
        resp["schema_registry_reader_highest_offset"] = self.schema_registry.schema_reader.highest_offset()
        cs = self.schema_registry.mc.get_coordinator_status()
        resp["schema_registry_is_primary"] = cs.is_primary
        resp["schema_registry_is_primary_eligible"] = cs.is_primary_eligible
        resp["schema_registry_primary_url"] = cs.primary_url
        resp["schema_registry_coordinator_running"] = cs.is_running
        resp["schema_registry_coordinator_generation_id"] = cs.group_generation_id
        return resp

    async def _start_schema_registry(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
        """Callback for aiohttp.Application.on_startup"""
        await self.schema_registry.start()

    async def _create_forward_client(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
        """Callback for aiohttp.Application.on_startup"""
        self._forward_client = aiohttp.ClientSession(headers={"User-Agent": SERVER_NAME})

    async def _start_authorizer(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
        """Callback for aiohttp.Application.on_startup"""
        await self._auth.start_refresh_task(self.stats)

    def _check_authorization(self, user: User | None, operation: Operation, resource: str) -> None:
        if self._auth:
            if not self._auth.check_authorization(user, operation, resource):
                self.r(body={"message": "Forbidden"}, content_type=JSON_CONTENT_TYPE, status=HTTPStatus.FORBIDDEN)

    async def _forward_if_not_ready_to_serve(self, request: HTTPRequest) -> None:
        if self.schema_registry.schema_reader.ready:
            pass
        else:
            # Not ready, still loading the state.
            # Needs only the master_url
            _, master_url = await self.schema_registry.get_master(ignore_readiness=True)
            if not master_url:
                self.no_master_error(request.content_type)
            elif f"{self.config['advertised_hostname']}:{self.config['advertised_port']}" in master_url:
                # If master url is the same as the url of this Karapace respond 503.
                self.r(
                    body="",
                    content_type=request.get_header("Content-Type"),
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
            else:
                url = f"{master_url}{request.url.path}"
                await self._forward_request_remote(
                    request=request,
                    body=request.json,
                    url=url,
                    content_type=request.get_header("Content-Type"),
                    method=request.method,
                )

    def _add_schema_registry_routes(self) -> None:
        self.route(
            "/compatibility/subjects/<subject:path>/versions/<version:path>",
            callback=self.compatibility_check,
            method="POST",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/config/<subject:path>",
            callback=self.config_subject_get,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/config/<subject:path>",
            callback=self.config_subject_set,
            method="PUT",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/config/<subject:path>",
            callback=self.config_subject_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/config",
            callback=self.config_get,
            method="GET",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/config",
            callback=self.config_set,
            method="PUT",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/schemas",
            callback=self.schemas_list,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/schemas/ids/<schema_id:path>/versions",
            callback=self.schemas_get_versions,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/schemas/ids/<schema_id:path>",
            callback=self.schemas_get,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route("/schemas/types", callback=self.schemas_types, method="GET", schema_request=True, auth=None)
        self.route(
            "/subjects",
            callback=self.subjects_list,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>/versions",
            callback=self.subject_post,
            method="POST",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>",
            callback=self.subjects_schema_post,
            method="POST",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>/versions",
            callback=self.subject_versions_list,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>",
            callback=self.subject_version_get,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version:path>",  # needs
            callback=self.subject_version_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>/schema",
            callback=self.subject_version_schema_get,
            method="GET",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>/referencedby",
            callback=self.subject_version_referencedby_get,
            method="GET",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>",
            callback=self.subject_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/mode",
            callback=self.get_global_mode,
            method="GET",
            schema_request=True,
            with_request=False,
            json_body=False,
            auth=self._auth,
        )
        self.route(
            "/mode/<subject:path>",
            callback=self.get_subject_mode,
            method="GET",
            schema_request=True,
            with_request=False,
            json_body=False,
            auth=self._auth,
        )

    async def close(self) -> None:
        self.log.info("Closing karapace_schema_registry_controller")
        async with AsyncExitStack() as stack:
            stack.push_async_callback(super().close)
            stack.push_async_callback(self.schema_registry.close)
            if self._forward_client:
                stack.push_async_callback(self._forward_client.close)
            if self._auth is not None:
                stack.push_async_callback(self._auth.close)

    def _subject_get(self, subject: str, content_type: str, include_deleted: bool = False) -> dict[Version, SchemaVersion]:
        try:
            schema_versions = self.schema_registry.subject_get(subject, include_deleted)
        except SubjectNotFoundException:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        except SchemasNotFoundException:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        return schema_versions

    def _invalid_version(self, content_type, version):
        """Shall be called when InvalidVersion is raised"""
        self.r(
            body={
                "error_code": SchemaErrorCodes.INVALID_VERSION_ID.value,
                "message": (
                    f"The specified version '{version}' is not a valid version id. "
                    'Allowed values are between [1, 2^31-1] and the string "latest"'
                ),
            },
            content_type=content_type,
            status=HTTPStatus.UNPROCESSABLE_ENTITY,
        )

    async def compatibility_check(
        self, content_type: str, *, subject: str, version: str, request: HTTPRequest, user: User | None = None
    ) -> None:
        """Check for schema compatibility"""

        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        body = request.json
        schema_type = self._validate_schema_type(content_type=content_type, data=body)
        references = self._validate_references(content_type, schema_type, body)
        try:
            references, new_schema_dependencies = self.schema_registry.resolve_references(references)
            new_schema = ValidatedTypedSchema.parse(
                schema_type=schema_type,
                schema_str=body["schema"],
                references=references,
                dependencies=new_schema_dependencies,
                use_protobuf_formatter=self.config["use_protobuf_formatter"],
            )
        except InvalidSchema:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_type} schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        try:
            old = self.schema_registry.subject_version_get(subject=subject, version=Versioner.V(version))
        except InvalidVersion:
            self._invalid_version(content_type, version)
        except (VersionNotFoundException, SchemasNotFoundException, SubjectNotFoundException):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        old_schema_type = self._validate_schema_type(content_type=content_type, data=old)
        try:
            old_references = old.get("references", None)
            old_dependencies = None
            if old_references:
                old_references, old_dependencies = self.schema_registry.resolve_references(old_references)
            old_schema = ParsedTypedSchema.parse(old_schema_type, old["schema"], old_references, old_dependencies)
        except InvalidSchema:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Found an invalid {old_schema_type} schema registered",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        try:
            compatibility_mode = self.schema_registry.get_compatibility_mode(subject=subject)
        except ValueError as ex:
            # Using INTERNAL_SERVER_ERROR because the subject and configuration
            # should have been validated before.
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": str(ex),
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        result = check_compatibility(
            old_schema=old_schema,
            new_schema=new_schema,
            compatibility_mode=compatibility_mode,
        )
        if is_incompatible(result):
            self.r({"is_compatible": False}, content_type)
        self.r({"is_compatible": True}, content_type)

    async def schemas_list(self, content_type: str, *, request: HTTPRequest, user: User | None = None):
        deleted = request.query.get("deleted", "false").lower() == "true"
        latest_only = request.query.get("latestOnly", "false").lower() == "true"

        schemas = await self.schema_registry.schemas_list(include_deleted=deleted, latest_only=latest_only)
        response_schemas = []
        for subject, schema_versions in schemas.items():
            if self._auth and not self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"):
                continue
            for schema_version in schema_versions:
                response_schema = {
                    "subject": schema_version.subject,
                    "version": schema_version.version.value,
                    "id": schema_version.schema_id,
                    "schemaType": schema_version.schema.schema_type,
                }
                if schema_version.references:
                    response_schema["references"] = [r.to_dict() for r in schema_version.references]
                response_schema["schema"] = schema_version.schema.schema_str
                response_schemas.append(response_schema)

        self.r(
            body=response_schemas,
            content_type=content_type,
            status=HTTPStatus.OK,
        )

    async def schemas_get(
        self, content_type: str, *, request: HTTPRequest, user: User | None = None, schema_id: str
    ) -> None:
        try:
            parsed_schema_id = SchemaId(int(schema_id))
        except ValueError:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        include_subjects = request.query.get("includeSubjects", "false").lower() == "true"

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
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                        "message": "Schema not found",
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )

        fetch_max_id = request.query.get("fetchMaxId", "false").lower() == "true"
        schema = self.schema_registry.schemas_get(parsed_schema_id, fetch_max_id=fetch_max_id)
        if not schema:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                    "message": "Schema not found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        schema_str = schema.schema_str
        format_serialized = request.query.get("format", "").lower() == "serialized"
        if format_serialized and schema.schema_type == SchemaType.PROTOBUF:
            parsed_schema = ParsedTypedSchema.parse(schema_type=schema.schema_type, schema_str=schema_str)
            schema_str = parsed_schema.serialize()
        response_body = {"schema": schema_str}

        if include_subjects:
            response_body["subjects"] = self.schema_registry.database.subjects_for_schema(parsed_schema_id)

        if schema.schema_type is not SchemaType.AVRO:
            response_body["schemaType"] = schema.schema_type
        if schema.references:
            response_body["references"] = [r.to_dict() for r in schema.references]
        if fetch_max_id:
            response_body["maxId"] = schema.max_id

        self.r(response_body, content_type)

    async def schemas_get_versions(
        self, content_type: str, *, schema_id: str, request: HTTPRequest, user: User | None = None
    ) -> None:
        try:
            schema_id_int = int(schema_id)
        except ValueError:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        deleted = request.query.get("deleted", "false").lower() == "true"
        subject_versions = []
        for subject_version in self.schema_registry.get_subject_versions_for_schema(schema_id_int, include_deleted=deleted):
            subject = subject_version["subject"]
            if self._auth and not self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"):
                continue
            subject_versions.append(
                {
                    "subject": subject_version["subject"],
                    "version": subject_version["version"].value,
                }
            )
        self.r(subject_versions, content_type)

    async def schemas_types(self, content_type: str) -> None:
        self.r(["JSON", "AVRO", "PROTOBUF"], content_type)

    async def config_get(self, content_type: str, *, user: User | None = None) -> None:
        self._check_authorization(user, Operation.Read, "Config:")

        # Note: The format sent by the user differs from the return value, this
        # is for compatibility reasons.
        self.r({"compatibilityLevel": self.schema_registry.schema_reader.config["compatibility"]}, content_type)

    async def config_set(self, content_type: str, *, request: HTTPRequest, user: User | None = None) -> None:
        self._check_authorization(user, Operation.Write, "Config:")

        body = request.json

        try:
            compatibility_level = CompatibilityModes(request.json["compatibility"])
        except (ValueError, KeyError):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": SchemaErrorMessages.INVALID_COMPATIBILITY_LEVEL.value,
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            self.schema_registry.send_config_message(compatibility_level=compatibility_level, subject=None)
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/config"
            await self._forward_request_remote(request=request, body=body, url=url, content_type=content_type, method="PUT")

        self.r({"compatibility": self.schema_registry.schema_reader.config["compatibility"]}, content_type)

    async def config_subject_get(
        self, content_type: str, subject: str, *, request: HTTPRequest, user: User | None = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        # Config for a subject can exist without schemas so no need to check for their existence
        assert self.schema_registry.schema_reader, "KarapaceSchemaRegistry not initialized. Missing call to _init"
        if self.schema_registry.database.find_subject(subject=subject) is None:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        compatibility = self.schema_registry.database.get_subject_compatibility(subject=subject)
        default_to_global = request.query.get("defaultToGlobal", "false").lower() == "true"
        if not compatibility and default_to_global:
            compatibility = self.schema_registry.compatibility
        if compatibility:
            # Note: The format sent by the user differs from the return
            # value, this is for compatibility reasons.
            self.r(
                {"compatibilityLevel": compatibility},
                content_type,
            )

        self.r(
            body={
                "error_code": SchemaErrorCodes.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE.value,
                "message": SchemaErrorMessages.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_FMT.value.format(subject=subject),
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def config_subject_set(
        self,
        content_type: str,
        *,
        subject: str,
        request: HTTPRequest,
        user: User | None = None,
    ) -> None:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        try:
            compatibility_level = CompatibilityModes(request.json["compatibility"])
        except (ValueError, KeyError):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": "Invalid compatibility level",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            self.schema_registry.send_config_message(compatibility_level=compatibility_level, subject=subject)
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/config/{subject}"
            await self._forward_request_remote(
                request=request, body=request.json, url=url, content_type=content_type, method="PUT"
            )

        self.r({"compatibility": compatibility_level.value}, content_type)

    async def config_subject_delete(
        self,
        content_type,
        *,
        subject: str,
        request: HTTPRequest,
        user: User | None = None,
    ) -> None:
        if self._auth:
            if not self._auth.check_authorization(user, Operation.Write, f"Subject:{subject}"):
                self.r(body={"message": "Forbidden"}, content_type=JSON_CONTENT_TYPE, status=HTTPStatus.FORBIDDEN)

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            self.schema_registry.send_config_subject_delete_message(subject=subject)
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/config/{subject}"
            await self._forward_request_remote(
                request=request, body=request.json, url=url, content_type=content_type, method="PUT"
            )

        self.r({"compatibility": self.schema_registry.schema_reader.config["compatibility"]}, content_type)

    async def subjects_list(self, content_type: str, *, request: HTTPRequest, user: User | None = None) -> None:
        deleted = request.query.get("deleted", "false").lower() == "true"
        subjects = self.schema_registry.database.find_subjects(include_deleted=deleted)
        if self._auth is not None:
            subjects = list(
                filter(
                    lambda subject: self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"),
                    subjects,
                )
            )
        self.r(subjects, content_type, status=HTTPStatus.OK)

    async def subject_delete(
        self, content_type: str, *, subject: str, request: HTTPRequest, user: User | None = None
    ) -> None:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        permanent = request.query.get("permanent", "false").lower() == "true"

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            try:
                version_list = await self.schema_registry.subject_delete_local(subject=subject, permanent=permanent)
                self.r([version.value for version in version_list], content_type, status=HTTPStatus.OK)
            except (SubjectNotFoundException, SchemasNotFoundException):
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                        "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )
            except SubjectNotSoftDeletedException:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SUBJECT_NOT_SOFT_DELETED.value,
                        "message": f"Subject '{subject}' was not deleted first before being permanently deleted",
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )
            except SubjectSoftDeletedException:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SUBJECT_SOFT_DELETED.value,
                        "message": f"Subject '{subject}' was soft deleted.Set permanent=true to delete permanently",
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )

            except ReferenceExistsException as arg:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.REFERENCE_EXISTS.value,
                        "message": (
                            f"One or more references exist to the schema "
                            f"{{magic=1,keytype=SCHEMA,subject={subject},version={arg.version}}}."
                        ),
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}?permanent={permanent}"
            await self._forward_request_remote(request=request, body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_get(
        self, content_type: str, *, subject: str, version: str, request: HTTPRequest, user: User | None = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        deleted = request.query.get("deleted", "false").lower() == "true"
        try:
            subject_data = self.schema_registry.subject_version_get(subject, Versioner.V(version), include_deleted=deleted)
            if "compatibility" in subject_data:
                del subject_data["compatibility"]
            self.r(subject_data, content_type)
        except (SubjectNotFoundException, SchemasNotFoundException):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        except VersionNotFoundException:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        except InvalidVersion:
            self._invalid_version(content_type, version)

    async def subject_version_delete(
        self, content_type: str, *, subject: str, version: str, request: HTTPRequest, user: User | None = None
    ) -> None:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")
        permanent = request.query.get("permanent", "false").lower() == "true"

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            try:
                resolved_version = await self.schema_registry.subject_version_delete_local(
                    subject, Versioner.V(version), permanent
                )
                self.r(str(resolved_version), content_type, status=HTTPStatus.OK)
            except (SubjectNotFoundException, SchemasNotFoundException):
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                        "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )
            except VersionNotFoundException:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                        "message": f"Version {version} not found.",
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )
            except SchemaVersionSoftDeletedException:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SCHEMAVERSION_SOFT_DELETED.value,
                        "message": (
                            f"Subject '{subject}' Version {version} was soft deleted. "
                            "Set permanent=true to delete permanently"
                        ),
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )
            except SchemaVersionNotSoftDeletedException:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SCHEMAVERSION_NOT_SOFT_DELETED.value,
                        "message": (
                            f"Subject '{subject}' Version {version} was not deleted "
                            "first before being permanently deleted"
                        ),
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )
            except ReferenceExistsException as arg:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.REFERENCE_EXISTS.value,
                        "message": (
                            f"One or more references exist to the schema "
                            f"{{magic=1,keytype=SCHEMA,subject={subject},version={arg.version}}}."
                        ),
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )
            except InvalidVersion:
                self._invalid_version(content_type, version)
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}/versions/{version}?permanent={permanent}"
            await self._forward_request_remote(request=request, body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_schema_get(
        self, content_type: str, *, subject: str, version: str, user: User | None = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        try:
            subject_data = self.schema_registry.subject_version_get(subject, Versioner.V(version))
            self.r(subject_data["schema"], content_type)
        except InvalidVersion:
            self._invalid_version(content_type, version)
        except VersionNotFoundException:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        except (SchemasNotFoundException, SubjectNotFoundException):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

    async def subject_version_referencedby_get(self, content_type, *, subject, version, user: User | None = None):
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        try:
            referenced_by = await self.schema_registry.subject_version_referencedby_get(subject, Versioner.V(version))
        except (SubjectNotFoundException, SchemasNotFoundException):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        except VersionNotFoundException:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        except InvalidVersion:
            self._invalid_version(content_type, version)

        self.r(referenced_by, content_type, status=HTTPStatus.OK)

    async def subject_versions_list(
        self, content_type: str, *, subject: str, request: HTTPRequest, user: User | None = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")
        deleted = request.query.get("deleted", "false").lower() == "true"
        try:
            schema_versions = self.schema_registry.subject_get(subject, include_deleted=deleted)
            version_list = [version.value for version in schema_versions]
            self.r(version_list, content_type, status=HTTPStatus.OK)
        except (SubjectNotFoundException, SchemasNotFoundException):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

    def _validate_subject(self, content_type: str, subject: str) -> None:
        """Subject may not contain control characters."""
        if bool([c for c in subject if (ord(c) <= 31 or (ord(c) >= 127 and ord(c) <= 159))]):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_SUBJECT.value,
                    "message": f"The specified subject '{subject}' is not a valid.",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
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
            if field not in {"schema", "schemaType", "references"}:
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

    def _validate_schema_key(self, content_type: str, body: dict) -> None:
        if "schema" not in body:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": "Empty schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

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
        self, content_type: str, *, subject: str, request: HTTPRequest, user: User | None = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        body = request.json
        self._validate_schema_request_body(content_type, body)
        deleted = request.query.get("deleted", "false").lower() == "true"
        try:
            subject_data = self._subject_get(subject, content_type, include_deleted=deleted)
        except (SchemasNotFoundException, SubjectNotFoundException):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        new_schema = None
        if "schema" not in body:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": f"Error while looking up schema under subject {subject}",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        schema_str = body["schema"]
        schema_type = self._validate_schema_type(content_type=content_type, data=body)
        references = self._validate_references(content_type, schema_type, body)
        references, new_schema_dependencies = self.schema_registry.resolve_references(references)
        normalize = request.query.get("normalize", "false").lower() == "true"
        try:
            # When checking if schema is already registered, allow unvalidated schema in as
            # there might be stored schemas that are non-compliant from the past.
            new_schema = ParsedTypedSchema.parse(
                schema_type=schema_type,
                schema_str=schema_str,
                references=references,
                dependencies=new_schema_dependencies,
                normalize=normalize,
                use_protobuf_formatter=self.config["use_protobuf_formatter"],
            )
        except InvalidSchema:
            self.log.warning("Invalid schema: %r", schema_str)
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Error while looking up schema under subject {subject}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        except InvalidReferences:
            human_error = "Provided references is not valid"
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_type} references. Error: {human_error}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
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
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                        "message": f"Error while looking up schema under subject {subject}",
                    },
                    content_type=content_type,
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            if schema_type is SchemaType.JSONSCHEMA:
                schema_valid = parsed_typed_schema.to_dict() == new_schema.to_dict()
            else:
                schema_valid = new_schema.match(parsed_typed_schema)
            if parsed_typed_schema.schema_type == new_schema.schema_type and schema_valid:
                ret = {
                    "subject": subject,
                    "version": schema_version.version.value,
                    "id": schema_version.schema_id,
                    "schema": parsed_typed_schema.schema_str,
                }
                if schema_type is not SchemaType.AVRO:
                    ret["schemaType"] = schema_type
                self.r(ret, content_type)
            else:
                self.log.debug("Schema %r did not match %r", schema_version, parsed_typed_schema)

        self.r(
            body={
                "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                "message": "Schema not found",
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def subject_post(
        self,
        content_type: str,
        *,
        subject: str,
        request: HTTPRequest,
        user: User | None = None,
    ) -> None:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        body = request.json
        self.log.debug("POST with subject: %r, request: %r", subject, body)
        self._validate_subject(content_type, subject)
        self._validate_schema_request_body(content_type, body)
        schema_type = self._validate_schema_type(content_type, body)
        self._validate_schema_key(content_type, body)
        normalize = request.query.get("normalize", "false").lower() == "true"
        references = self._validate_references(content_type, schema_type, body)

        try:
            references, resolved_dependencies = self.schema_registry.resolve_references(references)
            new_schema = ValidatedTypedSchema.parse(
                schema_type=schema_type,
                schema_str=body["schema"],
                references=references,
                dependencies=resolved_dependencies,
                normalize=normalize,
                use_protobuf_formatter=self.config["use_protobuf_formatter"],
            )
        except (InvalidReferences, InvalidSchema, InvalidSchemaType) as e:
            self.log.warning("Invalid schema: %r", body["schema"], exc_info=True)
            if isinstance(e.__cause__, (SchemaParseException, JSONDecodeError, ProtobufUnresolvedDependencyException)):
                human_error = f"{e.__cause__.args[0]}"  # pylint: disable=no-member
            else:
                from_body_schema_str = body["schema"]
                human_error = f"Invalid schema {from_body_schema_str} with refs {references} of type {schema_type}"
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_type.value} schema. Error: {human_error}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        schema_id = self.get_schema_id_if_exists(subject=subject, schema=new_schema, include_deleted=False)
        if schema_id is not None:
            self.r({"id": schema_id}, content_type)

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            try:
                schema_id = await self.schema_registry.write_new_schema_local(subject, new_schema, references)
                self.r(
                    body={"id": schema_id},
                    content_type=content_type,
                )
            except InvalidSchema as ex:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                        "message": f"Invalid {schema_type.value} schema. Error: {str(ex)}",
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )
            except IncompatibleSchema as ex:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.HTTP_CONFLICT.value,
                        "message": str(ex),
                    },
                    content_type=content_type,
                    status=HTTPStatus.CONFLICT,
                )
            except SchemaTooLargeException:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SCHEMA_TOO_LARGE_ERROR_CODE.value,
                        "message": "Schema is too large",
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )
            except Exception as xx:
                raise xx

        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}/versions"
            await self._forward_request_remote(request=request, body=body, url=url, content_type=content_type, method="POST")

    async def get_global_mode(
        self,
        content_type: str,
        *,
        user: User | None = None,
    ) -> None:
        self._check_authorization(user, Operation.Read, "Config:")
        self.r(
            body={"mode": str(self.schema_registry.get_global_mode())},
            content_type=content_type,
            status=HTTPStatus.OK,
        )

    async def get_subject_mode(
        self,
        content_type: str,
        *,
        subject: str,
        user: User | None = None,
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        if self.schema_registry.database.find_subject(subject=Subject(subject)) is None:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        self.r(
            body={"mode": str(self.schema_registry.get_global_mode())},
            content_type=content_type,
            status=HTTPStatus.OK,
        )

    def get_schema_id_if_exists(self, *, subject: str, schema: TypedSchema, include_deleted: bool) -> SchemaId | None:
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

        with async_timeout.timeout(timeout):
            async with func(url, headers=headers, json=body) as response:
                if response.headers.get("content-type", "").startswith(JSON_CONTENT_TYPE):
                    resp_content = await response.json()
                else:
                    resp_content = await response.text()

        self.r(body=resp_content, content_type=content_type, status=HTTPStatus(response.status))

    def no_master_error(self, content_type: str) -> None:
        self.r(
            body={
                "error_code": SchemaErrorCodes.NO_MASTER_ERROR.value,
                "message": "Error while forwarding the request to the master.",
            },
            content_type=content_type,
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
