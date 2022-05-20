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
    InvalidSchema,
    InvalidSchemaType,
    InvalidVersion,
    SchemasNotFoundException,
    SchemaVersionNotSoftDeletedException,
    SchemaVersionSoftDeletedException,
    SubjectNotFoundException,
    SubjectNotSoftDeletedException,
    VersionNotFoundException,
)
from karapace.karapace import KarapaceBase
from karapace.rapu import HTTPRequest, JSON_CONTENT_TYPE, SERVER_NAME
from karapace.schema_models import SchemaType, TypedSchema, ValidatedTypedSchema
from karapace.schema_registry import KarapaceSchemaRegistry
from karapace.typing import JsonData
from typing import Any, Dict, Optional, Union

import aiohttp
import async_timeout
import json


@unique
class SchemaErrorCodes(Enum):
    EMPTY_SCHEMA = 42201
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
    INVALID_VERSION_ID = 42202
    INVALID_COMPATIBILITY_LEVEL = 42203
    INVALID_AVRO_SCHEMA = 44201
    INVALID_SUBJECT = 42208
    NO_MASTER_ERROR = 50003


@unique
class SchemaErrorMessages(Enum):
    SUBJECT_NOT_FOUND_FMT = "Subject '{subject}' not found."
    INVALID_COMPATIBILITY_LEVEL = (
        "Invalid compatibility level. Valid values are none, backward, "
        "forward, full, backward_transitive, forward_transitive, and "
        "full_transitive"
    )


class KarapaceSchemaRegistryController(KarapaceBase):
    def __init__(self, config: Config) -> None:
        super().__init__(config=config)

        self._auth: Optional[HTTPAuthorizer] = None
        if self.config["registry_authfile"] is not None:
            self._auth = HTTPAuthorizer(str(self.config["registry_authfile"]))
            self.app.on_startup.append(self._start_authorizer)

        self.schema_registry = KarapaceSchemaRegistry(config)
        self._add_schema_registry_routes()
        self.schema_registry.start()

        self._forward_client = None
        self.app.on_startup.append(self._create_forward_client)

    async def _create_forward_client(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
        """Callback for aiohttp.Application.on_startup"""
        self._forward_client = aiohttp.ClientSession(headers={"User-Agent": SERVER_NAME})

    async def _start_authorizer(self, app: aiohttp.web.Application) -> None:  # pylint: disable=unused-argument
        """Callback for aiohttp.Application.on_startup"""
        await self._auth.start_refresh_task(self.stats)

    def _check_authorization(self, user: Optional[User], operation: Operation, resource: str) -> None:
        if self._auth:
            if not self._auth.check_authorization(user, operation, resource):
                self.r(body={"message": "Forbidden"}, content_type=JSON_CONTENT_TYPE, status=HTTPStatus.FORBIDDEN)

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
            "/schemas/ids/<schema_id:path>/versions",
            callback=self.schemas_get_versions,
            method="GET",
            schema_request=True,
            auth=self._auth,
        )
        self.route(
            "/schemas/ids/<schema_id:path>",
            callback=self.schemas_get,
            method="GET",
            schema_request=True,
            auth=self._auth,
        )
        self.route("/schemas/types", callback=self.schemas_types, method="GET", schema_request=True, auth=None)
        self.route(
            "/subjects",
            callback=self.subjects_list,
            method="GET",
            schema_request=True,
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
            auth=self._auth,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>",
            callback=self.subject_version_get,
            method="GET",
            schema_request=True,
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
            "/subjects/<subject:path>",
            callback=self.subject_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
            auth=self._auth,
        )

    async def close(self) -> None:
        async with AsyncExitStack() as stack:
            stack.push_async_callback(super().close)
            stack.push_async_callback(self.schema_registry.close)
            if self._forward_client:
                stack.push_async_callback(self._forward_client.close)
            if self._auth is not None:
                stack.push_async_callback(self._auth.close)

    def _subject_get(self, subject: str, content_type: str, include_deleted: bool = False) -> Dict[str, Any]:
        subject_data = self.schema_registry.subject_get(subject, include_deleted)
        if not subject_data:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        schemas = self.schema_registry.get_schemas(subject, include_deleted=include_deleted)
        if not schemas:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        subject_data = subject_data.copy()
        subject_data["schemas"] = schemas
        return subject_data

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
        self, content_type: str, *, subject: str, version: str, request: HTTPRequest, user: Optional[User] = None
    ) -> None:
        """Check for schema compatibility"""

        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        body = request.json
        schema_type = self._validate_schema_type(content_type=content_type, data=body)
        try:
            new_schema = ValidatedTypedSchema.parse(schema_type, body["schema"])
        except InvalidSchema:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                    "message": f"Invalid {schema_type} schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        try:
            old = await self.schema_registry.subject_version_get(subject=subject, version=version)
        except InvalidVersion:
            self._invalid_version(content_type, version)
        except SubjectNotFoundException:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        except (VersionNotFoundException, SchemasNotFoundException):
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
            old_schema = ValidatedTypedSchema.parse(old_schema_type, old["schema"])
        except InvalidSchema:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                    "message": f"Found an invalid {old_schema_type} schema registered",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        try:
            compatibility_mode = self.schema_registry.get_compatibility_mode(subject=old)
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

    async def schemas_get(self, content_type: str, *, user: Optional[User] = None, schema_id: str) -> None:
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
        schema = self.schema_registry.schemas_get(schema_id_int)

        def _has_subject_with_id() -> bool:
            with self.schema_registry.schema_reader.id_lock:
                for subject, val in self.schema_registry.subjects.items():
                    if "schemas" not in val:
                        continue
                    for schema in val["schemas"].values():
                        if (
                            int(schema["id"]) == schema_id_int
                            and not schema["deleted"]
                            and self._auth is not None
                            and self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}")
                        ):
                            return True
            return False

        if self._auth:
            if not _has_subject_with_id():
                schema = None

        if not schema:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                    "message": "Schema not found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        response_body = {"schema": schema.schema_str}
        if schema.schema_type is not SchemaType.AVRO:
            response_body["schemaType"] = schema.schema_type
        self.r(response_body, content_type)

    async def schemas_get_versions(self, content_type: str, *, schema_id: str, user: Optional[User] = None) -> None:
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

        subject_versions = []
        for subject_version in self.schema_registry.get_versions(schema_id_int):
            subject = subject_version["subject"]
            if self._auth and not self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"):
                continue
            subject_versions.append(subject_version)
        self.r(subject_versions, content_type)

    async def schemas_types(self, content_type: str) -> None:
        self.r(["JSON", "AVRO", "PROTOBUF"], content_type)

    async def config_get(self, content_type: str, *, user: Optional[User] = None) -> None:
        self._check_authorization(user, Operation.Read, "Config:")

        # Note: The format sent by the user differs from the return value, this
        # is for compatibility reasons.
        self.r({"compatibilityLevel": self.schema_registry.schema_reader.config["compatibility"]}, content_type)

    async def config_set(self, content_type: str, *, request: HTTPRequest, user: Optional[User] = None) -> None:
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
        self, content_type: str, subject: str, *, request: HTTPRequest, user: Optional[User] = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        # Config for a subject can exist without schemas so no need to check for their existence
        assert self.schema_registry.schema_reader, "KarapaceSchemaRegistry not initialized. Missing call to _init"
        subject_data = self.schema_registry.subjects.get(subject, {})

        if subject_data:
            default_to_global = request.query.get("defaultToGlobal", "false").lower() == "true"
            compatibility = subject_data.get("compatibility")
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
                "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def config_subject_set(
        self, content_type: str, *, request: HTTPRequest, user: Optional[User] = None, subject: str
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

    async def subjects_list(self, content_type: str, *, user: Optional[User] = None) -> None:
        subjects = self.schema_registry.subjects_list
        if self._auth is not None:
            subjects = list(
                filter(
                    lambda subject: self._auth.check_authorization(user, Operation.Read, f"Subject:{subject}"),
                    subjects,
                )
            )
        self.r(subjects, content_type, status=HTTPStatus.OK)

    async def subject_delete(
        self, content_type: str, *, subject: str, request: HTTPRequest, user: Optional[User] = None
    ) -> None:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        permanent = request.query.get("permanent", "false").lower() == "true"

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            try:
                version_list = await self.schema_registry.subject_delete_local(subject, permanent)
                self.r(version_list, content_type, status=HTTPStatus.OK)
            except SubjectNotSoftDeletedException:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.SUBJECT_NOT_SOFT_DELETED.value,
                        "message": f"Subject '{subject}' was not deleted first before being permanently deleted",
                    },
                    content_type=content_type,
                    status=HTTPStatus.NOT_FOUND,
                )
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}?permanent={permanent}"
            await self._forward_request_remote(request=request, body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_get(
        self, content_type: str, *, subject: str, version: str, user: Optional[User] = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        try:
            subject_data = await self.schema_registry.subject_version_get(subject, version)
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
        self, content_type: str, *, subject: str, version: str, request: HTTPRequest, user: Optional[User] = None
    ) -> None:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")
        version = int(version)
        permanent = request.query.get("permanent", "false").lower() == "true"

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            try:
                resolved_version = await self.schema_registry.subject_version_delete_local(subject, version, permanent)
                self.r(str(resolved_version), content_type, status=HTTPStatus.OK)
            except SubjectNotFoundException:
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
                            f"Subject '{subject}' Version 1 was soft deleted.Set permanent=true to delete permanently"
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
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}/versions/{version}?permanent={permanent}"
            await self._forward_request_remote(request=request, body={}, url=url, content_type=content_type, method="DELETE")

    async def subject_version_schema_get(
        self, content_type: str, *, subject: str, version: str, user: Optional[User] = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        try:
            subject_data = await self.schema_registry.subject_version_get(subject, version)
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
        except SubjectNotFoundException:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

    async def subject_versions_list(self, content_type: str, *, subject: str, user: Optional[User] = None) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")
        try:
            subject_data = self.schema_registry.subject_get(subject)
            schemas = list(subject_data["schemas"])
            self.r(schemas, content_type, status=HTTPStatus.OK)
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

    def _validate_schema_request_body(self, content_type: str, body: Union[dict, Any]) -> None:
        if not isinstance(body, dict):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": "Internal Server Error",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        for attr in body:
            if attr not in {"schema", "schemaType"}:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                        "message": f"Unrecognized field: {attr}",
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )

    def _validate_schema_type(self, content_type: str, data: JsonData) -> SchemaType:
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
                    "error_code": SchemaErrorCodes.EMPTY_SCHEMA.value,
                    "message": "Empty schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

    async def subjects_schema_post(
        self, content_type: str, *, subject: str, request: HTTPRequest, user: Optional[User] = None
    ) -> None:
        self._check_authorization(user, Operation.Read, f"Subject:{subject}")

        body = request.json
        self._validate_schema_request_body(content_type, body)
        try:
            subject_data = self._subject_get(subject, content_type)
        except SubjectNotFoundException:
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
        try:
            new_schema = ValidatedTypedSchema.parse(schema_type, schema_str)
        except InvalidSchema:
            self.log.exception("No proper parser found")
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": f"Error while looking up schema under subject {subject}",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        for schema in subject_data["schemas"].values():
            validated_typed_schema = ValidatedTypedSchema.parse(schema["schema"].schema_type, schema["schema"].schema_str)
            if schema_type is SchemaType.JSONSCHEMA:
                schema_valid = validated_typed_schema.to_dict() == new_schema.to_dict()
            else:
                schema_valid = validated_typed_schema.schema == new_schema.schema
            if validated_typed_schema.schema_type == new_schema.schema_type and schema_valid:
                ret = {
                    "subject": subject,
                    "version": schema["version"],
                    "id": schema["id"],
                    "schema": validated_typed_schema.schema_str,
                }
                if schema_type is not SchemaType.AVRO:
                    ret["schemaType"] = schema_type
                self.r(ret, content_type)
            else:
                self.log.debug("Schema %r did not match %r", schema, validated_typed_schema)
        self.r(
            body={
                "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                "message": "Schema not found",
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def subject_post(
        self, content_type: str, *, subject: str, request: HTTPRequest, user: Optional[User] = None
    ) -> None:
        self._check_authorization(user, Operation.Write, f"Subject:{subject}")

        body = request.json
        self.log.debug("POST with subject: %r, request: %r", subject, body)
        self._validate_subject(content_type, subject)
        self._validate_schema_request_body(content_type, body)
        schema_type = self._validate_schema_type(content_type, body)
        self._validate_schema_key(content_type, body)

        try:
            new_schema = ValidatedTypedSchema.parse(schema_type=schema_type, schema_str=body["schema"])
        except (InvalidSchema, InvalidSchemaType) as e:
            self.log.warning("Invalid schema: %r", body["schema"], exc_info=True)
            if isinstance(e.__cause__, (SchemaParseException, json.JSONDecodeError)):
                human_error = f"{e.__cause__.args[0]}"  # pylint: disable=no-member
            else:
                human_error = "Provided schema is not valid"
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                    "message": f"Invalid {schema_type} schema. Error: {human_error}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        schema_id = self.get_schema_id_if_exists(subject=subject, schema=new_schema)
        if schema_id is not None:
            self.r({"id": schema_id}, content_type)

        are_we_master, master_url = await self.schema_registry.get_master()
        if are_we_master:
            try:
                schema_id = await self.schema_registry.write_new_schema_local(subject, new_schema)
                self.r(
                    body={"id": schema_id},
                    content_type=content_type,
                )
            except InvalidSchema as ex:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                        "message": f"Invalid {schema_type} schema. Error: {str(ex)}",
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
        elif not master_url:
            self.no_master_error(content_type)
        else:
            url = f"{master_url}/subjects/{subject}/versions"
            await self._forward_request_remote(request=request, body=body, url=url, content_type=content_type, method="POST")

    def get_schema_id_if_exists(self, *, subject: str, schema: TypedSchema) -> Optional[int]:
        return self.schema_registry.schema_reader.get_schema_id_if_exists(subject=subject, schema=schema)

    async def _forward_request_remote(
        self, *, request: HTTPRequest, body: Optional[dict], url: str, content_type: str, method: str = "POST"
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
