"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from avro.errors import SchemaParseException
from dependency_injector.wiring import inject, Provide
from fastapi import Depends, HTTPException, Request, status
from karapace.api.forward_client import ForwardClient
from karapace.api.routers.errors import no_primary_url_error, SchemaErrorCodes, SchemaErrorMessages
from karapace.api.routers.requests import (
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
from karapace.core.auth import AuthenticatorAndAuthorizer, Operation, User
from karapace.core.compatibility import CompatibilityModes
from karapace.core.compatibility.jsonschema.checks import is_incompatible
from karapace.core.compatibility.schema_compatibility import SchemaCompatibility
from karapace.core.config import Config
from karapace.core.container import KarapaceContainer
from karapace.core.errors import (
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
from karapace.core.protobuf.exception import ProtobufUnresolvedDependencyException
from karapace.core.schema_models import (
    ParsedTypedSchema,
    SchemaType,
    SchemaVersion,
    TypedSchema,
    ValidatedTypedSchema,
    Versioner,
)
from karapace.core.schema_references import LatestVersionReference, Reference
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.statsd import StatsClient
from karapace.core.typing import JsonData, JsonObject, SchemaId, Subject, Version
from karapace.core.utils import JSONDecodeError
from typing import Any, cast

import json
import logging
import time

LOG = logging.getLogger(__name__)


class KarapaceSchemaRegistryController:
    def __init__(self, config: Config, schema_registry: KarapaceSchemaRegistry, stats: StatsClient) -> None:
        # super().__init__(config=config, not_ready_handler=self._forward_if_not_ready_to_serve)

        self.config = config
        self._process_start_time = time.monotonic()
        self.stats = stats
        self.schema_registry = schema_registry

    def _add_schema_registry_routes(self) -> None:
        pass

    def _subject_get(self, subject: Subject, include_deleted: bool = False) -> dict[Version, SchemaVersion]:
        try:
            schema_versions = self.schema_registry.subject_get(subject, include_deleted)
        except SubjectNotFoundException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc
        except SchemasNotFoundException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc
        return schema_versions

    def _invalid_version(self, version: str | int) -> HTTPException:
        """Shall be called when InvalidVersion is raised"""
        return HTTPException(
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
        self,
        *,
        subject: Subject,
        schema_request: SchemaRequest,
        version: str,
    ) -> CompatibilityCheckResponse:
        """Check for schema compatibility"""
        try:
            compatibility_mode = self.schema_registry.get_compatibility_mode(subject=subject)
        except ValueError as exc:
            # Using INTERNAL_SERVER_ERROR because the subject and configuration
            # should have been validated before.
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": str(exc),
                },
            ) from exc

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

    @inject
    async def schemas_list(
        self,
        *,
        deleted: bool,
        latest_only: bool,
        user: User | None,
        authorizer: AuthenticatorAndAuthorizer = Depends(Provide[KarapaceContainer.authorizer]),
    ) -> list[SchemaListingItem]:
        schemas = await self.schema_registry.schemas_list(include_deleted=deleted, latest_only=latest_only)
        response_schemas: list[SchemaListingItem] = []
        for subject, schema_versions in schemas.items():
            if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
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

    @inject
    async def schemas_get(
        self,
        *,
        schema_id: str,
        fetch_max_id: bool,
        include_subjects: bool,
        format_serialized: str,
        user: User | None,
        authorizer: AuthenticatorAndAuthorizer = Depends(Provide[KarapaceContainer.authorizer]),
    ) -> SchemasResponse:
        try:
            parsed_schema_id = SchemaId(int(schema_id))
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
            ) from exc

        def _has_subject_with_id() -> bool:
            # Fast path
            if authorizer is None or authorizer.check_authorization(user, Operation.Read, "Subject:*"):
                return True

            subjects = self.schema_registry.database.subjects_for_schema(schema_id=parsed_schema_id)
            resources = [f"Subject:{subject}" for subject in subjects]
            return authorizer.check_authorization_any(user=user, operation=Operation.Read, resources=resources)

        if authorizer:
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
            schemaType=schema_type,
            references=references,
            maxId=maxId,
        )

    @inject
    async def schemas_get_versions(
        self,
        *,
        schema_id: str,
        deleted: bool,
        user: User | None,
        authorizer: AuthenticatorAndAuthorizer = Depends(Provide[KarapaceContainer.authorizer]),
    ) -> list[SubjectVersion]:
        try:
            schema_id_int = SchemaId(int(schema_id))
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
            ) from exc

        subject_versions = []
        for subject_version in self.schema_registry.get_subject_versions_for_schema(schema_id_int, include_deleted=deleted):
            subject = subject_version["subject"]
            if authorizer and not authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"):
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

    async def config_get(self) -> CompatibilityLevelResponse:
        # Note: The format sent by the user differs from the return value, this
        # is for compatibility reasons.
        return CompatibilityLevelResponse(compatibilityLevel=self.schema_registry.schema_reader.config.compatibility)

    async def config_set(
        self,
        *,
        compatibility_level_request: CompatibilityRequest,
    ) -> CompatibilityResponse:
        try:
            compatibility_level = CompatibilityModes(compatibility_level_request.compatibility)
        except (ValueError, KeyError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": SchemaErrorMessages.INVALID_COMPATIBILITY_LEVEL.value,
                },
            ) from exc

        self.schema_registry.send_config_message(compatibility_level=compatibility_level, subject=None)
        return CompatibilityResponse(compatibility=self.schema_registry.schema_reader.config.compatibility)

    async def config_subject_get(
        self,
        *,
        subject: str,
        default_to_global: bool,
    ) -> CompatibilityLevelResponse:
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
        *,
        subject: str,
        compatibility_level_request: CompatibilityRequest,
    ) -> CompatibilityResponse:
        try:
            compatibility_level = CompatibilityModes(compatibility_level_request.compatibility)
        except (ValueError, KeyError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": "Invalid compatibility level",
                },
            ) from exc

        self.schema_registry.send_config_message(compatibility_level=compatibility_level, subject=Subject(subject))
        return CompatibilityResponse(compatibility=compatibility_level.value)

    async def config_subject_delete(
        self,
        *,
        subject: str,
    ) -> CompatibilityResponse:
        self.schema_registry.send_config_subject_delete_message(subject=Subject(subject))
        return CompatibilityResponse(compatibility=self.schema_registry.schema_reader.config.compatibility)

    @inject
    async def subjects_list(
        self,
        deleted: bool,
        user: User | None,
        authorizer: AuthenticatorAndAuthorizer = Depends(Provide[KarapaceContainer.authorizer]),
    ) -> list[str]:
        subjects = [str(subject) for subject in self.schema_registry.database.find_subjects(include_deleted=deleted)]
        if authorizer:
            subjects = list(
                filter(
                    lambda subject: authorizer.check_authorization(user, Operation.Read, f"Subject:{subject}"),
                    subjects,
                )
            )
        return subjects

    async def subject_delete(
        self,
        *,
        subject: str,
        permanent: bool,
    ) -> list[int]:
        try:
            version_list = await self.schema_registry.subject_delete_local(subject=Subject(subject), permanent=permanent)
            return [version.value for version in version_list]
        except (SubjectNotFoundException, SchemasNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc
        except SubjectNotSoftDeletedException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_SOFT_DELETED.value,
                    "message": f"Subject '{subject}' was not deleted first before being permanently deleted",
                },
            ) from exc
        except SubjectSoftDeletedException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_SOFT_DELETED.value,
                    "message": f"Subject '{subject}' was soft deleted.Set permanent=true to delete permanently",
                },
            ) from exc

        except ReferenceExistsException as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.REFERENCE_EXISTS.value,
                    "message": (
                        f"One or more references exist to the schema "
                        f"{{magic=1,keytype=SCHEMA,subject={subject},version={exc.version}}}."
                    ),
                },
            ) from exc

    async def subject_version_get(
        self,
        subject: str,
        version: str,
        deleted: bool,
    ) -> SubjectSchemaVersionResponse:
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
                schemaType=subject_data.get("schemaType", None),
                compatibility=None,  # Do not return compatibility from this endpoint.
            )
        except (SubjectNotFoundException, SchemasNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc
        except VersionNotFoundException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            ) from exc
        except InvalidVersion as exc:
            raise self._invalid_version(version) from exc

    async def subject_version_delete(
        self,
        *,
        subject: str,
        version: str,
        permanent: bool,
    ) -> int:
        try:
            resolved_version = await self.schema_registry.subject_version_delete_local(
                Subject(subject), Versioner.V(version), permanent
            )
            return resolved_version.value
        except (SubjectNotFoundException, SchemasNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc
        except VersionNotFoundException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            ) from exc
        except SchemaVersionSoftDeletedException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SCHEMAVERSION_SOFT_DELETED.value,
                    "message": (
                        f"Subject '{subject}' Version {version} was soft deleted. Set permanent=true to delete permanently"
                    ),
                },
            ) from exc
        except SchemaVersionNotSoftDeletedException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SCHEMAVERSION_NOT_SOFT_DELETED.value,
                    "message": (
                        f"Subject '{subject}' Version {version} was not deleted first before being permanently deleted"
                    ),
                },
            ) from exc
        except ReferenceExistsException as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.REFERENCE_EXISTS.value,
                    "message": (
                        f"One or more references exist to the schema "
                        f"{{magic=1,keytype=SCHEMA,subject={subject},version={exc.version}}}."
                    ),
                },
            ) from exc
        except InvalidVersion as exc:
            raise self._invalid_version(version) from exc

    async def subject_version_schema_get(
        self,
        *,
        subject: str,
        version: str,
    ) -> dict:
        try:
            subject_data = self.schema_registry.subject_version_get(Subject(subject), Versioner.V(version))
            return json.loads(cast(str, subject_data["schema"]))  # TODO typing
        except InvalidVersion as exc:
            raise self._invalid_version(version) from exc
        except VersionNotFoundException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            ) from exc
        except (SchemasNotFoundException, SubjectNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc

    async def subject_version_referencedby_get(
        self,
        *,
        subject: str,
        version,
    ) -> list[int]:
        referenced_by: list[int] = []
        try:
            referenced_by = await self.schema_registry.subject_version_referencedby_get(
                Subject(subject), Versioner.V(version)
            )
        except (SubjectNotFoundException, SchemasNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc
        except VersionNotFoundException as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            ) from exc
        except InvalidVersion as exc:
            raise self._invalid_version(version) from exc

        return referenced_by

    async def subject_versions_list(
        self,
        *,
        subject: str,
        deleted: bool,
    ) -> list[int]:
        try:
            schema_versions = self.schema_registry.subject_get(Subject(subject), include_deleted=deleted)
            version_list = [version.value for version in schema_versions]
            return version_list
        except (SubjectNotFoundException, SchemasNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc

    def _validate_schema_type(self, data: JsonData) -> SchemaType:
        # TODO: simplify the calling code, this functionality should not be required
        # for old schemas.
        if not isinstance(data, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_BAD_REQUEST.value,
                    "message": "Malformed request",
                },
            )
        schema_type_unparsed = data.get("schemaType", SchemaType.AVRO.value)
        try:
            schema_type = SchemaType(schema_type_unparsed)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                    "message": f"Invalid schemaType {schema_type_unparsed}",
                },
            ) from exc
        return schema_type

    def _validate_references(
        self,
        schema_request: SchemaRequest,
    ) -> list[Reference | LatestVersionReference] | None:
        references = schema_request.references
        # Allow passing `null` as value for compatibility
        if references is None:
            return None
        if references and schema_request.schema_type != SchemaType.PROTOBUF:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.REFERENCES_SUPPORT_NOT_IMPLEMENTED.value,
                    "message": SchemaErrorMessages.REFERENCES_SUPPORT_NOT_IMPLEMENTED.value.format(
                        schema_type=schema_request.schema_type.value
                    ),
                },
            )

        validated_references = []
        for reference in references:
            version = Versioner.V(reference.version)
            if version.is_latest:
                validated_references.append(
                    LatestVersionReference(
                        name=reference.name,
                        subject=Subject(reference.subject),
                    )
                )
            else:
                validated_references.append(
                    Reference(
                        name=reference.name,
                        subject=Subject(reference.subject),
                        version=version,
                    )
                )
        if validated_references:
            return validated_references
        return None

    async def subjects_schema_post(
        self,
        *,
        subject: Subject,
        schema_request: SchemaRequest,
        deleted: bool,
        normalize: bool,
    ) -> SchemaResponse:
        try:
            subject_data = self._subject_get(subject, include_deleted=deleted)
        except (SchemasNotFoundException, SubjectNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": SchemaErrorMessages.SUBJECT_NOT_FOUND_FMT.value.format(subject=subject),
                },
            ) from exc
        references = None
        new_schema_dependencies = None
        references = self._validate_references(schema_request)
        references, new_schema_dependencies = self.schema_registry.resolve_references(references)

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
        except InvalidSchema as exc:
            LOG.warning("Invalid schema: %r", schema_request.schema_str)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Error while looking up schema under subject {subject}",
                },
            ) from exc
        except InvalidReferences as exc:
            human_error = "Provided references is not valid"
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_request.schema_type} references. Error: {human_error}",
                },
            ) from exc

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
            except InvalidSchema as exc:
                failed_schema_id = schema_version.schema_id
                LOG.exception("Existing schema failed to parse. Id: %s", failed_schema_id)
                self.stats.unexpected_exception(
                    ex=exc, where="Matching existing schemas to posted. Failed schema id: {failed_schema_id}"
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={
                        "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                        "message": f"Error while looking up schema under subject {subject}",
                    },
                ) from exc

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
            LOG.debug("Schema %r did not match %r", schema_version, parsed_typed_schema)

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
        forward_client: ForwardClient,
        request: Request,
    ) -> SchemaIdResponse:
        LOG.debug("POST with subject: %r, request: %r", subject, schema_request)

        references = self._validate_references(schema_request=schema_request)

        try:
            references, resolved_dependencies = self.schema_registry.resolve_references(references)
            new_schema = ValidatedTypedSchema.parse(
                schema_type=schema_request.schema_type,
                schema_str=schema_request.schema_str,
                references=references,
                dependencies=resolved_dependencies,
                normalize=normalize,
                use_protobuf_formatter=self.config.use_protobuf_formatter,
            )
        except (InvalidReferences, InvalidSchema, InvalidSchemaType) as exc:
            LOG.warning("Invalid schema: %r", schema_request.schema_str, exc_info=True)
            if isinstance(exc.__cause__, (SchemaParseException, JSONDecodeError, ProtobufUnresolvedDependencyException)):
                human_error = f"{exc.__cause__.args[0]}"
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
            ) from exc

        schema_id = self.get_schema_id_if_exists(subject=Subject(subject), schema=new_schema, include_deleted=False)
        if schema_id is not None:
            return SchemaIdResponse(id=schema_id)

        primary_info = await self.schema_registry.get_master()
        if primary_info.primary:
            try:
                schema_id = await self.schema_registry.write_new_schema_local(Subject(subject), new_schema, references)
                return SchemaIdResponse(id=schema_id)
            except InvalidSchema as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                        "message": f"Invalid {schema_request.schema_type.value} schema. Error: {str(exc)}",
                    },
                ) from exc
            except IncompatibleSchema as exc:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error_code": SchemaErrorCodes.HTTP_CONFLICT.value,
                        "message": str(exc),
                    },
                ) from exc
            except SchemaTooLargeException as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "error_code": SchemaErrorCodes.SCHEMA_TOO_LARGE_ERROR_CODE.value,
                        "message": "Schema is too large",
                    },
                ) from exc
            except Exception as xx:
                raise xx

        if not primary_info.primary_url:
            raise no_primary_url_error()
        else:
            return await forward_client.forward_request_remote(
                request=request, primary_url=primary_info.primary_url, response_type=SchemaIdResponse
            )

    async def get_global_mode(self) -> ModeResponse:
        return ModeResponse(mode=str(self.schema_registry.get_global_mode()))

    async def get_subject_mode(
        self,
        *,
        subject: str,
    ) -> ModeResponse:
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

    def get_new_schema(self, schema_request: SchemaRequest) -> ValidatedTypedSchema:
        references = self._validate_references(schema_request=schema_request)
        try:
            references, new_schema_dependencies = self.schema_registry.resolve_references(references)
            return ValidatedTypedSchema.parse(
                schema_type=schema_request.schema_type,
                schema_str=schema_request.schema_str,
                references=references,
                dependencies=new_schema_dependencies,
                use_protobuf_formatter=self.config.use_protobuf_formatter,
            )
        except InvalidSchema as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Invalid {schema_request.schema_type} schema",
                },
            ) from exc

    def get_old_schema(self, subject: Subject, version: Version) -> ParsedTypedSchema:
        old: JsonObject | None = None
        try:
            old = self.schema_registry.subject_version_get(subject=subject, version=version)
        except InvalidVersion:
            self._invalid_version(version.value)
        except (VersionNotFoundException, SchemasNotFoundException, SubjectNotFoundException) as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
            ) from exc
        assert old is not None
        old_schema_type = self._validate_schema_type(data=old)
        try:
            old_references = old.get("references", None)
            old_dependencies = None
            if old_references:
                old_references, old_dependencies = self.schema_registry.resolve_references(old_references)
            old_schema = ParsedTypedSchema.parse(old_schema_type, old["schema"], old_references, old_dependencies)
            return old_schema
        except InvalidSchema as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": SchemaErrorCodes.INVALID_SCHEMA.value,
                    "message": f"Found an invalid {old_schema_type} schema registered",
                },
            ) from exc
