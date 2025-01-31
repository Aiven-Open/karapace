"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from avro.compatibility import SchemaCompatibilityResult, SchemaCompatibilityType
from collections.abc import Sequence
from contextlib import AsyncExitStack, closing
from karapace.api.telemetry.tracer import Tracer
from karapace.core.compatibility import CompatibilityModes
from karapace.core.compatibility.jsonschema.checks import is_incompatible
from karapace.core.compatibility.schema_compatibility import SchemaCompatibility
from karapace.core.config import Config
from karapace.core.coordinator.master_coordinator import MasterCoordinator
from karapace.core.dependency import Dependency
from karapace.core.errors import (
    IncompatibleSchema,
    ReferenceExistsException,
    SchemasNotFoundException,
    SchemaVersionNotSoftDeletedException,
    SchemaVersionSoftDeletedException,
    SubjectNotFoundException,
    SubjectNotSoftDeletedException,
    SubjectSoftDeletedException,
    VersionNotFoundException,
)
from karapace.core.in_memory_database import InMemoryDatabase
from karapace.core.key_format import KeyFormatter
from karapace.core.messaging import KarapaceProducer
from karapace.core.offset_watcher import OffsetWatcher
from karapace.core.schema_models import (
    ParsedTypedSchema,
    SchemaType,
    SchemaVersion,
    TypedSchema,
    ValidatedTypedSchema,
    Versioner,
)
from karapace.core.schema_reader import KafkaSchemaReader
from karapace.core.schema_references import LatestVersionReference, Reference
from karapace.core.stats import StatsClient
from karapace.core.typing import JsonObject, Mode, PrimaryInfo, SchemaId, Subject, Version

import asyncio
import logging

LOG = logging.getLogger(__name__)


class KarapaceSchemaRegistry:
    def __init__(self, config: Config, stats: StatsClient) -> None:
        # TODO: compatibility was previously in mutable dict, fix the runtime config to be distinct from static config.
        self.config = config
        self._tracer = Tracer()
        self._key_formatter = KeyFormatter()

        offset_watcher = OffsetWatcher()
        self.producer = KarapaceProducer(
            config=self.config, offset_watcher=offset_watcher, key_formatter=self._key_formatter
        )

        self.mc = MasterCoordinator(config=self.config)
        self.database = InMemoryDatabase()
        self.schema_reader = KafkaSchemaReader(
            config=self.config,
            offset_watcher=offset_watcher,
            key_formatter=self._key_formatter,
            master_coordinator=self.mc,
            database=self.database,
            stats=stats,
        )
        self.mc.set_stoppper(self.schema_reader)

        self.schema_lock = asyncio.Lock()
        self._master_lock = asyncio.Lock()

    def subjects_list(self, include_deleted: bool = False) -> list[Subject]:
        return self.database.find_subjects(include_deleted=include_deleted)

    @property
    def compatibility(self) -> str:
        return str(self.config.compatibility)

    def get_schemas(self, subject: Subject, *, include_deleted: bool = False) -> list[SchemaVersion]:
        schema_versions = self.database.find_subject_schemas(subject=subject, include_deleted=include_deleted)
        return list(schema_versions.values())

    async def start(self) -> None:
        self.mc.start()
        self.schema_reader.start()
        self.producer.initialize_karapace_producer()

    async def close(self) -> None:
        async with AsyncExitStack() as stack:
            stack.push_async_callback(self.mc.close)
            stack.enter_context(closing(self.schema_reader))
            stack.enter_context(closing(self.producer))

    async def get_master(self) -> PrimaryInfo:
        """Resolve if current node is the primary and the primary node address.

        :return PrimaryInfo: returns the PrimaryInfo object with primary state and primary url.
        """
        async with self._master_lock:
            primary_info = self.mc.get_master_info()
            if (
                # If node is not primary and no known primary url
                not primary_info.primary
                and primary_info.primary_url is None
            ):
                LOG.warning("No master set: %r", primary_info)
            if self.schema_reader.ready() is False:
                LOG.info("Schema reader isn't ready yet: %r", self.schema_reader.ready)
                return PrimaryInfo(False, primary_url=primary_info.primary_url)
            return primary_info

    def get_compatibility_mode(self, subject: Subject) -> CompatibilityModes:
        compatibility = self.database.get_subject_compatibility(subject=subject)
        if compatibility is None:
            # If no subject compatiblity found, use global compatibility
            compatibility = self.config.compatibility
        try:
            compatibility_mode = CompatibilityModes(compatibility)
        except ValueError as e:
            raise ValueError(f"Unknown compatibility mode {compatibility}") from e
        return compatibility_mode

    async def schemas_list(self, *, include_deleted: bool, latest_only: bool) -> dict[Subject, list[SchemaVersion]]:
        async with self.schema_lock:
            schemas = self.database.find_schemas(include_deleted=include_deleted, latest_only=latest_only)
            return schemas

    def schemas_get(self, schema_id: SchemaId, *, fetch_max_id: bool = False) -> TypedSchema | None:
        try:
            schema = self.database.find_schema(schema_id=schema_id)
        except KeyError:
            return None

        if schema and fetch_max_id:
            schema.max_id = self.database.global_schema_id

        return schema

    async def subject_delete_local(self, subject: Subject, permanent: bool) -> list[Version]:
        async with self.schema_lock:
            schema_versions = self.subject_get(subject, include_deleted=True)

            # Subject can be permanently deleted if no schemas or all are soft deleted.
            can_permanent_delete = not bool(
                [version_id for version_id, schema_version in schema_versions.items() if not schema_version.deleted]
            )
            if permanent and not can_permanent_delete:
                raise SubjectNotSoftDeletedException()

            # Subject is soft deleted if all schemas in subject have deleted flag
            already_soft_deleted = len(schema_versions) == len(
                [version_id for version_id, schema_version in schema_versions.items() if schema_version.deleted]
            )
            if not permanent and already_soft_deleted:
                raise SubjectSoftDeletedException()

            if permanent:
                version_list = list(schema_versions)
                for version_id, schema_version in list(schema_versions.items()):
                    referenced_by = self.schema_reader.get_referenced_by(subject, schema_version.version)
                    if referenced_by and len(referenced_by) > 0:
                        raise ReferenceExistsException(referenced_by, version_id)

                for version_id, schema_version in list(schema_versions.items()):
                    LOG.info(
                        "Permanently deleting subject '%s' version %s (schema id=%s)",
                        subject,
                        version_id,
                        schema_version.schema_id,
                    )
                    self.send_schema_message(
                        subject=subject,
                        schema=None,
                        schema_id=schema_version.schema_id,
                        version=version_id,
                        deleted=True,
                        references=schema_version.references,
                    )
            else:
                try:
                    schema_versions_live = self.subject_get(subject, include_deleted=False)
                except SchemasNotFoundException:
                    latest_version_id = Versioner.V(Version.MINUS_1_VERSION_TAG)
                    version_list = []
                else:
                    version_list = list(schema_versions_live)
                    if version_list:
                        latest_version_id = version_list[-1]
                    else:
                        return []

                referenced_by = self.schema_reader.get_referenced_by(subject, latest_version_id)
                if referenced_by and len(referenced_by) > 0:
                    raise ReferenceExistsException(referenced_by, latest_version_id)
                self.send_delete_subject_message(subject, latest_version_id)

            return version_list

    async def subject_version_delete_local(self, subject: Subject, version: Version, permanent: bool) -> Version:
        async with self.schema_lock:
            schema_versions = self.subject_get(subject, include_deleted=True)
            if not permanent and version.is_latest:
                schema_versions = {
                    version_id: schema_version
                    for version_id, schema_version in schema_versions.items()
                    if schema_version.deleted is False
                }
            resolved_version = Versioner.from_schema_versions(schema_versions=schema_versions, version=version)
            schema_version = schema_versions.get(resolved_version, None)

            if not schema_version:
                raise VersionNotFoundException()
            if schema_version.deleted and not permanent:
                raise SchemaVersionSoftDeletedException()

            # Cannot directly hard delete
            if permanent and not schema_version.deleted:
                raise SchemaVersionNotSoftDeletedException()

            referenced_by = self.schema_reader.get_referenced_by(subject, resolved_version)
            if referenced_by and len(referenced_by) > 0:
                raise ReferenceExistsException(referenced_by, resolved_version)

            self.send_schema_message(
                subject=subject,
                schema=None if permanent else schema_version.schema,
                schema_id=schema_version.schema_id,
                version=resolved_version,
                deleted=True,
                references=schema_version.references,
            )
            return resolved_version

    def subject_get(self, subject: Subject, include_deleted: bool = False) -> dict[Version, SchemaVersion]:
        subject_found = self.database.find_subject(subject=subject)
        if not subject_found:
            raise SubjectNotFoundException()

        schemas = self.database.find_subject_schemas(subject=subject, include_deleted=include_deleted)
        if not schemas:
            raise SchemasNotFoundException
        return schemas

    def subject_version_get(self, subject: Subject, version: Version, *, include_deleted: bool = False) -> JsonObject:
        schema_versions = self.subject_get(subject, include_deleted=include_deleted)
        if not schema_versions:
            raise SubjectNotFoundException()
        resolved_version = Versioner.from_schema_versions(schema_versions=schema_versions, version=version)
        schema_data: SchemaVersion | None = schema_versions.get(resolved_version, None)

        if not schema_data:
            raise VersionNotFoundException()
        schema_id = schema_data.schema_id
        schema = schema_data.schema

        ret: JsonObject = {
            "subject": subject,
            "version": resolved_version.value,
            "id": schema_id,
            "schema": schema.schema_str,
        }
        if schema.references is not None:
            ret["references"] = [reference.to_dict() for reference in schema.references]
        if schema.schema_type is not SchemaType.AVRO:
            ret["schemaType"] = schema.schema_type
        # Return also compatibility information to compatibility check
        compatibility = self.database.get_subject_compatibility(subject=subject)
        if compatibility:
            ret["compatibility"] = compatibility

        return ret

    async def subject_version_referencedby_get(
        self, subject: Subject, version: Version, *, include_deleted: bool = False
    ) -> list:
        schema_versions = self.subject_get(subject, include_deleted=include_deleted)
        if not schema_versions:
            raise SubjectNotFoundException()
        resolved_version = Versioner.from_schema_versions(schema_versions=schema_versions, version=version)
        schema_data: SchemaVersion | None = schema_versions.get(resolved_version, None)
        if not schema_data:
            raise VersionNotFoundException()
        referenced_by = self.schema_reader.get_referenced_by(schema_data.subject, schema_data.version)

        if referenced_by and len(referenced_by) > 0:
            return list(referenced_by)
        return []

    def resolve_and_parse(self, schema: TypedSchema) -> ParsedTypedSchema:
        references, dependencies = self.resolve_references(schema.references) if schema.references else (None, None)
        return ParsedTypedSchema.parse(
            schema_type=schema.schema_type,
            schema_str=schema.schema_str,
            references=references,
            dependencies=dependencies,
        )

    async def write_new_schema_local(
        self,
        subject: Subject,
        new_schema: ValidatedTypedSchema,
        new_schema_references: Sequence[Reference] | None,
    ) -> int:
        """Write new schema and return new id or return id of matching existing schema

        This function is allowed to be called only from the Karapace master node.
        """
        LOG.info("Writing new schema locally since we're the master")
        async with self.schema_lock:
            # When waiting for a lock, another writer may have written the schema.
            # Fast path check for resolving.
            maybe_schema_id = self.database.get_schema_id_if_exists(
                subject=subject, schema=new_schema, include_deleted=False
            )
            if maybe_schema_id is not None:
                LOG.debug("Schema id %r found from subject+schema cache", maybe_schema_id)
                return maybe_schema_id

            all_schema_versions = self.database.find_subject_schemas(subject=subject, include_deleted=True)
            if not all_schema_versions:
                version = Version(1)
                schema_id = self.database.get_schema_id(new_schema)
                LOG.debug(
                    "Registering new subject: %r, id: %r with version: %r with schema %r, schema_id: %r",
                    subject,
                    schema_id,
                    version,
                    new_schema.schema_str,
                    schema_id,
                )
            else:
                # First check if any of the existing schemas for the subject match
                live_versions = self.get_live_versions_sorted(all_schema_versions)
                if not live_versions:  # Previous ones have been deleted by the user.
                    version = self.database.get_next_version(subject=subject)
                    schema_id = self.database.get_schema_id(new_schema)
                    LOG.debug(
                        "Registering subject: %r, id: %r new version: %r with schema %r, schema_id: %r",
                        subject,
                        schema_id,
                        version,
                        new_schema.schema_str,
                        schema_id,
                    )
                    self.send_schema_message(
                        subject=subject,
                        schema=new_schema,
                        schema_id=schema_id,
                        version=version,
                        deleted=False,
                        references=new_schema_references,
                    )
                    return schema_id

                result = self.check_schema_compatibility(new_schema, subject)

                if is_incompatible(result):
                    LOG.warning(
                        "Incompatible schema: %s, incompatibilities: %s", result.compatibility, result.incompatibilities
                    )
                    compatibility_mode = self.get_compatibility_mode(subject=subject)
                    raise IncompatibleSchema(
                        f"Incompatible schema, compatibility_mode={compatibility_mode.value}. "
                        f"Incompatibilities: {', '.join(result.messages)[:300]}"
                    )

                # We didn't find an existing schema and the schema is compatible so go and create one
                version = self.database.get_next_version(subject=subject)
                schema_id = self.database.get_schema_id(new_schema)
                LOG.debug(
                    "Registering subject: %r, id: %r new version: %r with schema %s, schema_id: %r",
                    subject,
                    schema_id,
                    version,
                    new_schema,
                    schema_id,
                )

            self.send_schema_message(
                subject=subject,
                schema=new_schema,
                schema_id=schema_id,
                version=version,
                deleted=False,
                references=new_schema_references,
            )
            return schema_id

    def get_subject_versions_for_schema(
        self, schema_id: SchemaId, *, include_deleted: bool = False
    ) -> list[dict[str, Subject | Version]]:
        subject_versions: list[dict[str, Subject | Version]] = []
        schema_versions = self.database.find_schema_versions_by_schema_id(
            schema_id=schema_id,
            include_deleted=include_deleted,
        )
        for schema_version in schema_versions:
            subject_versions.append({"subject": schema_version.subject, "version": schema_version.version})
        subject_versions = sorted(subject_versions, key=lambda s: (s["subject"], s["version"]))
        return subject_versions

    def get_global_mode(self) -> Mode:
        return Mode.readwrite

    def get_subject_mode(self) -> Mode:
        return Mode.readwrite

    def send_schema_message(
        self,
        *,
        subject: Subject,
        schema: TypedSchema | None,
        schema_id: int,
        version: Version,
        deleted: bool,
        references: Sequence[Reference] | None,
    ) -> None:
        key = {"subject": subject, "version": version.value, "magic": 1, "keytype": "SCHEMA"}
        if schema:
            value = {
                "subject": subject,
                "version": version.value,
                "id": schema_id,
                "schema": str(schema),
                "deleted": deleted,
            }
            if references:
                value["references"] = [reference.to_dict() for reference in references]
            if schema.schema_type is not SchemaType.AVRO:
                value["schemaType"] = schema.schema_type
        else:
            value = None
        self.producer.send_message(key=key, value=value)

    def send_config_message(self, compatibility_level: CompatibilityModes, subject: Subject | None = None) -> None:
        key = {"subject": subject, "magic": 0, "keytype": "CONFIG"}
        value = {"compatibilityLevel": compatibility_level.value}
        self.producer.send_message(key=key, value=value)

    def send_config_subject_delete_message(self, subject: Subject) -> None:
        key = {"subject": subject, "magic": 0, "keytype": "CONFIG"}
        self.producer.send_message(key=key, value=None)

    def resolve_references(
        self,
        references: Sequence[Reference | LatestVersionReference] | Sequence[JsonObject] | None,
    ) -> tuple[Sequence[Reference], dict[str, Dependency]] | tuple[None, None]:
        return self.schema_reader.resolve_references(references) if references else (None, None)

    def send_delete_subject_message(self, subject: Subject, version: Version) -> None:
        key = {"subject": subject, "magic": 0, "keytype": "DELETE_SUBJECT"}
        value = {"subject": subject, "version": version.value}
        self.producer.send_message(key=key, value=value)

    def check_schema_compatibility(
        self,
        new_schema: ValidatedTypedSchema,
        subject: Subject,
    ) -> SchemaCompatibilityResult:
        result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

        compatibility_mode = self.get_compatibility_mode(subject=subject)
        all_schema_versions: dict[Version, SchemaVersion] = self.database.find_subject_schemas(
            subject=subject, include_deleted=True
        )
        live_versions = self.get_live_versions_sorted(all_schema_versions)

        if not live_versions:
            old_versions = []
        elif compatibility_mode.is_transitive():
            # Check against all versions
            old_versions = live_versions
        else:
            # Only check against latest version
            old_versions = [live_versions[-1]]

        for old_version in old_versions:
            old_parsed_schema = self.resolve_and_parse(all_schema_versions[old_version].schema)

            result = SchemaCompatibility.check_compatibility(
                old_schema=old_parsed_schema,
                new_schema=new_schema,
                compatibility_mode=compatibility_mode,
            )

            if is_incompatible(result):
                return result

        return result

    @staticmethod
    def get_live_versions_sorted(all_schema_versions: dict[Version, SchemaVersion]) -> list[Version]:
        live_schema_versions = {
            version_id: schema_version
            for version_id, schema_version in all_schema_versions.items()
            if schema_version.deleted is False
        }
        return sorted(live_schema_versions)
