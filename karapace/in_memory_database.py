"""
karapace - Schema and subjects in memory database

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass, field
from karapace.schema_models import SchemaVersion, TypedSchema
from karapace.typing import ResolvedVersion, SchemaId, Subject
from threading import Lock, RLock
from typing import Dict, List, Optional, Tuple

import logging

LOG = logging.getLogger(__name__)


@dataclass
class SubjectData:
    schemas: Dict[ResolvedVersion, SchemaVersion] = field(default_factory=dict)
    compatibility: Optional[str] = None


class InMemoryDatabase:
    def __init__(self) -> None:
        self.global_schema_id = 0
        self.id_lock_thread = Lock()
        self.subjects: Dict[Subject, SubjectData] = {}
        self.schemas: Dict[SchemaId, TypedSchema] = {}
        self.schema_lock_thread = RLock()

        # Content based deduplication of schemas. This is used to reduce memory
        # usage when the same schema is produce multiple times to the same or
        # different subjects. The deduplication is based on the schema content
        # instead of the ids to handle corrupt data (where the ids are equal
        # but the schema themselves don't match)
        self._hash_to_schema: Dict[str, TypedSchema] = {}
        self._hash_to_schema_id_on_subject: Dict[Subject, Dict[str, SchemaId]] = {}

    def log_state(self) -> None:
        if LOG.isEnabledFor(logging.DEBUG):
            debug_str = "\nState\n\tSchemas:\n"
            for sid, schema in self.schemas.items():
                debug_str += f"\t\tid: {sid}, schema: {schema}\n"
            debug_str += "\tSubjects:\n"
            for subject, subject_data in self.subjects.items():
                debug_str += f"\t\t{subject}\n"
                for schema_version in subject_data.schemas.values():
                    sid = schema_version.schema_id
                    ver = schema_version.version
                    schema_str = schema_version.schema.schema_str[:128]
                    debug_str += f"\t\t\tid: {sid}, ver: {ver}, schema: {schema_str}\n"
            debug_str += "--------------------------------------------------------------------------------------\n"
            LOG.debug(debug_str)

    def _get_schema_id_from_storage(self, *, new_schema: TypedSchema) -> Optional[SchemaId]:
        for schema_id, schema in self.schemas.items():
            if schema == new_schema:
                return schema_id
        return None

    def get_schema_id(self, new_schema: TypedSchema) -> SchemaId:
        with self.id_lock_thread:
            maybe_schema_id = self._get_schema_id_from_storage(new_schema=new_schema)
            if maybe_schema_id is not None:
                return maybe_schema_id
            self.global_schema_id += 1
            return self.global_schema_id

    def get_schema_id_if_exists(
        self, *, subject: Subject, schema: TypedSchema, include_deleted: bool  # pylint: disable=unused-argument
    ) -> Optional[SchemaId]:
        subject_fingerprints = self._hash_to_schema_id_on_subject.get(subject)
        if subject_fingerprints:
            return subject_fingerprints.get(schema.fingerprint(), None)
        return None

    def _set_schema_id_on_subject(self, *, subject: Subject, schema: TypedSchema, schema_id: SchemaId) -> None:
        schema_to_schema_id = self._hash_to_schema_id_on_subject.setdefault(subject, {})
        schema_to_schema_id[schema.fingerprint()] = schema_id

    def _delete_from_schema_id_on_subject(self, *, subject: Subject, schema: TypedSchema) -> None:
        schema_to_schema_id = self._hash_to_schema_id_on_subject.get(subject, None)
        if schema_to_schema_id is not None:
            schema_to_schema_id.pop(schema.fingerprint(), None)
            if len(schema_to_schema_id) == 0:
                self._hash_to_schema_id_on_subject.pop(subject)

    def _delete_subject_from_schema_id_on_subject(self, *, subject: Subject) -> None:
        self._hash_to_schema_id_on_subject.pop(subject, None)

    def _get_from_hash_cache(self, *, typed_schema: TypedSchema) -> TypedSchema:
        return self._hash_to_schema.setdefault(typed_schema.fingerprint(), typed_schema)

    def get_next_version(self, *, subject: Subject) -> ResolvedVersion:
        return max(self.subjects[subject].schemas) + 1

    def insert_schema_version(
        self, *, subject: Subject, schema_id: SchemaId, version: ResolvedVersion, deleted: bool, schema: TypedSchema
    ) -> None:
        with self.schema_lock_thread:
            self.global_schema_id = max(self.global_schema_id, schema_id)

            # dedup schemas to reduce memory pressure
            schema = self._get_from_hash_cache(typed_schema=schema)

            if self.find_subject(subject=subject) is None:
                LOG.info("Adding first version of subject: %r with no schemas", subject)
                self.insert_subject(subject=subject)

            if version in self.subjects[subject].schemas:
                LOG.info("Updating entry subject: %r version: %r id: %r", subject, version, schema_id)
            else:
                LOG.info("Adding entry subject: %r version: %r id: %r", subject, version, schema_id)
            self.schemas[schema_id] = schema
            self.subjects[subject].schemas[version] = SchemaVersion(
                subject=subject,
                version=version,
                deleted=deleted,
                schema_id=schema_id,
                schema=schema,
            )

            if not deleted:
                self._set_schema_id_on_subject(
                    subject=subject,
                    schema=schema,
                    schema_id=schema_id,
                )
            else:
                self._delete_from_schema_id_on_subject(
                    subject=subject,
                    schema=schema,
                )

    def insert_subject(self, *, subject: Subject) -> None:
        self.subjects.setdefault(subject, SubjectData())

    def get_subject_compatibility(self, *, subject: Subject) -> Optional[str]:
        if subject in self.subjects:
            return self.subjects[subject].compatibility
        return None

    def delete_subject_compatibility(self, *, subject: Subject) -> None:
        if subject in self.subjects:
            self.subjects[subject].compatibility = None

    def set_subject_compatibility(self, *, subject: Subject, compatibility: str) -> None:
        if subject in self.subjects:
            self.subjects[subject].compatibility = compatibility

    def find_schema(self, *, schema_id: SchemaId) -> Optional[TypedSchema]:
        return self.schemas[schema_id]

    def find_schemas(self, *, include_deleted: bool, latest_only: bool) -> Dict[Subject, List[SchemaVersion]]:
        res_schemas = {}
        with self.schema_lock_thread:
            for subject, subject_data in self.subjects.items():
                selected_schemas: List[SchemaVersion] = []
                schemas = list(subject_data.schemas.values())
                if latest_only:
                    # TODO don't include the deleted here?
                    selected_schemas = [schemas[-1]]
                else:
                    selected_schemas = schemas
                if include_deleted:
                    selected_schemas = [schema for schema in selected_schemas if schema.deleted is False]
                res_schemas[subject] = selected_schemas
        return res_schemas

    def find_schema_versions_by_schema_id(self, *, schema_id: SchemaId, include_deleted: bool) -> List[SchemaVersion]:
        schema_versions: List[SchemaVersion] = []
        with self.schema_lock_thread:
            for subject in self.subjects:
                # find_subject_schemas will also acquire the schema_lock_thread, RLock
                found_schema_versions = self.find_subject_schemas(subject=subject, include_deleted=include_deleted)
                for schema_version in found_schema_versions.values():
                    if schema_version.schema_id == schema_id:
                        schema_versions.append(schema_version)
        return schema_versions

    def find_subject(self, *, subject: Subject) -> Optional[Subject]:
        return subject if subject in self.subjects else None

    def find_subjects(self, *, include_deleted: bool) -> List[Subject]:
        if include_deleted:
            return list(self.subjects.keys())
        with self.schema_lock_thread:
            return [
                subject for subject in self.subjects if self.find_subject_schemas(subject=subject, include_deleted=False)
            ]

    def find_subject_schemas(self, *, subject: Subject, include_deleted: bool) -> Dict[ResolvedVersion, SchemaVersion]:
        if subject not in self.subjects:
            return {}
        if include_deleted:
            return self.subjects[subject].schemas
        with self.schema_lock_thread:
            return {
                version_id: schema_version
                for version_id, schema_version in self.subjects[subject].schemas.items()
                if schema_version.deleted is False
            }

    def delete_subject(self, *, subject: Subject, version: ResolvedVersion) -> None:
        with self.schema_lock_thread:
            for schema_version in self.subjects[subject].schemas.values():
                if schema_version.version <= version:
                    schema_version.deleted = True
                self._delete_from_schema_id_on_subject(subject=subject, schema=schema_version.schema)

    def delete_subject_hard(self, *, subject: Subject) -> None:
        with self.schema_lock_thread:
            del self.subjects[subject]
            self._delete_subject_from_schema_id_on_subject(subject=subject)

    def delete_subject_schema(self, *, subject: Subject, version: ResolvedVersion) -> None:
        with self.schema_lock_thread:
            self.subjects[subject].schemas.pop(version, None)

    def num_schemas(self) -> int:
        return len(self.schemas)

    def num_subjects(self) -> int:
        return len(self.subjects)

    def num_schema_versions(self) -> Tuple[int, int]:
        live_versions = 0
        soft_deleted_versions = 0
        with self.schema_lock_thread:
            for subject_data in self.subjects.values():
                for version in subject_data.schemas.values():
                    if not version.deleted:
                        live_versions += 1
                    else:
                        soft_deleted_versions += 1
        return (live_versions, soft_deleted_versions)
