from contextlib import AsyncExitStack, closing
from kafka import errors as kafka_errors, KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from karapace.compatibility import check_compatibility, CompatibilityModes
from karapace.compatibility.jsonschema.checks import is_incompatible
from karapace.config import Config
from karapace.errors import (
    IncompatibleSchema,
    InvalidVersion,
    SchemasNotFoundException,
    SchemaTooLargeException,
    SchemaVersionNotSoftDeletedException,
    SchemaVersionSoftDeletedException,
    SubjectNotFoundException,
    SubjectNotSoftDeletedException,
    SubjectSoftDeletedException,
    VersionNotFoundException,
)
from karapace.key_format import KeyFormatter
from karapace.master_coordinator import MasterCoordinator
from karapace.schema_models import SchemaType, TypedSchema, ValidatedTypedSchema
from karapace.schema_reader import KafkaSchemaReader
from karapace.typing import ResolvedVersion, Subject, SubjectData, Version
from karapace.utils import json_encode, KarapaceKafkaClient
from karapace.version import __version__
from typing import cast, Dict, List, Optional, Tuple, Union

import asyncio
import logging
import time

LOG = logging.getLogger(__name__)
X_REGISTRY_VERSION_HEADER = ("X-Registry-Version", f"karapace-{__version__}".encode("utf8"))


def _resolve_version(subject_data: SubjectData, version: Version) -> ResolvedVersion:
    max_version = max(subject_data["schemas"])
    resolved_version: int
    if isinstance(version, str) and version == "latest":
        return max_version

    version = int(version)
    if version <= max_version:
        resolved_version = version
    else:
        raise VersionNotFoundException()
    return resolved_version


def validate_version(version: Version) -> Version:
    try:
        version_number = int(version)
        if version_number > 0:
            return version
        raise InvalidVersion(f"Invalid version {version_number}")
    except ValueError as ex:
        if version == "latest":
            return version
        raise InvalidVersion(f"Invalid version {version}") from ex


class KarapaceSchemaRegistry:
    def __init__(self, config: Config) -> None:
        self.config = config
        host: str = cast(str, self.config["host"])
        self.x_origin_host_header: Tuple[str, bytes] = ("X-Origin-Host", host.encode("utf8"))

        self.producer = self._create_producer()
        self.kafka_timeout = 10

        self.mc = MasterCoordinator(config=self.config)
        self.key_formatter = KeyFormatter()
        self.schema_reader = KafkaSchemaReader(
            config=self.config, key_formatter=self.key_formatter, master_coordinator=self.mc
        )

        self.schema_lock = asyncio.Lock()
        self._master_lock = asyncio.Lock()

    @property
    def subjects(self) -> Dict[Subject, SubjectData]:
        return self.schema_reader.subjects

    def subjects_list(self, include_deleted: bool = False) -> List[Subject]:
        return [
            key
            for key, val in self.schema_reader.subjects.items()
            if self.schema_reader.get_schemas(key, include_deleted=include_deleted)
        ]

    @property
    def compatibility(self) -> str:
        return str(self.config["compatibility"])

    def get_schemas(self, subject: str, *, include_deleted: bool = False) -> SubjectData:
        return self.schema_reader.get_schemas(subject=subject, include_deleted=include_deleted)

    def start(self) -> None:
        self.mc.start()
        self.schema_reader.start()

    async def close(self) -> None:
        async with AsyncExitStack() as stack:
            stack.enter_context(closing(self.mc))
            stack.enter_context(closing(self.schema_reader))
            stack.enter_context(closing(self.producer))

    def _create_producer(self) -> KafkaProducer:
        while True:
            try:
                return KafkaProducer(
                    bootstrap_servers=self.config["bootstrap_uri"],
                    security_protocol=self.config["security_protocol"],
                    ssl_cafile=self.config["ssl_cafile"],
                    ssl_certfile=self.config["ssl_certfile"],
                    ssl_keyfile=self.config["ssl_keyfile"],
                    sasl_mechanism=self.config["sasl_mechanism"],
                    sasl_plain_username=self.config["sasl_plain_username"],
                    sasl_plain_password=self.config["sasl_plain_password"],
                    api_version=(1, 0, 0),
                    metadata_max_age_ms=self.config["metadata_max_age_ms"],
                    max_block_ms=2000,  # missing topics will block unless we cache cluster metadata and pre-check
                    connections_max_idle_ms=self.config["connections_max_idle_ms"],  # helps through cluster upgrades ??
                    kafka_client=KarapaceKafkaClient,
                )
            except:  # pylint: disable=bare-except
                LOG.exception("Unable to create producer, retrying")
                time.sleep(1)

    async def get_master(self, ignore_readiness: bool = False) -> Tuple[bool, Optional[str]]:
        """Resolve if current node is the primary and the primary node address.

        :param bool ignore_readiness: Ignore waiting to become ready and return
                                      follower/primary state and primary url.
        :return (bool, Optional[str]): returns the primary/follower state and primary url
        """
        async with self._master_lock:
            while True:
                are_we_master, master_url = self.mc.get_master_info()
                if are_we_master is None:
                    LOG.info("No master set: %r, url: %r", are_we_master, master_url)
                elif not ignore_readiness and self.schema_reader.ready is False:
                    LOG.info("Schema reader isn't ready yet: %r", self.schema_reader.ready)
                else:
                    return are_we_master, master_url
                await asyncio.sleep(1.0)

    def get_compatibility_mode(self, subject: SubjectData) -> CompatibilityModes:
        compatibility = subject.get("compatibility", self.config["compatibility"])
        try:
            compatibility_mode = CompatibilityModes(compatibility)
        except ValueError as e:
            raise ValueError(f"Unknown compatibility mode {compatibility}") from e
        return compatibility_mode

    async def schemas_list(self, *, include_deleted: bool, latest_only: bool) -> Dict[Subject, SubjectData]:
        async with self.schema_lock:
            return self.schema_reader.get_schemas_list(include_deleted=include_deleted, latest_only=latest_only)

    def schemas_get(self, schema_id: int, *, fetch_max_id: bool = False) -> Optional[TypedSchema]:
        with self.schema_reader.id_lock:
            try:
                schema = self.schema_reader.schemas.get(schema_id)

                if schema and fetch_max_id:
                    schema.max_id = self.schema_reader.global_schema_id

                return schema
            except KeyError:
                return None

    async def subject_delete_local(self, subject: str, permanent: bool) -> List[ResolvedVersion]:
        async with self.schema_lock:
            subject_data_all = self.subject_get(subject, include_deleted=True)
            subject_schemas_all = subject_data_all["schemas"]

            # Subject can be permanently deleted if no schemas or all are soft deleted.
            can_permanent_delete = not bool(
                [version for version, value in subject_schemas_all.items() if not value.get("deleted", False)]
            )
            if permanent and not can_permanent_delete:
                raise SubjectNotSoftDeletedException()

            # Subject is soft deleted if all schemas in subject have deleted flag
            already_soft_deleted = len(subject_schemas_all) == len(
                [version for version, value in subject_schemas_all.items() if value.get("deleted", False)]
            )

            if not permanent and already_soft_deleted:
                raise SubjectSoftDeletedException()

            latest_schema_id = 0
            version_list = []
            if permanent:
                version_list = list(subject_schemas_all)
                latest_schema_id = version_list[-1]
                for version, value in list(subject_schemas_all.items()):
                    schema_id = value.get("id")
                    LOG.info("Permanently deleting subject '%s' version %s (schema id=%s)", subject, version, schema_id)
                    self.send_schema_message(
                        subject=subject, schema=None, schema_id=schema_id, version=version, deleted=True
                    )
            else:
                try:
                    subject_data_live = self.subject_get(subject, include_deleted=False)
                    version_list = list(subject_data_live["schemas"])
                    if version_list:
                        latest_schema_id = version_list[-1]
                except SchemasNotFoundException:
                    pass
                self.send_delete_subject_message(subject, latest_schema_id)

            return version_list

    async def subject_version_delete_local(self, subject: Subject, version: Version, permanent: bool) -> ResolvedVersion:
        async with self.schema_lock:
            subject_data = self.subject_get(subject, include_deleted=True)
            if not permanent and isinstance(version, str) and version == "latest":
                subject_data["schemas"] = {
                    key: value for (key, value) in subject_data["schemas"].items() if value.get("deleted", False) is False
                }
            resolved_version = _resolve_version(subject_data=subject_data, version=version)
            subject_schema_data = subject_data["schemas"].get(resolved_version, None)

            if not subject_schema_data:
                raise VersionNotFoundException()
            if subject_schema_data.get("deleted", False) and not permanent:
                raise SchemaVersionSoftDeletedException()

            # Cannot directly hard delete
            if permanent and not subject_schema_data.get("deleted", False):
                raise SchemaVersionNotSoftDeletedException()

            schema_id = subject_schema_data["id"]
            schema = subject_schema_data["schema"]
            self.send_schema_message(
                subject=subject,
                schema=None if permanent else schema,
                schema_id=schema_id,
                version=resolved_version,
                deleted=True,
            )
            return resolved_version

    def subject_get(self, subject: Subject, include_deleted: bool = False) -> SubjectData:
        subject_data = self.schema_reader.subjects.get(subject)
        if not subject_data:
            raise SubjectNotFoundException()

        schemas = self.schema_reader.get_schemas(subject, include_deleted=include_deleted)
        if not schemas:
            raise SchemasNotFoundException

        subject_data = subject_data.copy()
        subject_data["schemas"] = schemas
        return subject_data

    async def subject_version_get(self, subject: Subject, version: Version, *, include_deleted: bool = False) -> SubjectData:
        validate_version(version)
        subject_data = self.subject_get(subject, include_deleted=include_deleted)
        if not subject_data:
            raise SubjectNotFoundException()
        schema_data = None

        resolved_version = _resolve_version(subject_data=subject_data, version=version)
        schema_data = subject_data["schemas"].get(resolved_version, None)

        if not schema_data:
            raise VersionNotFoundException()
        schema_id = schema_data["id"]
        schema = schema_data["schema"]

        ret = {
            "subject": subject,
            "version": resolved_version,
            "id": schema_id,
            "schema": schema.schema_str,
        }
        if schema.schema_type is not SchemaType.AVRO:
            ret["schemaType"] = schema.schema_type
        # Return also compatibility information to compatibility check
        if subject_data.get("compatibility"):
            ret["compatibility"] = subject_data.get("compatibility")
        return ret

    async def write_new_schema_local(
        self,
        subject: Subject,
        new_schema: ValidatedTypedSchema,
    ) -> int:
        """Write new schema and return new id or return id of matching existing schema

        This function is allowed to be called only from the Karapace master node.
        """
        LOG.info("Writing new schema locally since we're the master")
        async with self.schema_lock:
            subject_data = self.schema_reader.subjects.get(subject, None)
            if subject_data is None or not subject_data.get("schemas"):
                version = 1
                schema_id = self.schema_reader.get_schema_id(new_schema)
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
                schemas = self.schema_reader.get_schemas(subject)
                if not schemas:  # Previous ones have been deleted by the user.
                    version = max(self.schema_reader.subjects[subject]["schemas"]) + 1
                    schema_id = self.schema_reader.get_schema_id(new_schema)
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
                    )
                    return schema_id

                maybe_schema_id = self.schema_reader.get_schema_id_if_exists(subject=subject, schema=new_schema)
                if maybe_schema_id is not None:
                    LOG.debug("Schema id %r found from subject+schema cache", maybe_schema_id)
                    return maybe_schema_id

                compatibility_mode = self.get_compatibility_mode(subject=subject_data)

                # Run a compatibility check between on file schema(s) and the one being submitted now
                # the check is either towards the latest one or against all previous ones in case of
                # transitive mode
                schema_versions = sorted(list(schemas))
                if compatibility_mode.is_transitive():
                    check_against = schema_versions
                else:
                    check_against = [schema_versions[-1]]

                for old_version in check_against:
                    old_schema = subject_data["schemas"][old_version]["schema"]
                    validated_old_schema = ValidatedTypedSchema.parse(
                        schema_type=old_schema.schema_type, schema_str=old_schema.schema_str
                    )
                    result = check_compatibility(
                        old_schema=validated_old_schema,
                        new_schema=new_schema,
                        compatibility_mode=compatibility_mode,
                    )
                    if is_incompatible(result):
                        message = set(result.messages).pop() if result.messages else ""
                        LOG.warning("Incompatible schema: %s", result)
                        raise IncompatibleSchema(
                            f"Incompatible schema, compatibility_mode={compatibility_mode.value} {message}"
                        )

                # We didn't find an existing schema and the schema is compatible so go and create one
                schema_id = self.schema_reader.get_schema_id(new_schema)
                version = max(self.schema_reader.subjects[subject]["schemas"]) + 1
                if new_schema.schema_type is SchemaType.PROTOBUF:
                    LOG.debug(
                        "Registering subject: %r, id: %r new version: %r with schema %r, schema_id: %r",
                        subject,
                        schema_id,
                        version,
                        new_schema.__str__(),
                        schema_id,
                    )
                else:
                    LOG.debug(
                        "Registering subject: %r, id: %r new version: %r with schema %r, schema_id: %r",
                        subject,
                        schema_id,
                        version,
                        new_schema.to_dict(),
                        schema_id,
                    )

            self.send_schema_message(
                subject=subject,
                schema=new_schema,
                schema_id=schema_id,
                version=version,
                deleted=False,
            )
            return schema_id

    def get_versions(self, schema_id: int, *, include_deleted: bool = False) -> List[SubjectData]:
        subject_versions = []
        with self.schema_reader.id_lock:
            for subject, val in self.schema_reader.subjects.items():
                if self.schema_reader.get_schemas(subject, include_deleted=include_deleted) and "schemas" in val:
                    schemas = val["schemas"]
                    for version, schema in schemas.items():
                        if int(schema["id"]) == schema_id and (include_deleted or not schema.get("deleted", False)):
                            subject_versions.append({"subject": subject, "version": int(version)})
        subject_versions = sorted(subject_versions, key=lambda s: (s["subject"], s["version"]))
        return subject_versions

    def send_kafka_message(self, key: Union[bytes, str], value: Union[bytes, str]) -> FutureRecordMetadata:
        if isinstance(key, str):
            key = key.encode("utf8")
        if isinstance(value, str):
            value = value.encode("utf8")

        future = self.producer.send(
            self.config["topic_name"],
            key=key,
            value=value,
            headers=[X_REGISTRY_VERSION_HEADER, self.x_origin_host_header],
        )
        self.producer.flush(timeout=self.kafka_timeout)
        try:
            msg = future.get(self.kafka_timeout)
        except kafka_errors.MessageSizeTooLargeError as ex:
            raise SchemaTooLargeException from ex

        sent_offset = msg.offset

        LOG.info(
            "Waiting for schema reader to caught up. key: %r, value: %r, offset: %r",
            key,
            value,
            sent_offset,
        )

        if self.schema_reader.offset_watcher.wait_for_offset(sent_offset, timeout=60) is True:
            LOG.info(
                "Schema reader has found key. key: %r, value: %r, offset: %r",
                key,
                value,
                sent_offset,
            )
        else:
            raise RuntimeError(
                "Schema reader timed out while looking for key. key: {!r}, value: {!r}, offset: {}".format(
                    key, value, sent_offset
                )
            )

        return future

    def send_schema_message(
        self,
        *,
        subject: Subject,
        schema: Optional[TypedSchema],
        schema_id: int,
        version: int,
        deleted: bool,
    ) -> FutureRecordMetadata:
        key = self.key_formatter.format_key(
            {"subject": subject, "version": version, "magic": 1, "keytype": "SCHEMA"},
        )
        if schema:
            valuedict = {
                "subject": subject,
                "version": version,
                "id": schema_id,
                "schema": schema.schema_str,
                "deleted": deleted,
            }
            if schema.schema_type is not SchemaType.AVRO:
                valuedict["schemaType"] = schema.schema_type
            value = json_encode(valuedict)
        else:
            value = ""
        return self.send_kafka_message(key, value)

    def send_config_message(
        self, compatibility_level: CompatibilityModes, subject: Optional[Subject] = None
    ) -> FutureRecordMetadata:
        key = self.key_formatter.format_key(
            {
                "subject": subject,
                "magic": 0,
                "keytype": "CONFIG",
            }
        )
        value = '{{"compatibilityLevel":"{}"}}'.format(compatibility_level.value)
        return self.send_kafka_message(key, value)

    def send_config_subject_delete_message(self, subject: Subject) -> FutureRecordMetadata:
        key = self.key_formatter.format_key(
            {
                "subject": subject,
                "magic": 0,
                "keytype": "CONFIG",
            }
        )
        return self.send_kafka_message(key, "".encode("utf-8"))

    def send_delete_subject_message(self, subject: Subject, version: Version) -> FutureRecordMetadata:
        key = self.key_formatter.format_key(
            {
                "subject": subject,
                "magic": 0,
                "keytype": "DELETE_SUBJECT",
            }
        )
        value = '{{"subject":"{}","version":{}}}'.format(subject, version)
        return self.send_kafka_message(key, value)
