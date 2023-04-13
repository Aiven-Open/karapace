"""
karapace - Kafka schema reader

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from avro.schema import Schema as AvroSchema
from contextlib import closing, ExitStack
from jsonschema.validators import Draft7Validator
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (
    InvalidReplicationFactorError,
    KafkaConfigurationError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    NodeNotReadyError,
    TopicAlreadyExistsError,
)
from karapace import constants
from karapace.config import Config
from karapace.dependency import Dependency
from karapace.errors import InvalidReferences, InvalidSchema
from karapace.in_memory_database import InMemoryDatabase
from karapace.key_format import is_key_in_canonical_format, KeyFormatter, KeyMode
from karapace.master_coordinator import MasterCoordinator
from karapace.offset_watcher import OffsetWatcher
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_models import parse_protobuf_schema_definition, SchemaType, SchemaVersion, TypedSchema
from karapace.schema_references import Reference, Referents
from karapace.statsd import StatsClient
from karapace.typing import JsonObject, ResolvedVersion, SchemaId
from karapace.utils import json_decode, JSONDecodeError, KarapaceKafkaClient
from threading import Event, Thread
from typing import Dict, List, Optional, Union

import json
import logging
import time

Offset = int
Subject = str
Version = int

# The value `0` is a valid offset and it represents the first message produced
# to a topic, therefore it can not be used.
OFFSET_UNINITIALIZED = -2
OFFSET_EMPTY = -1
LOG = logging.getLogger(__name__)

KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS = 2.0
SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS = 5.0

# Metric names
METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT = "karapace_schema_reader_records_processed"
METRIC_SCHEMA_TOPIC_RECORDS_PER_KEYMODE_GAUGE = "karapace_schema_reader_records_per_keymode"
METRIC_SCHEMAS_GAUGE = "karapace_schema_reader_schemas"
METRIC_SUBJECTS_GAUGE = "karapace_schema_reader_subjects"
METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE = "karapace_schema_reader_subject_data_schema_versions"


def _create_consumer_from_config(config: Config) -> KafkaConsumer:
    # Group not set on purpose, all consumers read the same data
    session_timeout_ms = config["session_timeout_ms"]
    request_timeout_ms = max(session_timeout_ms, KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"])
    return KafkaConsumer(
        config["topic_name"],
        enable_auto_commit=False,
        api_version=(1, 0, 0),
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        fetch_max_wait_ms=50,
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        auto_offset_reset="earliest",
        session_timeout_ms=session_timeout_ms,
        request_timeout_ms=request_timeout_ms,
        kafka_client=KarapaceKafkaClient,
        metadata_max_age_ms=config["metadata_max_age_ms"],
    )


def _create_admin_client_from_config(config: Config) -> KafkaAdminClient:
    return KafkaAdminClient(
        api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
    )


def new_schema_topic_from_config(config: Config) -> NewTopic:
    return NewTopic(
        name=config["topic_name"],
        num_partitions=constants.SCHEMA_TOPIC_NUM_PARTITIONS,
        replication_factor=config["replication_factor"],
        topic_configs={"cleanup.policy": "compact"},
    )


class KafkaSchemaReader(Thread):
    def __init__(
        self,
        config: Config,
        offset_watcher: OffsetWatcher,
        key_formatter: KeyFormatter,
        database: InMemoryDatabase,
        master_coordinator: Optional[MasterCoordinator] = None,
    ) -> None:
        Thread.__init__(self, name="schema-reader")
        self.master_coordinator = master_coordinator
        self.timeout_ms = 200
        self.config = config

        self.database = database
        self.admin_client: Optional[KafkaAdminClient] = None
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer: Optional[KafkaConsumer] = None
        self._offset_watcher = offset_watcher
        self.stats = StatsClient(config=config)

        # Thread synchronization objects
        # - offset is used by the REST API to wait until this thread has
        # consumed the produced messages. This makes the REST APIs more
        # consistent (e.g. a request to a schema that was just produced will
        # return the correct data.)
        # - highest_offset's last seen value is stored by the REST API to
        # expose reader loading progress for health status endpoint.
        # - ready is used by the REST API to wait until this thread has
        # synchronized with data in the schema topic. This prevents the server
        # from returning stale data (e.g. a schemas has been soft deleted, but
        # the topic has not been compacted yet, waiting allows this to consume
        # the soft delete message and return the correct data instead of the
        # old stale version that has not been deleted yet.)
        self.offset = OFFSET_UNINITIALIZED
        self._highest_offset = OFFSET_UNINITIALIZED
        self.ready = False

        # This event controls when the Reader should stop running, it will be
        # set by another thread (e.g. `KarapaceSchemaRegistry`)
        self._stop = Event()

        self.key_formatter = key_formatter

        # Metrics
        self.processed_canonical_keys_total = 0
        self.processed_deprecated_karapace_keys_total = 0
        self.last_check = time.monotonic()

    def close(self) -> None:
        LOG.info("Closing schema_reader")
        self._stop.set()

    def run(self) -> None:
        with ExitStack() as stack:
            while not self._stop.is_set() and self.admin_client is None:
                try:
                    self.admin_client = _create_admin_client_from_config(self.config)
                    stack.enter_context(closing(self.admin_client))
                except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                    LOG.warning("[Admin Client] No Brokers available yet. Retrying")
                    self._stop.wait(timeout=KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS)
                except KafkaConfigurationError:
                    LOG.exception("[Admin Client] Invalid configuration. Bailing")
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    LOG.exception("[Admin Client] Unexpected exception. Retrying")
                    self.stats.unexpected_exception(ex=e, where="admin_client_instantiation")
                    self._stop.wait(timeout=2.0)

            while not self._stop.is_set() and self.consumer is None:
                try:
                    self.consumer = _create_consumer_from_config(self.config)
                    stack.enter_context(closing(self.consumer))
                except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                    LOG.warning("[Consumer] No Brokers available yet. Retrying")
                    self._stop.wait(timeout=2.0)
                except KafkaConfigurationError:
                    LOG.exception("[Consumer] Invalid configuration. Bailing")
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    LOG.exception("[Consumer] Unexpected exception. Retrying")
                    self.stats.unexpected_exception(ex=e, where="consumer_instantiation")
                    self._stop.wait(timeout=KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS)

            schema_topic_exists = False
            schema_topic = new_schema_topic_from_config(self.config)
            schema_topic_create = [schema_topic]
            while not self._stop.is_set() and not schema_topic_exists:
                try:
                    LOG.info("[Schema Topic] Creating %r", schema_topic.name)
                    self.admin_client.create_topics(schema_topic_create, timeout_ms=constants.TOPIC_CREATION_TIMEOUT_MS)
                    LOG.info("[Schema Topic] Successfully created %r", schema_topic.name)
                    schema_topic_exists = True
                except TopicAlreadyExistsError:
                    LOG.warning("[Schema Topic] Already exists %r", schema_topic.name)
                    schema_topic_exists = True
                except InvalidReplicationFactorError:
                    LOG.info(
                        "[Schema Topic] Failed to create topic %r, not enough Kafka brokers ready yet, retrying",
                        schema_topic.name,
                    )
                    self._stop.wait(timeout=SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS)
                except:  # pylint: disable=bare-except
                    LOG.exception("[Schema Topic] Failed to create %r, retrying", schema_topic.name)
                    self._stop.wait(timeout=SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS)

            while not self._stop.is_set():
                if self.offset == OFFSET_UNINITIALIZED:
                    # Handles also a unusual case of purged schemas topic where starting offset can be > 0
                    # and no records to process.
                    self.offset = self._get_beginning_offset()
                try:
                    self.handle_messages()
                except Exception as e:  # pylint: disable=broad-except
                    self.stats.unexpected_exception(ex=e, where="schema_reader_loop")
                    LOG.exception("Unexpected exception in schema reader loop")

    def _get_beginning_offset(self) -> int:
        try:
            offsets = self.consumer.beginning_offsets([TopicPartition(self.config["topic_name"], 0)])
            # Offset in the response is the offset for last offset.
            # Reduce by one for matching on startup.
            beginning_offset = list(offsets.values())[0] - 1
            return beginning_offset
        except KafkaTimeoutError:
            LOG.exception("Reading begin offsets timed out.")
        except Exception as e:  # pylint: disable=broad-except
            self.stats.unexpected_exception(ex=e, where="_get_beginning_offset")
            LOG.exception("Unexpected exception when reading begin offsets.")
        return OFFSET_UNINITIALIZED

    def _is_ready(self) -> bool:
        if self.ready:
            return True
        try:
            offsets = self.consumer.end_offsets([TopicPartition(self.config["topic_name"], 0)])
        except KafkaTimeoutError:
            LOG.exception("Reading end offsets timed out.")
            return False
        except Exception as e:  # pylint: disable=broad-except
            self.stats.unexpected_exception(ex=e, where="_is_ready")
            LOG.exception("Unexpected exception when reading end offsets.")
            return False
        # Offset in the response is the offset for the next upcoming message.
        # Reduce by one for actual highest offset.
        self._highest_offset = list(offsets.values())[0] - 1
        cur_time = time.monotonic()
        time_from_last_check = cur_time - self.last_check
        progress_pct = 0 if not self._highest_offset else round((self.offset / self._highest_offset) * 100, 2)
        LOG.info(
            "Replay progress (%s): %s/%s (%s %%)",
            round(time_from_last_check, 2),
            self.offset,
            self._highest_offset,
            progress_pct,
        )
        self.last_check = cur_time
        return self.offset >= self._highest_offset

    def highest_offset(self) -> int:
        return max(self._highest_offset, self._offset_watcher.greatest_offset())

    def handle_messages(self) -> None:
        assert self.consumer is not None, "Thread must be started"

        raw_msgs = self.consumer.poll(timeout_ms=self.timeout_ms)
        if self.ready is False:
            self.ready = self._is_ready()

        watch_offsets = False
        if self.master_coordinator is not None:
            are_we_master, _ = self.master_coordinator.get_master_info()
            # keep old behavior for True. When are_we_master is False, then we are a follower, so we should not accept direct
            # writes anyway. When are_we_master is None, then this particular node is waiting for a stable value, so any
            # messages off the topic are writes performed by another node
            # Also if master_eligibility is disabled by configuration, disable writes too
            if are_we_master is True:
                watch_offsets = True

        for _, msgs in raw_msgs.items():
            schema_records_processed_keymode_canonical = 0
            schema_records_processed_keymode_deprecated_karapace = 0
            for msg in msgs:
                try:
                    key = json_decode(msg.key)
                except JSONDecodeError:
                    LOG.exception("Invalid JSON in msg.key")
                    continue

                msg_keymode = KeyMode.CANONICAL if is_key_in_canonical_format(key) else KeyMode.DEPRECATED_KARAPACE
                # Key mode detection happens on startup.
                # Default keymode is CANONICAL and preferred unless any data consumed
                # has key in non-canonical format. If keymode is set to DEPRECATED_KARAPACE
                # the subsequent keys are omitted from detection.
                if not self.ready and self.key_formatter.get_keymode() == KeyMode.CANONICAL:
                    if msg_keymode == KeyMode.DEPRECATED_KARAPACE:
                        self.key_formatter.set_keymode(KeyMode.DEPRECATED_KARAPACE)

                value = None
                if msg.value:
                    try:
                        value = json_decode(msg.value)
                    except JSONDecodeError:
                        LOG.exception("Invalid JSON in msg.value")
                        continue

                self.handle_msg(key, value)
                self.offset = msg.offset

                if msg_keymode == KeyMode.CANONICAL:
                    schema_records_processed_keymode_canonical += 1
                else:
                    schema_records_processed_keymode_deprecated_karapace += 1

                if self.ready and watch_offsets:
                    self._offset_watcher.offset_seen(self.offset)

            self._report_schema_metrics(
                schema_records_processed_keymode_canonical,
                schema_records_processed_keymode_deprecated_karapace,
            )

    def _report_schema_metrics(
        self,
        schema_records_processed_keymode_canonical: int,
        schema_records_processed_keymode_deprecated_karapace: int,
    ) -> None:
        # Update processing counter always.
        self.stats.increase(
            metric=METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT,
            inc_value=schema_records_processed_keymode_canonical,
            tags={"keymode": KeyMode.CANONICAL},
        )
        self.stats.increase(
            metric=METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT,
            inc_value=schema_records_processed_keymode_deprecated_karapace,
            tags={"keymode": KeyMode.DEPRECATED_KARAPACE},
        )

        # Update following gauges only if there is a possibility of a change.
        records_processed = bool(
            schema_records_processed_keymode_canonical or schema_records_processed_keymode_deprecated_karapace
        )
        if records_processed:
            self.processed_canonical_keys_total += schema_records_processed_keymode_canonical
            self.stats.gauge(
                metric=METRIC_SCHEMA_TOPIC_RECORDS_PER_KEYMODE_GAUGE,
                value=self.processed_canonical_keys_total,
                tags={"keymode": KeyMode.CANONICAL},
            )
            self.processed_deprecated_karapace_keys_total += schema_records_processed_keymode_deprecated_karapace
            self.stats.gauge(
                metric=METRIC_SCHEMA_TOPIC_RECORDS_PER_KEYMODE_GAUGE,
                value=self.processed_deprecated_karapace_keys_total,
                tags={"keymode": KeyMode.DEPRECATED_KARAPACE},
            )
            num_schemas = self.database.num_schemas()
            num_subjects = self.database.num_subjects()
            self.stats.gauge(metric=METRIC_SCHEMAS_GAUGE, value=num_schemas)
            self.stats.gauge(metric=METRIC_SUBJECTS_GAUGE, value=num_subjects)
            live_versions, soft_deleted_versions = self.database.num_schema_versions()
            self.stats.gauge(
                metric=METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE,
                value=live_versions,
                tags={"state": "live"},
            )
            self.stats.gauge(
                metric=METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE,
                value=soft_deleted_versions,
                tags={"state": "soft_deleted"},
            )

    def _handle_msg_config(self, key: dict, value: Optional[dict]) -> None:
        subject = key.get("subject")
        if subject is not None:
            if self.database.find_subject(subject=subject) is None:
                LOG.info("Adding first version of subject: %r with no schemas", subject)
                self.database.insert_subject(subject=subject)
            if not value:
                LOG.info("Deleting compatibility config completely for subject: %r", subject)
                self.database.delete_subject_compatibility(subject=subject)
            else:
                LOG.info("Setting subject: %r config to: %r, value: %r", subject, value["compatibilityLevel"], value)
                self.database.set_subject_compatibility(subject=subject, compatibility=value["compatibilityLevel"])
        elif value is not None:
            LOG.info("Setting global config to: %r, value: %r", value["compatibilityLevel"], value)
            self.config["compatibility"] = value["compatibilityLevel"]

    def _handle_msg_delete_subject(self, key: dict, value: Optional[dict]) -> None:  # pylint: disable=unused-argument
        if value is None:
            LOG.error("DELETE_SUBJECT record doesnt have a value, should have")
            return

        subject = value["subject"]
        version = value["version"]
        if self.database.find_subject(subject=subject) is None:
            LOG.error("Subject: %r did not exist, should have", subject)
        else:
            LOG.info("Deleting subject: %r, value: %r", subject, value)
            self.database.delete_subject(subject=subject, version=version)

    def _handle_msg_schema_hard_delete(self, key: dict) -> None:
        subject, version = key["subject"], key["version"]

        if self.database.find_subject(subject=subject) is None:
            LOG.error("Hard delete: Subject %s did not exist, should have", subject)
        elif version not in self.database.find_subject_schemas(subject=subject, include_deleted=True):
            LOG.error("Hard delete: Version %d for subject %s did not exist, should have", version, subject)
        else:
            LOG.info("Hard delete: subject: %r version: %r", subject, version)
            self.database.delete_subject_schema(subject=subject, version=version)
            if not self.database.find_subject_schemas(subject=subject, include_deleted=True):
                LOG.info("Hard delete last version, subject %r is gone", subject)
                self.database.delete_subject_hard(subject=subject)

    def _handle_msg_schema(self, key: dict, value: Optional[dict]) -> None:
        if not value:
            self._handle_msg_schema_hard_delete(key)
            return

        schema_type = value.get("schemaType", "AVRO")
        schema_str = value["schema"]
        schema_subject = value["subject"]
        schema_id = value["id"]
        schema_version = value["version"]
        schema_deleted = value.get("deleted", False)
        schema_references = value.get("references", None)
        resolved_references: Optional[List[Reference]] = None
        resolved_dependencies: Optional[Dict[str, Dependency]] = None

        try:
            schema_type_parsed = SchemaType(schema_type)
        except ValueError:
            LOG.error("Invalid schema type: %s", schema_type)
            return

        # This does two jobs:
        # - Validates the schema's JSON
        # - Re-encode the schema to make sure small differences on formatting
        # won't interfere with the equality. Note: This means it is possible
        # for the REST API to return data that is formatted differently from
        # what is available in the topic.

        parsed_schema: Optional[Union[Draft7Validator, AvroSchema, ProtobufSchema]] = None
        resolved_dependencies: Optional[Dict[str, Dependency]] = None
        if schema_type_parsed in [SchemaType.AVRO, SchemaType.JSONSCHEMA]:
            try:
                schema_str = json.dumps(json.loads(schema_str), sort_keys=True)
            except json.JSONDecodeError:
                LOG.error("Schema is not valid JSON")
                return
        elif schema_type_parsed == SchemaType.PROTOBUF:
            try:
                if schema_references:
                    resolved_references = [
                        Reference(reference["name"], reference["subject"], reference["version"])
                        for reference in schema_references
                    ]
                    resolved_dependencies = self.resolve_references(resolved_references)
                parsed_schema = parse_protobuf_schema_definition(
                    schema_str,
                    resolved_references,
                    resolved_dependencies,
                    validate_references=False,
                )
                schema_str = str(parsed_schema)
            except InvalidSchema:
                LOG.exception("Schema is not valid ProtoBuf definition")
                return
            except InvalidReferences:
                LOG.exception("Invalid Protobuf references")
                return

        try:
            typed_schema = TypedSchema(
                schema_type=schema_type_parsed,
                schema_str=schema_str,
                references=resolved_references,
                dependencies=resolved_dependencies,
                schema=parsed_schema,
            )
        except (InvalidSchema, JSONDecodeError):
            return

        self.database.insert_schema_version(
            subject=schema_subject,
            schema_id=schema_id,
            version=schema_version,
            deleted=schema_deleted,
            schema=typed_schema,
            references=resolved_references,
        )

        if resolved_references:
            for ref in resolved_references:
                self.database.insert_referenced_by(subject=ref.subject, version=ref.version, schema_id=schema_id)

    def handle_msg(self, key: dict, value: Optional[dict]) -> None:
        if key["keytype"] == "CONFIG":
            self._handle_msg_config(key, value)
        elif key["keytype"] == "SCHEMA":
            self._handle_msg_schema(key, value)
        elif key["keytype"] == "DELETE_SUBJECT":
            self._handle_msg_delete_subject(key, value)
        elif key["keytype"] == "NOOP":  # for spec completeness
            pass

    def remove_referenced_by(self, schema_id: SchemaId, references: List[Reference]):
        self.database.remove_referenced_by(schema_id, references)

    def get_referenced_by(self, subject: Subject, version: ResolvedVersion) -> Optional[Referents]:
        return self.database.get_referenced_by(subject, version)

    def _resolve_reference(self, reference: Reference) -> Dependency:
        subject_data = self.database.find_subject_schemas(subject=reference.subject, include_deleted=False)
        if not subject_data:
            raise InvalidReferences(f"Subject not found {reference.subject}.")
        schema_version: SchemaVersion = subject_data.get(reference.version, None)
        if schema_version is None:
            raise InvalidReferences(f"Subject {reference.subject} has no such schema version")
        schema: TypedSchema = schema_version.schema
        if not schema:
            raise InvalidReferences(f"No schema in {reference.subject} with version {reference.version}.")
        if schema.references:
            schema_dependencies = self.resolve_references(schema.references)
            if schema.dependencies is None:
                schema.dependencies = schema_dependencies
        return Dependency.of(reference, schema)

    def resolve_references(self, references: Union[List[Reference], JsonObject]) -> Dict[str, Dependency]:
        dependencies: Dict[str, Dependency] = dict()
        for reference in references:
            if isinstance(reference, Reference):
                dependencies[reference.name] = self._resolve_reference(reference)
            else:
                dependencies[reference["name"]] = self._resolve_reference(
                    Reference(reference["name"], reference["subject"], reference["version"])
                )
        return dependencies
