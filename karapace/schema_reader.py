"""
karapace - Kafka schema reader

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from avro.schema import Schema as AvroSchema
from confluent_kafka import Message, TopicPartition
from contextlib import closing, ExitStack
from enum import Enum
from jsonschema.validators import Draft7Validator
from kafka.errors import (
    GroupAuthorizationFailedError,
    InvalidReplicationFactorError,
    KafkaConfigurationError,
    KafkaTimeoutError,
    LeaderNotAvailableError,
    NoBrokersAvailable,
    NodeNotReadyError,
    NotLeaderForPartitionError,
    TopicAlreadyExistsError,
    TopicAuthorizationFailedError,
    UnknownTopicOrPartitionError,
)
from karapace import constants
from karapace.config import Config
from karapace.dependency import Dependency
from karapace.errors import InvalidReferences, InvalidSchema
from karapace.in_memory_database import InMemoryDatabase
from karapace.kafka.admin import KafkaAdminClient
from karapace.kafka.common import translate_from_kafkaerror
from karapace.kafka.consumer import KafkaConsumer
from karapace.key_format import is_key_in_canonical_format, KeyFormatter, KeyMode
from karapace.master_coordinator import MasterCoordinator
from karapace.offset_watcher import OffsetWatcher
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_models import parse_protobuf_schema_definition, SchemaType, TypedSchema, ValidatedTypedSchema
from karapace.schema_references import LatestVersionReference, Reference, reference_from_mapping, Referents
from karapace.statsd import StatsClient
from karapace.typing import JsonObject, ResolvedVersion, SchemaId, Subject
from karapace.utils import json_decode, JSONDecodeError
from threading import Event, Thread
from typing import Final, Mapping, Sequence

import json
import logging
import time

LOG = logging.getLogger(__name__)

# The value `0` is a valid offset and it represents the first message produced
# to a topic, therefore it can not be used.
OFFSET_UNINITIALIZED: Final = -2
OFFSET_EMPTY: Final = -1

KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS: Final = 2.0
SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS: Final = 5.0

# For good startup performance the consumption of multiple
# records for each consume round is essential.
# Consumer default is 1 message for each consume call and after
# startup the default is a good value. If consumer would expect
# more messages it would return control back after timeout and
# making schema storing latency to be `processing time + timeout`.
MAX_MESSAGES_TO_CONSUME_ON_STARTUP: Final = 1000
MAX_MESSAGES_TO_CONSUME_AFTER_STARTUP: Final = 1
MESSAGE_CONSUME_TIMEOUT_SECONDS: Final = 0.2

# Metric names
METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT: Final = "karapace_schema_reader_records_processed"
METRIC_SCHEMA_TOPIC_RECORDS_PER_KEYMODE_GAUGE: Final = "karapace_schema_reader_records_per_keymode"
METRIC_SCHEMAS_GAUGE: Final = "karapace_schema_reader_schemas"
METRIC_SUBJECTS_GAUGE: Final = "karapace_schema_reader_subjects"
METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE: Final = "karapace_schema_reader_subject_data_schema_versions"


class MessageType(Enum):
    config = "CONFIG"
    schema = "SCHEMA"
    delete_subject = "DELETE_SUBJECT"
    no_operation = "NOOP"


def _create_consumer_from_config(config: Config) -> KafkaConsumer:
    # Group not set on purpose, all consumers read the same data
    session_timeout_ms = config["session_timeout_ms"]
    return KafkaConsumer(
        bootstrap_servers=config["bootstrap_uri"],
        topic=config["topic_name"],
        enable_auto_commit=False,
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
        metadata_max_age_ms=config["metadata_max_age_ms"],
    )


def _create_admin_client_from_config(config: Config) -> KafkaAdminClient:
    return KafkaAdminClient(
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


class KafkaSchemaReader(Thread):
    def __init__(
        self,
        config: Config,
        offset_watcher: OffsetWatcher,
        key_formatter: KeyFormatter,
        database: InMemoryDatabase,
        master_coordinator: MasterCoordinator | None = None,
    ) -> None:
        Thread.__init__(self, name="schema-reader")
        self.master_coordinator = master_coordinator
        self.timeout_s = MESSAGE_CONSUME_TIMEOUT_SECONDS
        self.max_messages_to_process = MAX_MESSAGES_TO_CONSUME_ON_STARTUP
        self.config = config

        self.database = database
        self.admin_client: KafkaAdminClient | None = None
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer: KafkaConsumer | None = None
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
        self._stop_schema_reader = Event()

        self.key_formatter = key_formatter

        # Metrics
        self.processed_canonical_keys_total = 0
        self.processed_deprecated_karapace_keys_total = 0
        self.last_check = time.monotonic()
        self.start_time = time.monotonic()
        self.startup_previous_processed_offset = 0

    def close(self) -> None:
        LOG.info("Closing schema_reader")
        self._stop_schema_reader.set()

    def run(self) -> None:
        with ExitStack() as stack:
            while not self._stop_schema_reader.is_set() and self.admin_client is None:
                try:
                    self.admin_client = _create_admin_client_from_config(self.config)
                except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                    LOG.warning("[Admin Client] No Brokers available yet. Retrying")
                    self._stop_schema_reader.wait(timeout=KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS)
                except KafkaConfigurationError:
                    LOG.info("[Admin Client] Invalid configuration. Bailing")
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    LOG.exception("[Admin Client] Unexpected exception. Retrying")
                    self.stats.unexpected_exception(ex=e, where="admin_client_instantiation")
                    self._stop_schema_reader.wait(timeout=2.0)

            assert self.admin_client is not None

            while not self._stop_schema_reader.is_set() and self.consumer is None:
                try:
                    self.consumer = _create_consumer_from_config(self.config)
                    stack.enter_context(closing(self.consumer))
                except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                    LOG.warning("[Consumer] No Brokers available yet. Retrying")
                    self._stop_schema_reader.wait(timeout=2.0)
                except KafkaConfigurationError:
                    LOG.info("[Consumer] Invalid configuration. Bailing")
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    LOG.exception("[Consumer] Unexpected exception. Retrying")
                    self.stats.unexpected_exception(ex=e, where="consumer_instantiation")
                    self._stop_schema_reader.wait(timeout=KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS)

            assert self.consumer is not None

            schema_topic_exists = False
            while not self._stop_schema_reader.is_set() and not schema_topic_exists:
                try:
                    LOG.info("[Schema Topic] Creating %r", self.config["topic_name"])
                    topic = self.admin_client.new_topic(
                        name=self.config["topic_name"],
                        num_partitions=constants.SCHEMA_TOPIC_NUM_PARTITIONS,
                        replication_factor=self.config["replication_factor"],
                        config={"cleanup.policy": "compact"},
                    )
                    LOG.info("[Schema Topic] Successfully created %r", topic.topic)
                    schema_topic_exists = True
                except TopicAlreadyExistsError:
                    LOG.warning("[Schema Topic] Already exists %r", self.config["topic_name"])
                    schema_topic_exists = True
                except InvalidReplicationFactorError:
                    LOG.info(
                        "[Schema Topic] Failed to create topic %r, not enough Kafka brokers ready yet, retrying",
                        self.config["topic_name"],
                    )
                    self._stop_schema_reader.wait(timeout=SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS)
                except:  # pylint: disable=bare-except
                    LOG.exception("[Schema Topic] Failed to create %r, retrying", self.config["topic_name"])
                    self._stop_schema_reader.wait(timeout=SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS)

            while not self._stop_schema_reader.is_set():
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
        assert self.consumer is not None, "Thread must be started"

        try:
            beginning_offset, _ = self.consumer.get_watermark_offsets(TopicPartition(self.config["topic_name"], 0))
            # The `-1` decrement here is due to historical reasons (evolution of schema reader and offset watcher):
            # * The first `OffsetWatcher` implementation neeeded this for flagging empty offsets
            # * Then synchronization and locking was changed and this remained
            # * See https://github.com/Aiven-Open/karapace/pull/364/files
            # * See `OFFSET_EMPTY` and `OFFSET_UNINITIALIZED`
            return beginning_offset - 1
        except KafkaTimeoutError:
            LOG.warning("Reading begin offsets timed out.")
        except UnknownTopicOrPartitionError:
            LOG.warning("Topic does not yet exist.")
        except (LeaderNotAvailableError, NotLeaderForPartitionError):
            LOG.warning("Retrying to find leader for schema topic partition.")
        except Exception as e:  # pylint: disable=broad-except
            self.stats.unexpected_exception(ex=e, where="_get_beginning_offset")
            LOG.exception("Unexpected exception when reading begin offsets.")
        return OFFSET_UNINITIALIZED

    def _is_ready(self) -> bool:
        if self.ready:
            return True

        assert self.consumer is not None, "Thread must be started"

        try:
            _, end_offset = self.consumer.get_watermark_offsets(TopicPartition(self.config["topic_name"], 0))
        except KafkaTimeoutError:
            LOG.warning("Reading end offsets timed out.")
            return False
        except UnknownTopicOrPartitionError:
            LOG.warning("Topic does not yet exist.")
            return False
        except (LeaderNotAvailableError, NotLeaderForPartitionError):
            LOG.warning("Retrying to find leader for schema topic partition.")
            return False
        except Exception as e:  # pylint: disable=broad-except
            self.stats.unexpected_exception(ex=e, where="_is_ready")
            LOG.exception("Unexpected exception when reading end offsets.")
            return False
        # Offset in the response is the offset for the next upcoming message.
        # Reduce by one for actual highest offset.
        self._highest_offset = end_offset - 1
        cur_time = time.monotonic()
        time_from_last_check = cur_time - self.last_check
        progress_pct = 0 if not self._highest_offset else round((self.offset / self._highest_offset) * 100, 2)
        startup_processed_message_per_second = (self.offset - self.startup_previous_processed_offset) / time_from_last_check
        LOG.info(
            "Replay progress (%s): %s/%s (%s %%) (recs/s %s)",
            round(time_from_last_check, 2),
            self.offset,
            self._highest_offset,
            progress_pct,
            startup_processed_message_per_second,
        )
        self.last_check = cur_time
        self.startup_previous_processed_offset = self.offset
        ready = self.offset >= self._highest_offset
        if ready:
            self.max_messages_to_process = MAX_MESSAGES_TO_CONSUME_AFTER_STARTUP
            LOG.info("Ready in %s seconds", time.monotonic() - self.start_time)
        return ready

    def highest_offset(self) -> int:
        return max(self._highest_offset, self._offset_watcher.greatest_offset())

    @staticmethod
    def _parse_message_value(raw_value: str | bytes) -> JsonObject | None:
        value = json_decode(raw_value)
        if isinstance(value, dict):
            return value
        if value is None or value == "":
            return None
        raise TypeError("Invalid type for value")

    def handle_messages(self) -> None:
        assert self.consumer is not None, "Thread must be started"

        msgs: list[Message] = self.consumer.consume(timeout=self.timeout_s, num_messages=self.max_messages_to_process)
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

        schema_records_processed_keymode_canonical = 0
        schema_records_processed_keymode_deprecated_karapace = 0
        for msg in msgs:
            try:
                message_key = msg.key()
                message_error = msg.error()
                if message_error is not None:
                    raise translate_from_kafkaerror(message_error)

                assert message_key is not None
                key = json_decode(message_key)
            except JSONDecodeError:
                LOG.exception("Invalid JSON in msg.key() at offset %s", msg.offset())
                continue
            except (GroupAuthorizationFailedError, TopicAuthorizationFailedError) as exc:
                LOG.error(
                    "Kafka authorization error when consuming from %s: %s %s",
                    self.config["topic_name"],
                    exc,
                    msg.error(),
                )
                continue

            assert isinstance(key, dict)
            msg_keymode = KeyMode.CANONICAL if is_key_in_canonical_format(key) else KeyMode.DEPRECATED_KARAPACE
            # Key mode detection happens on startup.
            # Default keymode is CANONICAL and preferred unless any data consumed
            # has key in non-canonical format. If keymode is set to DEPRECATED_KARAPACE
            # the subsequent keys are omitted from detection.
            if not self.ready and self.key_formatter.get_keymode() == KeyMode.CANONICAL:
                if msg_keymode == KeyMode.DEPRECATED_KARAPACE:
                    self.key_formatter.set_keymode(KeyMode.DEPRECATED_KARAPACE)

            value = None
            message_value = msg.value()
            if message_value:
                try:
                    value = self._parse_message_value(message_value)
                except JSONDecodeError:
                    LOG.exception("Invalid JSON in msg.value() at offset %s", msg.offset())
                    continue

            self.handle_msg(key, value)
            self.offset = msg.offset()

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

    def _handle_msg_config(self, key: dict, value: dict | None) -> None:
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

    def _handle_msg_delete_subject(self, key: dict, value: dict | None) -> None:  # pylint: disable=unused-argument
        if value is None:
            LOG.warning("DELETE_SUBJECT record doesnt have a value, should have")
            return

        subject = value["subject"]
        version = value["version"]
        if self.database.find_subject(subject=subject) is None:
            LOG.warning("Subject: %r did not exist, should have", subject)
        else:
            LOG.info("Deleting subject: %r, value: %r", subject, value)
            self.database.delete_subject(subject=subject, version=version)

    def _handle_msg_schema_hard_delete(self, key: dict) -> None:
        subject, version = key["subject"], key["version"]

        if self.database.find_subject(subject=subject) is None:
            LOG.warning("Hard delete: Subject %s did not exist, should have", subject)
        elif version not in self.database.find_subject_schemas(subject=subject, include_deleted=True):
            LOG.warning("Hard delete: Version %d for subject %s did not exist, should have", version, subject)
        else:
            LOG.info("Hard delete: subject: %r version: %r", subject, version)
            self.database.delete_subject_schema(subject=subject, version=version)
            if not self.database.find_subject_schemas(subject=subject, include_deleted=True):
                LOG.info("Hard delete last version, subject %r is gone", subject)
                self.database.delete_subject_hard(subject=subject)

    def _handle_msg_schema(self, key: dict, value: dict | None) -> None:
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
        resolved_references: list[Reference] | None = None

        try:
            schema_type_parsed = SchemaType(schema_type)
        except ValueError:
            LOG.warning("Invalid schema type: %s", schema_type)
            return

        # This does two jobs:
        # - Validates the schema's JSON
        # - Re-encode the schema to make sure small differences on formatting
        # won't interfere with the equality. Note: This means it is possible
        # for the REST API to return data that is formatted differently from
        # what is available in the topic.

        parsed_schema: Draft7Validator | AvroSchema | ProtobufSchema | None = None
        resolved_dependencies: dict[str, Dependency] | None = None
        if schema_type_parsed in [SchemaType.AVRO, SchemaType.JSONSCHEMA]:
            try:
                schema_str = json.dumps(json.loads(schema_str), sort_keys=True)
            except json.JSONDecodeError:
                LOG.warning("Schema is not valid JSON")
                return
        elif schema_type_parsed == SchemaType.PROTOBUF:
            try:
                if schema_references:
                    candidate_references = [reference_from_mapping(reference_data) for reference_data in schema_references]
                    resolved_references, resolved_dependencies = self.resolve_references(candidate_references)
                parsed_schema = parse_protobuf_schema_definition(
                    schema_str,
                    resolved_references,
                    resolved_dependencies,
                    validate_references=False,
                    normalize=False,
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

    def handle_msg(self, key: dict, value: dict | None) -> None:
        if "keytype" in key:
            try:
                message_type = MessageType(key["keytype"])

                if message_type == MessageType.config:
                    self._handle_msg_config(key, value)
                elif message_type == MessageType.schema:
                    self._handle_msg_schema(key, value)
                elif message_type == MessageType.delete_subject:
                    self._handle_msg_delete_subject(key, value)
                elif message_type == MessageType.no_operation:
                    pass
            except (KeyError, ValueError):
                LOG.warning("The message %r-%r has been discarded because the %s is not managed", key, value, key["keytype"])

        else:
            LOG.warning(
                "The message %s-%s has been discarded because doesn't contain the `keytype` key in the key", key, value
            )

    def remove_referenced_by(
        self,
        schema_id: SchemaId,
        references: Sequence[Reference],
    ) -> None:
        self.database.remove_referenced_by(schema_id, references)

    def get_referenced_by(
        self,
        subject: Subject,
        version: ResolvedVersion,
    ) -> Referents | None:
        return self.database.get_referenced_by(subject, version)

    def _resolve_and_validate(self, schema: TypedSchema) -> ValidatedTypedSchema:
        references, dependencies = (
            self.resolve_references(schema.references) if schema.references else (schema.references, schema.dependencies)
        )
        return ValidatedTypedSchema.parse(
            schema_type=schema.schema_type,
            schema_str=schema.schema_str,
            references=references,
            dependencies=dependencies,
        )

    def _resolve_reference(
        self,
        reference: Reference | LatestVersionReference,
    ) -> tuple[Reference, Dependency]:
        subject_data = self.database.find_subject_schemas(
            subject=reference.subject,
            include_deleted=False,
        )

        if not subject_data:
            raise InvalidReferences(f"Subject not found {reference.subject}.")

        if isinstance(reference, LatestVersionReference):
            reference = reference.resolve(max(subject_data))

        schema_version = subject_data.get(reference.version, None)
        if schema_version is None:
            raise InvalidReferences(f"Subject {reference.subject} has no such schema version")

        if not schema_version.schema:
            raise InvalidReferences(f"No schema in {reference.subject} with version {reference.version}.")

        validated_schema = self._resolve_and_validate(schema_version.schema)

        return reference, Dependency.of(reference, validated_schema)

    def resolve_references(
        self,
        references: Sequence[Reference | LatestVersionReference] | Sequence[JsonObject],
    ) -> tuple[list[Reference], dict[str, Dependency]]:
        resolved_references = []
        dependencies = {}
        for reference in references:
            if isinstance(reference, Mapping):
                reference = reference_from_mapping(reference)
            resolved_reference, dependency = self._resolve_reference(reference)
            dependencies[resolved_reference.name] = dependency
            resolved_references.append(resolved_reference)
        return resolved_references, dependencies
