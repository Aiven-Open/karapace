from kafka import errors as kafka_errors, KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from karapace.compatibility import CompatibilityModes
from karapace.config import Config
from karapace.errors import SchemaTooLargeException
from karapace.key_format import KeyFormatter
from karapace.offsets_watcher import OffsetsWatcher
from karapace.schema_models import SchemaType, TypedSchema
from karapace.typing import Subject, Version
from karapace.utils import json_encode, KarapaceKafkaClient
from typing import Optional, Union

import logging
import time

LOG = logging.getLogger(__name__)


class SchemaRecordProducer:
    def __init__(self, config: Config, key_formatter: KeyFormatter, offsets_watcher: OffsetsWatcher) -> None:
        self.kafka_timeout = 10

        self.config = config
        self.producer = self._create_producer()
        self.key_formatter = key_formatter
        self.offsets_watcher = offsets_watcher

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

    def close(self) -> None:
        self.producer.close()

    def send_kafka_message(
        self, key: Union[bytes, str], value: Optional[Union[bytes, str]], wait_for_offset: bool
    ) -> FutureRecordMetadata:
        if isinstance(key, str):
            key = key.encode("utf8")
        if isinstance(value, str):
            value = value.encode("utf8")

        future = self.producer.send(self.config["topic_name"], key=key, value=value)
        self.producer.flush(timeout=self.kafka_timeout)
        try:
            msg = future.get(self.kafka_timeout)
        except kafka_errors.MessageSizeTooLargeError as ex:
            raise SchemaTooLargeException from ex

        sent_offset = msg.offset

        if wait_for_offset:
            LOG.info(
                "Waiting for schema reader to caught up. key: %r, value: %r, offset: %r",
                key,
                value,
                sent_offset,
            )

            if self.offsets_watcher.wait_for_offset(sent_offset, timeout=60) is True:
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
        else:
            LOG.info("Skip waiting of offset %s", sent_offset)

        return future

    def send_schema_message(
        self,
        *,
        subject: Subject,
        schema: Optional[TypedSchema],
        schema_id: int,
        version: int,
        deleted: bool,
        wait_for_offset: bool = True,
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
            value = None
        return self.send_kafka_message(key, value, wait_for_offset=wait_for_offset)

    def send_config_message(
        self,
        compatibility_level: CompatibilityModes,
        subject: Optional[Subject] = None,
        wait_for_offset: bool = True,
    ) -> FutureRecordMetadata:
        key = self.key_formatter.format_key(
            {
                "subject": subject,
                "magic": 0,
                "keytype": "CONFIG",
            }
        )
        value = '{{"compatibilityLevel":"{}"}}'.format(compatibility_level.value)
        return self.send_kafka_message(key, value, wait_for_offset=wait_for_offset)

    def send_config_subject_delete_message(self, subject: Subject, wait_for_offset: bool = True) -> FutureRecordMetadata:
        key = self.key_formatter.format_key(
            {
                "subject": subject,
                "magic": 0,
                "keytype": "CONFIG",
            }
        )
        return self.send_kafka_message(key, "".encode("utf-8"), wait_for_offset=wait_for_offset)

    def send_delete_subject_message(
        self, subject: Subject, version: Version, wait_for_offset: bool = True
    ) -> FutureRecordMetadata:
        key = self.key_formatter.format_key(
            {
                "subject": subject,
                "magic": 0,
                "keytype": "DELETE_SUBJECT",
            }
        )
        value = '{{"subject":"{}","version":{}}}'.format(subject, version)
        return self.send_kafka_message(key, value, wait_for_offset=wait_for_offset)
