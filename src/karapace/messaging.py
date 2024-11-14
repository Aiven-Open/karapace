"""
karapace - Karapace producer

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from aiokafka.errors import MessageSizeTooLargeError
from karapace.config import Config
from karapace.errors import SchemaTooLargeException
from karapace.kafka.producer import KafkaProducer
from karapace.key_format import KeyFormatter
from karapace.offset_watcher import OffsetWatcher
from karapace.utils import json_encode
from karapace.version import __version__
from typing import Any, Final, Optional, Union

import logging
import time

LOG = logging.getLogger(__name__)
X_REGISTRY_VERSION_HEADER = ("X-Registry-Version", f"karapace-{__version__}".encode())


class KarapaceProducer:
    def __init__(self, *, config: Config, offset_watcher: OffsetWatcher, key_formatter: KeyFormatter):
        self._producer: Optional[KafkaProducer] = None
        self._config = config
        self._offset_watcher = offset_watcher
        self._key_formatter = key_formatter
        self._kafka_timeout = 10
        self._schemas_topic = self._config.topic_name
        self._x_origin_host_header: Final = ("X-Origin-Host", self._config.host.encode())

    def initialize_karapace_producer(
        self,
    ) -> None:
        while True:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self._config.bootstrap_uri,
                    verify_connection=False,
                    security_protocol=self._config.security_protocol,
                    ssl_cafile=self._config.ssl_cafile,
                    ssl_certfile=self._config.ssl_certfile,
                    ssl_keyfile=self._config.ssl_keyfile,
                    sasl_mechanism=self._config.sasl_mechanism,
                    sasl_plain_username=self._config.sasl_plain_username,
                    sasl_plain_password=self._config.sasl_plain_password,
                    metadata_max_age_ms=self._config.metadata_max_age_ms,
                    socket_timeout_ms=2000,  # missing topics will block unless we cache cluster metadata and pre-check
                    connections_max_idle_ms=self._config.connections_max_idle_ms,  # helps through cluster upgrades ??
                )
                return
            except:  # pylint: disable=bare-except
                LOG.exception("Unable to create producer, retrying")
                time.sleep(1)

    def close(self) -> None:
        LOG.info("Closing karapace_producer")
        if self._producer is not None:
            self._producer.flush()

    def _send_kafka_message(self, key: Union[bytes, str], value: Union[bytes, str]) -> None:
        assert self._producer is not None

        if isinstance(key, str):
            key = key.encode("utf8")
        if isinstance(value, str):
            value = value.encode("utf8")

        future = self._producer.send(
            self._schemas_topic,
            key=key,
            value=value,
            headers=[X_REGISTRY_VERSION_HEADER, self._x_origin_host_header],
        )
        self._producer.flush(timeout=self._kafka_timeout)
        try:
            msg = future.result(self._kafka_timeout)
        except MessageSizeTooLargeError as ex:
            raise SchemaTooLargeException from ex

        sent_offset = msg.offset()

        LOG.info(
            "Waiting for schema reader to catch up. key: %r, value: %r, offset: %r",
            key,
            value,
            sent_offset,
        )

        if self._offset_watcher.wait_for_offset(sent_offset, timeout=60) is True:
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

    def send_message(self, *, key: dict[str, Any], value: Optional[dict[str, Any]]) -> None:
        key_bytes = self._key_formatter.format_key(key)
        value_bytes: Union[bytes, str] = b""
        if value is not None:
            value_bytes = json_encode(value, binary=True, compact=True)
        self._send_kafka_message(key=key_bytes, value=value_bytes)
