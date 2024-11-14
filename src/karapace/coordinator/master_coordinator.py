"""
karapace - master coordinator

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from aiokafka import AIOKafkaClient
from aiokafka.errors import KafkaConnectionError
from aiokafka.helpers import create_ssl_context
from aiokafka.protocol.commit import OffsetCommitRequest_v2 as OffsetCommitRequest
from karapace.config import Config
from karapace.coordinator.schema_coordinator import SchemaCoordinator, SchemaCoordinatorStatus
from karapace.kafka.types import DEFAULT_REQUEST_TIMEOUT_MS
from typing import Final

import asyncio
import logging

__all__ = ("MasterCoordinator",)

LOG = logging.getLogger(__name__)


class MasterCoordinator:
    """Handles primary election"""

    def __init__(self, config: Config) -> None:
        super().__init__()
        self._config: Final = config
        self._kafka_client: AIOKafkaClient | None = None
        self._running = True
        self._sc: SchemaCoordinator | None = None

    @property
    def schema_coordinator(self) -> SchemaCoordinator | None:
        return self._sc

    @property
    def config(self) -> Config:
        return self._config

    async def start(self) -> None:
        self._kafka_client = self.init_kafka_client()
        # Wait until schema coordinator is ready.
        # This probably needs better synchronization than plain waits.
        while True:
            try:
                await self._kafka_client.bootstrap()
                break
            except KafkaConnectionError:
                LOG.warning("Kafka client bootstrap failed.")
                await asyncio.sleep(0.5)

        while not self._kafka_client.cluster.brokers():
            LOG.info(
                "Waiting cluster metadata update after Kafka client bootstrap: %s.", self._kafka_client.cluster.brokers()
            )
            self._kafka_client.force_metadata_update()
            await asyncio.sleep(0.5)

        self._sc = self.init_schema_coordinator()
        while True:
            if self._sc.ready():
                return
            await asyncio.sleep(0.5)

    def init_kafka_client(self) -> AIOKafkaClient:
        ssl_context = create_ssl_context(
            cafile=self._config.ssl_cafile,
            certfile=self._config.ssl_certfile,
            keyfile=self._config.ssl_keyfile,
        )

        return AIOKafkaClient(
            bootstrap_servers=self._config.bootstrap_uri,
            client_id=self._config.client_id,
            metadata_max_age_ms=self._config.metadata_max_age_ms,
            request_timeout_ms=DEFAULT_REQUEST_TIMEOUT_MS,
            # Set default "PLAIN" if not configured, aiokafka expects
            # security protocol for SASL but requires a non-null value
            # for sasl mechanism.
            sasl_mechanism=self._config.sasl_mechanism or "PLAIN",
            sasl_plain_username=self._config.sasl_plain_username,
            sasl_plain_password=self._config.sasl_plain_password,
            security_protocol=self._config.security_protocol,
            ssl_context=ssl_context,
        )

    def init_schema_coordinator(self) -> SchemaCoordinator:
        assert self._kafka_client is not None
        schema_coordinator = SchemaCoordinator(
            client=self._kafka_client,
            election_strategy=self._config.master_election_strategy,
            group_id=self._config.group_id,
            hostname=self._config.advertised_hostname,
            master_eligibility=self._config.master_eligibility,
            port=self._config.advertised_port,
            scheme=self._config.advertised_protocol,
            session_timeout_ms=self._config.session_timeout_ms,
        )
        schema_coordinator.start()
        return schema_coordinator

    def get_coordinator_status(self) -> SchemaCoordinatorStatus:
        assert self._sc is not None
        generation = self._sc.generation if self._sc is not None else OffsetCommitRequest.DEFAULT_GENERATION_ID
        return SchemaCoordinatorStatus(
            is_primary=self._sc.are_we_master if self._sc is not None else None,
            is_primary_eligible=self._config.master_eligibility,
            primary_url=self._sc.master_url if self._sc is not None else None,
            is_running=True,
            group_generation_id=generation if generation is not None else -1,
        )

    def get_master_info(self) -> tuple[bool | None, str | None]:
        """Return whether we're the master, and the actual master url that can be used if we're not"""
        assert self._sc is not None
        return self._sc.are_we_master, self._sc.master_url

    async def close(self) -> None:
        LOG.info("Closing master_coordinator")
        if self._sc:
            await self._sc.close()
        if self._kafka_client:
            await self._kafka_client.close()
