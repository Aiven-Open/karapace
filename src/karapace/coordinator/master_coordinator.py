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
from karapace.typing import PrimaryInfo, SchemaReaderStoppper
from schema_registry.telemetry.tracer import Tracer
from threading import Thread
from typing import Final

import asyncio
import logging
import time

__all__ = ("MasterCoordinator",)


LOG = logging.getLogger(__name__)


class MasterCoordinator:
    """Handles primary election

    The coordination is run in own dedicated thread, under stress situation the main
    eventloop could have queue of items to work and having own thread will give more
    runtime for the coordination tasks as Python intrepreter will switch the active
    thread by the configured thread switch interval. Default interval in CPython is
    5 milliseconds.
    """

    def __init__(self, config: Config) -> None:
        super().__init__()
        self._config: Final = config
        self._kafka_client: AIOKafkaClient | None = None
        self._sc: SchemaCoordinator | None = None
        self._closing = asyncio.Event()
        self._thread: Thread = Thread(target=self._start_loop, daemon=True)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._schema_reader_stopper: SchemaReaderStoppper | None = None
        self._tracer = Tracer()

    def set_stoppper(self, schema_reader_stopper: SchemaReaderStoppper) -> None:
        self._schema_reader_stopper = schema_reader_stopper

    @property
    def schema_coordinator(self) -> SchemaCoordinator | None:
        return self._sc

    @property
    def config(self) -> Config:
        return self._config

    def start(self) -> None:
        self._thread.start()

    def _start_loop(self) -> None:
        # we should avoid the reassignment otherwise we leak resources
        assert self._loop is None, "Loop already started"
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.create_task(self._async_loop())
        self._loop.run_forever()

    async def _async_loop(self) -> None:
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

        # keeping the thread sleeping until it die.
        # we need to keep the schema_coordinator running
        # it contains the `heartbeat` and coordination logic.
        await self._closing.wait()

        LOG.info("Closing master_coordinator")
        if self._sc:
            await self._sc.close()
        while self._loop is not None and not self._loop.is_closed():
            self._loop.stop()
            if not self._loop.is_running():
                self._loop.close()
            time.sleep(0.5)
        if self._kafka_client:
            await self._kafka_client.close()

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
        assert self._schema_reader_stopper is not None
        schema_coordinator = SchemaCoordinator(
            client=self._kafka_client,
            schema_reader_stopper=self._schema_reader_stopper,
            election_strategy=self._config.master_election_strategy,
            group_id=self._config.group_id,
            hostname=self._config.get_advertised_hostname(),
            master_eligibility=self._config.master_eligibility,
            port=self._config.get_advertised_port(),
            scheme=self._config.advertised_protocol,
            session_timeout_ms=self._config.session_timeout_ms,
            waiting_time_before_acting_as_master_ms=self._config.waiting_time_before_acting_as_master_ms,
        )
        schema_coordinator.start()
        return schema_coordinator

    def get_coordinator_status(self) -> SchemaCoordinatorStatus:
        with self._tracer.get_tracer().start_as_current_span(
            self._tracer.get_name_from_caller_with_class(self, self.get_coordinator_status)
        ):
            assert self._sc is not None
            generation = self._sc.generation if self._sc is not None else OffsetCommitRequest.DEFAULT_GENERATION_ID
            return SchemaCoordinatorStatus(
                is_primary=self._sc.are_we_master() if self._sc is not None else None,
                is_primary_eligible=self._config.master_eligibility,
                primary_url=self._sc.master_url if self._sc is not None else None,
                is_running=True,
                group_generation_id=generation if generation is not None else -1,
            )

    def get_master_info(self) -> PrimaryInfo:
        """Return whether we're the master, and the actual master url that can be used if we're not"""
        if not self._sc:
            return PrimaryInfo(False, None)

        if not self._sc.ready():
            # we should wait for a while after we have been elected master, we should also consume
            # all the messages in the log before proceeding, check the doc of `self._sc.are_we_master`
            # for more details
            return PrimaryInfo(False, None)

        url: str | None = None
        if (
            self._sc.master_url is not None
            and self.config.get_address() not in self._sc.master_url
            and f"{self.config.advertised_hostname}:{self.config.advertised_port}" not in self._sc.master_url
        ):
            url = self._sc.master_url

        return PrimaryInfo(self._sc.are_we_master(), url)

    def __send_close_event(self) -> None:
        self._closing.set()

    async def close(self) -> None:
        LOG.info("Sending the close signal to the master coordinator thread")
        if self._loop is None:
            raise ValueError("Cannot stop the loop before `.start()` is called")
        self._loop.call_soon_threadsafe(self.__send_close_event)
