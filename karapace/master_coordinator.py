"""
karapace - master coordinator

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass
from kafka import KafkaConsumer
from kafka.coordinator.base import BaseCoordinator
from kafka.errors import NoBrokersAvailable, NodeNotReadyError
from kafka.metrics import MetricConfig, Metrics
from karapace import constants
from karapace.config import Config
from karapace.typing import JsonData, JsonObject
from karapace.utils import json_decode, json_encode, KarapaceKafkaClient
from karapace.version import __version__
from threading import Event, Thread
from typing import Any, Final, List, Optional, Sequence, Tuple
from typing_extensions import TypedDict

import logging
import time

__all__ = ("MasterCoordinator",)

# SR group errors
NO_ERROR: Final = 0
DUPLICATE_URLS: Final = 1
LOG = logging.getLogger(__name__)


class MemberIdentity(TypedDict):
    host: str
    port: int
    scheme: str
    master_eligibility: bool


class MemberAssignment(TypedDict):
    master: str
    master_identity: MemberIdentity


def get_member_url(scheme: str, host: str, port: int) -> str:
    return f"{scheme}://{host}:{port}"


def get_member_configuration(*, host: str, port: int, scheme: str, master_eligibility: bool) -> JsonData:
    return {
        "version": 2,
        "karapace_version": __version__,
        "host": host,
        "port": port,
        "scheme": scheme,
        "master_eligibility": master_eligibility,
    }


@dataclass
class SchemaCoordinatorStatus:
    is_primary: Optional[bool]
    is_primary_eligible: bool
    primary_url: Optional[str]
    is_running: bool
    group_generation_id: int


class SchemaCoordinator(BaseCoordinator):
    are_we_master: Optional[bool] = None
    master_url: Optional[str] = None

    def __init__(
        self,
        client: KarapaceKafkaClient,
        metrics: Metrics,
        hostname: str,
        port: int,
        scheme: str,
        master_eligibility: bool,
        election_strategy: str,
        **configs: Any,
    ) -> None:
        super().__init__(client=client, metrics=metrics, **configs)
        self.election_strategy = election_strategy
        self.hostname = hostname
        self.port = port
        self.scheme = scheme
        self.master_eligibility = master_eligibility

    def protocol_type(self) -> str:
        return "sr"

    def group_protocols(self) -> List[Tuple[str, str]]:
        assert self.scheme is not None
        return [
            (
                "v0",
                json_encode(
                    get_member_configuration(
                        host=self.hostname,
                        port=self.port,
                        scheme=self.scheme,
                        master_eligibility=self.master_eligibility,
                    ),
                    compact=True,
                ),
            )
        ]

    def _perform_assignment(
        self,
        leader_id: str,
        protocol: str,
        members: Sequence[Tuple[str, str]],
    ) -> JsonObject:
        LOG.info("Creating assignment: %r, protocol: %r, members: %r", leader_id, protocol, members)
        self.are_we_master = None
        error = NO_ERROR
        urls = {}
        fallback_urls = {}
        for member_id, member_data in members:
            member_identity = json_decode(member_data, MemberIdentity)
            if member_identity["master_eligibility"] is True:
                urls[get_member_url(member_identity["scheme"], member_identity["host"], member_identity["port"])] = (
                    member_id,
                    member_data,
                )
            else:
                fallback_urls[
                    get_member_url(member_identity["scheme"], member_identity["host"], member_identity["port"])
                ] = (member_id, member_data)
        if len(urls) > 0:
            chosen_url = sorted(urls, reverse=self.election_strategy.lower() == "highest")[0]
            schema_master_id, member_data = urls[chosen_url]
        else:
            # Protocol guarantees there is at least one member thus if urls is empty, fallback_urls cannot be
            chosen_url = sorted(fallback_urls, reverse=self.election_strategy.lower() == "highest")[0]
            schema_master_id, member_data = fallback_urls[chosen_url]
        member_identity = json_decode(member_data, MemberIdentity)
        identity = get_member_configuration(
            host=member_identity["host"],
            port=member_identity["port"],
            scheme=member_identity["scheme"],
            master_eligibility=member_identity["master_eligibility"],
        )
        LOG.info("Chose: %r with url: %r as the master", schema_master_id, chosen_url)

        assignments: JsonObject = {}
        for member_id, member_data in members:
            assignments[member_id] = json_encode(
                {"master": schema_master_id, "master_identity": identity, "error": error}, compact=True
            )
        return assignments

    def _on_join_prepare(self, generation: str, member_id: str) -> None:
        """Invoked prior to each group join or rejoin."""
        # needs to be implemented in our class for pylint to be satisfied

    def _on_join_complete(self, generation: str, member_id: str, protocol: str, member_assignment_bytes: bytes) -> None:
        LOG.info(
            "Join complete, generation %r, member_id: %r, protocol: %r, member_assignment_bytes: %r",
            generation,
            member_id,
            protocol,
            member_assignment_bytes,
        )
        member_assignment = json_decode(member_assignment_bytes, MemberAssignment)
        member_identity = member_assignment["master_identity"]

        master_url = get_member_url(
            scheme=member_identity["scheme"],
            host=member_identity["host"],
            port=member_identity["port"],
        )
        # On Kafka protocol we can be assigned to be master, but if not master eligible, then we're not master for real
        if member_assignment["master"] == member_id and member_identity["master_eligibility"]:
            self.master_url = master_url
            self.are_we_master = True
        elif not member_identity["master_eligibility"]:
            self.master_url = None
            self.are_we_master = False
        else:
            self.master_url = master_url
            self.are_we_master = False
        return super()._on_join_complete(generation, member_id, protocol, member_assignment_bytes)

    def _on_join_follower(self) -> None:
        LOG.info("We are a follower, not a master")
        return super()._on_join_follower()


class MasterCoordinator(Thread):
    """Handles schema topic creation and master election"""

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        self.timeout_ms = 10000
        self.kafka_client: Optional[KarapaceKafkaClient] = None
        self.running = True
        self.sc: Optional[SchemaCoordinator] = None
        metrics_tags = {"client-id": self.config["client_id"]}
        metric_config = MetricConfig(samples=2, time_window_ms=30000, tags=metrics_tags)
        self._metrics = Metrics(metric_config, reporters=[])
        self.schema_coordinator_ready = Event()

    def init_kafka_client(self) -> bool:
        try:
            self.kafka_client = KarapaceKafkaClient(
                api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
                sasl_mechanism=self.config["sasl_mechanism"],
                sasl_plain_username=self.config["sasl_plain_username"],
                sasl_plain_password=self.config["sasl_plain_password"],
                metadata_max_age_ms=self.config["metadata_max_age_ms"],
            )
            return True
        except (NodeNotReadyError, NoBrokersAvailable):
            LOG.warning("No Brokers available yet, retrying init_kafka_client()")
            time.sleep(2.0)
        return False

    def init_schema_coordinator(self) -> None:
        session_timeout_ms = self.config["session_timeout_ms"]
        assert self.kafka_client is not None
        self.sc = SchemaCoordinator(
            client=self.kafka_client,
            metrics=self._metrics,
            hostname=self.config["advertised_hostname"],
            port=self.config["advertised_port"],
            scheme=self.config["advertised_protocol"],
            master_eligibility=self.config["master_eligibility"],
            election_strategy=self.config.get("master_election_strategy", "lowest"),
            group_id=self.config["group_id"],
            session_timeout_ms=session_timeout_ms,
            request_timeout_ms=max(session_timeout_ms, KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"]),
        )
        self.schema_coordinator_ready.set()

    def get_coordinator_status(self) -> SchemaCoordinatorStatus:
        generation = self.sc.generation() if self.sc is not None else None
        return SchemaCoordinatorStatus(
            is_primary=self.sc.are_we_master if self.sc is not None else None,
            is_primary_eligible=self.config["master_eligibility"],
            primary_url=self.sc.master_url if self.sc is not None else None,
            is_running=self.is_alive(),
            group_generation_id=generation.generation_id if generation is not None else -1,
        )

    def get_master_info(self) -> Tuple[Optional[bool], Optional[str]]:
        """Return whether we're the master, and the actual master url that can be used if we're not"""
        self.schema_coordinator_ready.wait()
        assert self.sc is not None
        return self.sc.are_we_master, self.sc.master_url

    def close(self) -> None:
        LOG.info("Closing master_coordinator")
        self.running = False

    def run(self) -> None:
        _hb_interval = 3.0
        while self.running:
            try:
                if not self.kafka_client:
                    if self.init_kafka_client() is False:
                        # If Kafka client is not initialized sleep a bit
                        time.sleep(0.5)
                        continue
                if not self.sc:
                    self.init_schema_coordinator()
                    assert self.sc is not None
                    _hb_interval = self.sc.config["heartbeat_interval_ms"] / 1000

                self.sc.ensure_active_group()
                self.sc.poll_heartbeat()
                LOG.debug("We're master: %r: master_uri: %r", self.sc.are_we_master, self.sc.master_url)
                # In cases when heartbeat is missed the sleep min sleep time would be 0
                # from `time_to_next_heartbeat`. In that case halve the heartbeat interval for
                # some sane sleep instead of running the loop without sleep for a while.
                sleep_time = min(_hb_interval, self.sc.time_to_next_heartbeat())
                if not sleep_time:
                    sleep_time = _hb_interval / 2
                time.sleep(sleep_time)
            except:  # pylint: disable=bare-except
                LOG.exception("Exception in master_coordinator")
                time.sleep(1.0)

        if self.sc:
            self.sc.close()

        if self.kafka_client:
            self.kafka_client.close()
