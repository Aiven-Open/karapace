"""
karapace - master coordinator

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaConsumer
from kafka.coordinator.base import BaseCoordinator
from kafka.errors import NoBrokersAvailable, NodeNotReadyError
from kafka.metrics import MetricConfig, Metrics
from karapace import constants
from karapace.utils import KarapaceKafkaClient
from threading import Event, Thread
from typing import Optional, Tuple

import json
import logging
import time

# SR group errors
NO_ERROR = 0
DUPLICATE_URLS = 1


def get_identity_url(scheme, host, port):
    return "{}://{}:{}".format(scheme, host, port)


class SchemaCoordinator(BaseCoordinator):
    election_strategy = "lowest"
    hostname = None
    port = None
    scheme = None
    are_we_master = None
    master_url = None
    master_eligibility = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.log = logging.getLogger("SchemaCoordinator")

    def protocol_type(self):
        return "sr"

    def get_identity(self, *, host, port, scheme, json_encode=True):
        res = {"version": 1, "host": host, "port": port, "scheme": scheme, "master_eligibility": self.master_eligibility}
        if json_encode:
            return json.dumps(res)
        return res

    def group_protocols(self):
        return [("v0", self.get_identity(host=self.hostname, port=self.port, scheme=self.scheme))]

    def _perform_assignment(self, leader_id, protocol, members):
        self.log.info("Creating assignment: %r, protocol: %r, members: %r", leader_id, protocol, members)
        self.are_we_master = None
        error = NO_ERROR
        urls = {}
        fallback_urls = {}
        for member_id, member_data in members:
            member_identity = json.loads(member_data.decode("utf8"))
            if member_identity["master_eligibility"] is True:
                urls[get_identity_url(member_identity["scheme"], member_identity["host"], member_identity["port"])] = (
                    member_id,
                    member_data,
                )
            else:
                fallback_urls[
                    get_identity_url(member_identity["scheme"], member_identity["host"], member_identity["port"])
                ] = (member_id, member_data)
        if len(urls) > 0:
            chosen_url = sorted(urls, reverse=self.election_strategy.lower() == "highest")[0]
            schema_master_id, member_data = urls[chosen_url]
        else:
            # Protocol guarantees there is at least one member thus if urls is empty, fallback_urls cannot be
            chosen_url = sorted(fallback_urls, reverse=self.election_strategy.lower() == "highest")[0]
            schema_master_id, member_data = fallback_urls[chosen_url]
        member_identity = json.loads(member_data.decode("utf8"))
        identity = self.get_identity(
            host=member_identity["host"],
            port=member_identity["port"],
            scheme=member_identity["scheme"],
            json_encode=False,
        )
        self.log.info("Chose: %r with url: %r as the master", schema_master_id, chosen_url)

        assignments = {}
        for member_id, member_data in members:
            assignments[member_id] = json.dumps({"master": schema_master_id, "master_identity": identity, "error": error})
        return assignments

    def _on_join_prepare(self, generation, member_id):
        """Invoked prior to each group join or rejoin."""
        # needs to be implemented in our class for pylint to be satisfied

    def _on_join_complete(self, generation, member_id, protocol, member_assignment_bytes):
        self.log.info(
            "Join complete, generation %r, member_id: %r, protocol: %r, member_assignment_bytes: %r",
            generation,
            member_id,
            protocol,
            member_assignment_bytes,
        )
        member_assignment = json.loads(member_assignment_bytes.decode("utf8"))
        member_identity = member_assignment["master_identity"]

        master_url = get_identity_url(
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
        # pylint: disable=super-with-arguments
        return super(SchemaCoordinator, self)._on_join_complete(generation, member_id, protocol, member_assignment_bytes)

    def _on_join_follower(self):
        self.log.info("We are a follower, not a master")
        # pylint: disable=super-with-arguments
        return super(SchemaCoordinator, self)._on_join_follower()


class MasterCoordinator(Thread):
    """Handles schema topic creation and master election"""

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.timeout_ms = 10000
        self.kafka_client = None
        self.running = True
        self.sc = None
        metrics_tags = {"client-id": self.config["client_id"]}
        metric_config = MetricConfig(samples=2, time_window_ms=30000, tags=metrics_tags)
        self._metrics = Metrics(metric_config, reporters=[])
        self.schema_coordinator_ready = Event()
        self.log = logging.getLogger("MasterCoordinator")

    def init_kafka_client(self):
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
            self.log.warning("No Brokers available yet, retrying init_kafka_client()")
            time.sleep(2.0)
        return False

    def init_schema_coordinator(self):
        session_timeout_ms = self.config["session_timeout_ms"]
        self.sc = SchemaCoordinator(
            client=self.kafka_client,
            metrics=self._metrics,
            group_id=self.config["group_id"],
            session_timeout_ms=session_timeout_ms,
            request_timeout_ms=max(session_timeout_ms, KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"]),
        )
        self.sc.election_strategy = self.config["master_election_strategy"]
        self.sc.hostname = self.config["advertised_hostname"]
        self.sc.port = self.config["port"]
        self.sc.scheme = "http"
        self.sc.master_eligibility = self.config["master_eligibility"]
        self.schema_coordinator_ready.set()

    def get_master_info(self) -> Tuple[bool, Optional[str]]:
        """Return whether we're the master, and the actual master url that can be used if we're not"""
        self.schema_coordinator_ready.wait()
        return self.sc.are_we_master, self.sc.master_url

    def close(self):
        self.log.info("Closing master_coordinator")
        self.running = False

    def run(self):
        _hb_interval = 3.0
        while self.running:
            try:
                if not self.kafka_client:
                    if self.init_kafka_client() is False:
                        continue
                if not self.sc:
                    self.init_schema_coordinator()
                    _hb_interval = self.sc.config["heartbeat_interval_ms"] / 1000

                self.sc.ensure_active_group()
                self.sc.poll_heartbeat()
                self.log.debug("We're master: %r: master_uri: %r", self.sc.are_we_master, self.sc.master_url)
                time.sleep(min(_hb_interval, self.sc.time_to_next_heartbeat()))
            except:  # pylint: disable=bare-except
                self.log.exception("Exception in master_coordinator")
                time.sleep(1.0)

        if self.sc:
            self.sc.close()

        if self.kafka_client:
            self.kafka_client.close()
