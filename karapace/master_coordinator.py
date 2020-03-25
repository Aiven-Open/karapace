"""
karapace - master coordinator

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from kafka.client_async import KafkaClient
from kafka.coordinator.base import BaseCoordinator
from kafka.errors import NoBrokersAvailable, NodeNotReadyError
from kafka.metrics import MetricConfig, Metrics
from threading import Lock, Thread

import json
import logging
import time

# SR group errors
NO_ERROR = 0
DUPLICATE_URLS = 1


def get_identity_url(scheme, host, port):
    return "{}://{}:{}".format(scheme, host, port)


class SchemaCoordinator(BaseCoordinator):
    hostname = None
    port = None
    scheme = None
    master = None
    master_url = None
    log = logging.getLogger("SchemaCoordinator")

    def protocol_type(self):
        return "sr"

    @staticmethod
    def get_identity(*, host, port, scheme, json_encode=True):
        res = {"version": 1, "host": host, "port": port, "scheme": scheme, "master_eligibility": True}
        if json_encode:
            return json.dumps(res)
        return res

    def group_protocols(self):
        return [("v0", self.get_identity(host=self.hostname, port=self.port, scheme=self.scheme))]

    def _perform_assignment(self, leader_id, protocol, members):
        self.log.info("Creating assignment: %r, protocol: %r, members: %r", leader_id, protocol, members)
        self.master = None
        error = NO_ERROR
        urls = {}
        for member_id, member_data in members:
            member_identity = json.loads(member_data.decode("utf8"))
            if member_identity["master_eligibility"] is True:
                urls[get_identity_url(member_identity["scheme"], member_identity["host"],
                                      member_identity["port"])] = (member_id, member_data)

        lowest_url = sorted(urls)[0]
        schema_master_id, member_data = urls[lowest_url]
        member_identity = json.loads(member_data.decode("utf8"))
        identity = self.get_identity(
            host=member_identity["host"],
            port=member_identity["port"],
            scheme=member_identity["scheme"],
            json_encode=False,
        )
        self.log.info("Chose: %r with url: %r as the master", schema_master_id, lowest_url)
        self.master_url = lowest_url

        assignments = {}
        for member_id, member_data in members:
            assignments[member_id] = json.dumps({"master": schema_master_id, "master_identity": identity, "error": error})
        return assignments

    def _on_join_prepare(self, generation, member_id):
        """Invoked prior to each group join or rejoin."""
        # needs to be implemented in our class for pylint to be satisfied

    def _on_join_complete(self, generation, member_id, protocol, member_assignment_bytes):
        self.log.info(
            "Join complete, generation %r, member_id: %r, protocol: %r, member_assignment_bytes: %r", generation, member_id,
            protocol, member_assignment_bytes
        )
        member_assignment = json.loads(member_assignment_bytes.decode("utf8"))
        member_identity = member_assignment["master_identity"]

        master_url = get_identity_url(
            scheme=member_identity["scheme"],
            host=member_identity["host"],
            port=member_identity["port"],
        )
        if member_assignment["master"] == member_id:
            self.master_url = master_url
            self.master = True
        else:
            self.master_url = master_url
            self.master = False
        return super(SchemaCoordinator, self)._on_join_complete(generation, member_id, protocol, member_assignment_bytes)

    def _on_join_follower(self):
        self.log.info("We are a follower, not a master")
        return super(SchemaCoordinator, self)._on_join_follower()


class MasterCoordinator(Thread):
    """Handles schema topic creation and master election"""
    def __init__(self, config):
        Thread.__init__(self)
        self.config = config
        self.api_version_auto_timeout_ms = 30000
        self.timeout_ms = 10000
        self.kafka_client = None
        self.running = True
        self.sc = None
        metrics_tags = {"client-id": self.config["client_id"]}
        metric_config = MetricConfig(samples=2, time_window_ms=30000, tags=metrics_tags)
        self._metrics = Metrics(metric_config, reporters=[])
        self.lock = Lock()
        self.lock.acquire()
        self.log = logging.getLogger("MasterCoordinator")

    def init_kafka_client(self):
        try:
            self.kafka_client = KafkaClient(
                api_version_auto_timeout_ms=self.api_version_auto_timeout_ms,
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
                metadata_max_age_ms=self.config["metadata_max_age_ms"],
            )
            return True
        except (NodeNotReadyError, NoBrokersAvailable):
            self.log.warning("No Brokers available yet, retrying init_kafka_client()")
            time.sleep(2.0)
        return False

    def init_schema_coordinator(self):
        self.sc = SchemaCoordinator(
            client=self.kafka_client,
            metrics=self._metrics,
            group_id=self.config["group_id"],
        )
        self.sc.hostname = self.config["advertised_hostname"]
        self.sc.port = self.config["port"]
        self.sc.scheme = "http"
        self.lock.release()  # self.sc now exists, we get to release the lock

    def get_master_info(self):
        """Return whether we're the master, and the actual master url that can be used if we're not"""
        with self.lock:
            return self.sc.master, self.sc.master_url

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
                self.log.debug("We're master: %r: master_uri: %r", self.sc.master, self.sc.master_url)
                time.sleep(min(_hb_interval, self.sc.time_to_next_heartbeat()))
            except:  # pylint: disable=bare-except
                self.log.exception("Exception in master_coordinator")
                time.sleep(1.0)

        if self.sc:
            self.sc.close()

        if self.kafka_client:
            self.kafka_client.close()
