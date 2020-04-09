"""
karapace - master coordinator

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.consumer.subscription_state import SubscriptionState
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.errors import KafkaConnectionError, NoBrokersAvailable, NodeNotReadyError
from kafka.metrics import MetricConfig, Metrics
from karapace.config import create_ssl_context

import asyncio
import json
import logging

log = logging.getLogger(__name__)
# SR group errors
NO_ERROR = 0
DUPLICATE_URLS = 1


def get_identity_url(scheme, host, port):
    return "{}://{}:{}".format(scheme, host, port)


class RoundRobinJsonAssignor(RoundRobinPartitionAssignor):
    name = "sr"

    def __init__(self, hostname, port, scheme):
        self.hostname = hostname
        self.port = port
        self.scheme = scheme

    def metadata(self, topics):
        rv = json.dumps({
            "version": 1,
            "host": self.hostname,
            "port": self.port,
            "scheme": self.scheme,
            "master_eligibility": True
        }).encode("utf8")
        log.debug("get metadata %s", rv.decode())
        return rv


class SchemaCoordinator(GroupCoordinator):
    hostname = None
    port = None
    scheme = None
    master = None
    master_url = None
    log = logging.getLogger("SchemaCoordinator")

    def __init__(self, client, subscription, *, loop, lock, assignors, group_id):
        super().__init__(client, subscription, loop=loop, assignors=assignors, group_id=group_id)
        self.lock = lock
        self.first_join = self._loop.create_future()

    @staticmethod
    def get_identity(*, host, port, scheme, json_encode=True):
        res = {"version": 1, "host": host, "port": port, "scheme": scheme, "master_eligibility": True}
        if json_encode:
            return json.dumps(res)
        return res

    @asyncio.coroutine
    def _perform_assignment(self, leader_id, assignment_strategy, members):
        self.log.info("Creating assignment: %r, strategy: %r, members: %r", leader_id, assignment_strategy, members)
        self.master = None
        error = NO_ERROR
        urls = {}
        for member_id, member_data in members:
            self.log.info(member_data.decode("utf8"))
            member_identity = json.loads(member_data.decode("utf8"))
            self.log.info("gathered identity: %r", member_identity)
            if member_identity["master_eligibility"] is True:
                urls[get_identity_url(member_identity["scheme"], member_identity["host"],
                                      member_identity["port"])] = (member_id, member_data)
        self.log.info("Gathered urls %r", urls)
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

    async def _on_join_complete(self, generation, member_id, protocol, member_assignment_bytes):
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
        async with self.lock:
            if member_assignment["master"] == member_id:
                self.master_url = master_url
                self.master = True
            else:
                self.master_url = master_url
                self.master = False
        if not self.first_join.done():
            self.first_join.set_result(None)


class MasterCoordinator:
    """Handles schema topic creation and master election"""

    def __init__(self, config, *, loop=None):
        self.config = config
        self.api_version_auto_timeout_ms = 30000
        self.timeout_ms = 10000
        self.kafka_client = None
        self.sc = None
        self.fetcher = None
        self.loop = loop or asyncio.get_event_loop()
        self.exiting = asyncio.Event()
        self.done = asyncio.Event()
        self.running = asyncio.Event()
        self.subscription = SubscriptionState(loop=self.loop)
        self.subscription.subscribe({self.config["topic_name"]})
        metrics_tags = {"client-id": self.config["client_id"]}
        metric_config = MetricConfig(samples=2, time_window_ms=30000, tags=metrics_tags)
        self._metrics = Metrics(metric_config, reporters=[])
        self.init_lock = asyncio.Lock()
        self.master_info_lock = asyncio.Lock()
        self.log = logging.getLogger("MasterCoordinator")
        self.running_task = self.loop.create_task(self.run())

    async def init_kafka_client(self):
        while True:
            try:
                if not self.kafka_client:
                    self.kafka_client = AIOKafkaClient(
                        bootstrap_servers=self.config["bootstrap_uri"],
                        client_id=self.config["client_id"],
                        security_protocol=self.config["security_protocol"],
                        ssl_context=None if self.config["security_protocol"] != "SSL" else create_ssl_context(self.config),
                        metadata_max_age_ms=self.config["metadata_max_age_ms"],
                        loop=self.loop,
                    )
                    continue
                # i really want to avoid those post bootstrap failures
                if not self.kafka_client.cluster.brokers():
                    log.info("Forcing metadata reload / bootstrapping")
                    await self.kafka_client.bootstrap()
                    log.info("Cluster state after (re-)bootstrap: %r", self.kafka_client.cluster.brokers())
                    await asyncio.sleep(1.0)
                    continue
                self.fetcher = Fetcher(self.kafka_client, self.subscription, loop=self.loop)
                log.debug("Master coordinator initialized")
                break
            except (NodeNotReadyError, NoBrokersAvailable, KafkaConnectionError) as e:
                self.log.warning("No Brokers available yet, retrying init_kafka_client(): %r", e)
                await asyncio.sleep(1.0)

    async def init_schema_coordinator(self):
        while True:
            try:
                self.log.debug("initializing schema coordinator")
                self.sc = SchemaCoordinator(
                    client=self.kafka_client,
                    group_id=self.config["group_id"],
                    loop=self.loop,
                    lock=self.master_info_lock,
                    subscription=self.subscription,
                    assignors=(
                        RoundRobinJsonAssignor(
                            hostname=self.config["advertised_hostname"], port=self.config["port"], scheme="http"
                        ),
                    ),
                )
                self.sc.hostname = self.config["advertised_hostname"]
                self.sc.port = self.config["port"]
                self.sc.scheme = "http"
                self.log.debug("schema coordinator initialized")
                break
            except:  # pylint: disable=bare-except
                log.exception("Error initializing schema coordinator")

    async def get_master_info(self):
        """Return whether we're the master, and the actual master url that can be used if we're not"""
        async with self.init_lock, self.master_info_lock:
            return self.sc.master, self.sc.master_url

    async def close(self):
        self.log.info("Closing master_coordinator")
        self.exiting.set()
        if self.sc:
            await self.sc.close()
        if self.fetcher:
            await self.fetcher.close()
        if self.kafka_client:
            await self.kafka_client.close()
        try:
            self.exiting.set()
        except asyncio.InvalidStateError:
            pass
        await self.done.wait()

    async def run(self):
        async with self.init_lock:
            await self.init_kafka_client()
            await self.init_schema_coordinator()
            self.log.debug("awaiting on first join, for valid coordinator info")
            await self.sc.first_join
        self.log.info("We're master: %r: master_uri: %r", self.sc.master, self.sc.master_url)
        self.running.set()
        try:
            await self.exiting.wait()
            log.info("Preparing to exit MC")
        except (RuntimeError, GeneratorExit):
            self.log.exception("Exception while trying to exit")
        self.done.set()
