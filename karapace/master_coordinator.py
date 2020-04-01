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
from kafka.errors import NoBrokersAvailable, NodeNotReadyError
from kafka.metrics import MetricConfig, Metrics
from karapace.config import create_ssl_context

import asyncio
import json
import logging

# SR group errors
NO_ERROR = 0
DUPLICATE_URLS = 1


def get_identity_url(scheme, host, port):
    return "{}://{}:{}".format(scheme, host, port)


class RoundRobinJsonAssignor(RoundRobinPartitionAssignor):
    def __init__(self, hostname, port, scheme):
        self.hostname = hostname
        self.port = port
        self.scheme = scheme

    def metadata(self, topics):
        return json.dumps({
            "version": 1,
            "host": self.hostname,
            "port": self.port,
            "scheme": self.scheme,
            "master_eligibility": True
        })


class SchemaCoordinator(GroupCoordinator):
    hostname = None
    port = None
    scheme = None
    master = None
    master_url = None
    log = logging.getLogger("SchemaCoordinator")

    @staticmethod
    def get_identity(*, host, port, scheme, json_encode=True):
        res = {"version": 1, "host": host, "port": port, "scheme": scheme, "master_eligibility": True}
        if json_encode:
            return json.dumps(res)
        return res

    def group_protocols(self):
        return [("v0", self.get_identity(host=self.hostname, port=self.port, scheme=self.scheme))]

    @asyncio.coroutine
    def _perform_assignment(self, leader_id, assignment_strategy, members):
        self.log.info("Creating assignment: %r, strategy: %r, members: %r", leader_id, assignment_strategy, members)
        self.master = None
        error = NO_ERROR
        urls = {}
        for member_id, member_data in members:
            self.log.info(member_data.decode("utf8"))
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
        if member_assignment["master"] == member_id:
            self.master_url = master_url
            self.master = True
        else:
            self.master_url = master_url
            self.master = False


class MasterCoordinator:
    """Handles schema topic creation and master election"""

    def __init__(self, config):
        self.config = config
        self.api_version_auto_timeout_ms = 30000
        self.timeout_ms = 10000
        self.kafka_client = None
        self.done = asyncio.Future()
        self.running = asyncio.Future()
        self.sc = None
        self.subscription = SubscriptionState(loop=asyncio.get_event_loop())
        self.fetcher = None
        self.subscription.subscribe({self.config["topic_name"]})
        metrics_tags = {"client-id": self.config["client_id"]}
        metric_config = MetricConfig(samples=2, time_window_ms=30000, tags=metrics_tags)
        self._metrics = Metrics(metric_config, reporters=[])
        self.lock = asyncio.Lock()
        self.log = logging.getLogger("MasterCoordinator")

    async def init_kafka_client(self):
        try:
            self.kafka_client = AIOKafkaClient(
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_context=None if self.config["security_protocol"] != "SSL" else create_ssl_context(self.config),
                metadata_max_age_ms=self.config["metadata_max_age_ms"],
                loop=asyncio.get_event_loop(),
            )
            await self.kafka_client.bootstrap()
            self.log.info("Kafka client ready")
            self.fetcher = Fetcher(self.kafka_client, self.subscription, loop=asyncio.get_event_loop())
            return True
        except (NodeNotReadyError, NoBrokersAvailable):
            self.log.warning("No Brokers available yet, retrying init_kafka_client()")
            await asyncio.sleep(2.0)
        return False

    async def init_schema_coordinator(self):
        self.log.info("initializing schema coordinator")
        self.sc = SchemaCoordinator(
            client=self.kafka_client,
            group_id=self.config["group_id"],
            loop=asyncio.get_event_loop(),
            subscription=self.subscription,
            assignors=(
                RoundRobinJsonAssignor(hostname=self.config["advertised_hostname"], port=self.config["port"], scheme="http"),
            ),
        )
        self.sc.hostname = self.config["advertised_hostname"]
        self.sc.port = self.config["port"]
        self.sc.scheme = "http"
        self.lock.release()  # self.sc now exists, we get to release the lock

    async def get_master_info(self):
        """Return whether we're the master, and the actual master url that can be used if we're not"""
        async with self.lock:
            return self.sc.master, self.sc.master_url

    async def close(self):
        self.log.info("Closing master_coordinator")
        self.done.set_result(None)
        await self.sc.close()
        await self.fetcher.close()
        await self.kafka_client.close()

    async def run(self):
        await self.lock.acquire()
        while True:
            try:
                if not self.kafka_client:
                    if not await self.init_kafka_client():
                        self.log.error("could not init kafka client")
                        continue
                if not self.sc:
                    await self.init_schema_coordinator()
                # Both tasks already happen in the background task in the base class, so here we just check for
                # Close conditions
                self.log.info("We're master: %r: master_uri: %r", self.sc.master, self.sc.master_url)
                self.running.set_result(None)
                await self.done
                break
            except:  # pylint: disable=bare-except
                self.log.exception("Exception in master_coordinator")
                await asyncio.sleep(1.0)

        if self.sc:
            await self.sc.close()

        if self.kafka_client:
            await self.kafka_client.close()
