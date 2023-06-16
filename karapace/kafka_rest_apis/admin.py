"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import Cancelled, for_code, UnrecognizedBrokerVersion
from kafka.future import Future
from kafka.protocol.admin import DescribeConfigsRequest
from kafka.protocol.metadata import MetadataRequest, MetadataResponse_v1
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy

import logging

LOG = logging.getLogger(__name__)


class KafkaRestAdminClient(KafkaAdminClient):
    def get_topic_config(self, topic: str) -> dict:
        config_version = self._matching_api_version(DescribeConfigsRequest)
        req_cfgs = [ConfigResource(ConfigResourceType.TOPIC, topic)]
        cfgs = self.describe_configs(req_cfgs)
        assert len(cfgs) == 1
        assert len(cfgs[0].resources) == 1
        err, _, _, _, config_values = cfgs[0].resources[0]
        if err != 0:
            raise for_code(err)
        topic_config = {}
        for cv in config_values:
            if config_version == 0:
                name, val, _, _, _ = cv
            else:
                name, val, _, _, _, _ = cv
            topic_config[name] = val
        return topic_config

    def new_topic(self, name: str) -> None:
        self.create_topics([NewTopic(name, 1, 1)])

    def cluster_metadata(self, topics: list[str] | None = None, retries: int = 0) -> dict:
        """Fetch cluster metadata and topic information for given topics or all topics if not given."""
        metadata_version = self._matching_api_version(MetadataRequest)
        if metadata_version > 6 or metadata_version < 1:
            raise UnrecognizedBrokerVersion(
                f"Kafka Admin interface cannot determine the controller using MetadataRequest_v{metadata_version}."
            )
        request = MetadataRequest[1](topics=topics)
        future = self._send_request_to_least_loaded_node(request)
        try:
            self._wait_for_futures([future])
        except Cancelled:
            if retries > 3:
                raise
            LOG.debug("Retrying metadata with %d retires", retries)
            return self.cluster_metadata(topics, retries + 1)
        return self._make_metadata_response(future.value)

    @staticmethod
    def _make_metadata_response(metadata: MetadataResponse_v1) -> dict:
        resp_brokers = metadata.brokers
        brokers = set()
        for b in resp_brokers:
            node_id, _, _, _ = b
            brokers.add(node_id)
        if not metadata.topics:
            return {"topics": {}, "brokers": list(brokers)}

        topics: dict[str, dict] = {}
        for tup in metadata.topics:
            err, topic, _, partitions = tup
            if err:
                raise for_code(err)
            topic_data = []
            for part in partitions:
                _, partition_index, leader_id, replica_nodes, isr_nodes = part
                isr_nodes = set(isr_nodes)
                topic_response = {"partition": partition_index, "leader": leader_id, "replicas": []}
                for node in replica_nodes:
                    topic_response["replicas"].append(
                        {"broker": node, "leader": node == leader_id, "in_sync": node in isr_nodes}
                    )
                topic_data.append(topic_response)
            topics[topic] = {"partitions": topic_data}
        return {"topics": topics, "brokers": list(brokers)}

    def make_offsets_request(self, topic: str, partition_id: int, timestamp: int) -> Future:
        v = self._matching_api_version(OffsetRequest)
        replica_id = -1
        if v == 0:
            max_offsets = 1
            partitions_v0 = [(partition_id, timestamp, max_offsets)]
            topics_v0 = [(topic, partitions_v0)]
            request = OffsetRequest[0](replica_id, topics_v0)
        elif v == 1:
            partitions_v1 = [(partition_id, timestamp)]
            topics_v1 = [(topic, partitions_v1)]
            request = OffsetRequest[1](replica_id, topics_v1)
        else:
            isolation_level = 1
            partitions = [(partition_id, timestamp)]
            topics = [(topic, partitions)]
            request = OffsetRequest[2](replica_id, isolation_level, topics)

        future = self._send_request_to_least_loaded_node(request)
        return future

    def get_offsets(self, topic: str, partition_id: int) -> dict:
        beginning_f = self.make_offsets_request(topic, partition_id, OffsetResetStrategy.EARLIEST)
        end_f = self.make_offsets_request(topic, partition_id, OffsetResetStrategy.LATEST)
        self._wait_for_futures([beginning_f, end_f])
        beginning_resp = beginning_f.value
        end_resp = end_f.value
        v = self._matching_api_version(OffsetRequest)
        assert len(beginning_resp.topics) == 1
        assert len(end_resp.topics) == 1
        _, beginning_partitions = beginning_resp.topics[0]
        _, end_partitions = end_resp.topics[0]

        assert len(beginning_partitions) == 1
        assert len(end_partitions) == 1
        if v == 0:
            assert len(beginning_partitions[0][2]) == 1
            assert partition_id == beginning_partitions[0][0]
            assert partition_id == end_partitions[0][0]
            start_err = beginning_partitions[0][1]
            end_err = beginning_partitions[0][1]
            for e in [start_err, end_err]:
                if e != 0:
                    raise for_code(e)
            rv = {
                "beginning_offset": beginning_partitions[0][2][0],
                "end_offset": end_partitions[0][2][0],
            }
        else:
            start_err = beginning_partitions[0][1]
            end_err = beginning_partitions[0][1]
            for e in [start_err, end_err]:
                if e != 0:
                    raise for_code(e)
            rv = {
                "beginning_offset": beginning_partitions[0][3],
                "end_offset": end_partitions[0][3],
            }
        return rv
