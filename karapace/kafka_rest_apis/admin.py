from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import for_code, UnrecognizedBrokerVersion
from kafka.future import Future
from kafka.protocol.admin import DescribeConfigsRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from threading import Lock
from typing import List

import logging
import six


class KafkaRestAdminClient(KafkaAdminClient):
    def __init__(self, **configs):
        super().__init__(**configs)
        self.client_lock = Lock()
        self.first_boot = True
        self.log = logging.getLogger("AdminClient")

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

    def new_topic(self, name: str):
        self.create_topics([NewTopic(name, 1, 1)])

    def cluster_metadata(self, topics: List[str] = None) -> dict:
        """List all kafka topics."""
        node_found = True
        metadata_version = self._matching_api_version(MetadataRequest)
        if metadata_version > 6 or metadata_version < 1:
            raise UnrecognizedBrokerVersion(
                "Kafka Admin interface cannot determine the controller using MetadataRequest_v{}.".format(metadata_version)
            )
        # update_metadata on cluster only works with v1 type metadata
        request = MetadataRequest[1](topics=topics)
        with self.client_lock:
            node_id = self._client.least_loaded_node()
            # normal admin code will call poll on client not ready which will sometime block indefinitely on select
            if not self.first_boot or not self._client.ready(node_id):
                #  pylint: disable=protected-access
                node_id = list(self._client.cluster._bootstrap_brokers.keys())[0]
                self.first_boot = False
                node_found = False
            future = self._send_request_to_node(node_id, request)
            self.log.info("Retrieved metadata using node id %s", node_id)
        self._wait_for_futures([future])
        response = future.value
        if not node_found:
            self._client.cluster.update_metadata(response)
        return self._make_metadata_response(response)

    @staticmethod
    def _make_metadata_response(metadata):
        resp_brokers = metadata.brokers
        brokers = set()
        for b in resp_brokers:
            node_id, _, _, _ = b
            brokers.add(node_id)
        resp = {"topics": {}, "brokers": list(brokers)}
        if not metadata.topics:
            return resp

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
                    topic_response["replicas"].append({
                        "broker": node,
                        "leader": node == leader_id,
                        "in_sync": node in isr_nodes
                    })
                topic_data.append(topic_response)
            resp["topics"][topic] = {"partitions": topic_data}
        return resp

    def make_offsets_request(self, topic: str, partition_id: int, timestamp: int) -> Future:
        v = self._matching_api_version(OffsetRequest)
        if v == 0:
            request = OffsetRequest[0](-1, list(six.iteritems({topic: [(partition_id, timestamp, 1)]})))
        elif v == 1:
            request = OffsetRequest[1](-1, list(six.iteritems({topic: [(partition_id, timestamp)]})))
        else:
            request = OffsetRequest[2](-1, 1, list(six.iteritems({topic: [(partition_id, timestamp)]})))

        future = self._send_request_to_node(self._client.least_loaded_node(), request)
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
            rv = {
                "beginning_offset": beginning_partitions[0][3],
                "end_offset": end_partitions[0][3],
            }
        return rv
