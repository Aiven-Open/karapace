from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import for_code, UnknownTopicOrPartitionError, UnrecognizedBrokerVersion
from kafka.protocol.admin import DescribeConfigsRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from karapace import version as karapace_version
from karapace.karapace import KarapaceBase
from threading import Lock

import argparse
import os
import six
import sys


class KafkaRestAdminClient(KafkaAdminClient):
    def get_topic_config(self, topic):
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

    def cluster_metadata(self, topics=None):
        """List all kafka topics."""
        resp = {"topics": {}}
        brokers = set()
        metadata_version = self._matching_api_version(MetadataRequest)
        if 1 <= metadata_version <= 6:
            if not topics:
                request = MetadataRequest[metadata_version]()
            else:
                request = MetadataRequest[metadata_version](topics=topics)
            future = self._send_request_to_node(self._client.least_loaded_node(), request)
            self._wait_for_futures([future])
            response = future.value
            resp_brokers = response.brokers
            for b in resp_brokers:
                if metadata_version == 0:
                    node_id, _, _ = b
                else:
                    node_id, _, _, _ = b
                brokers.add(node_id)
            resp["brokers"] = list(brokers)
            if not response.topics:
                return resp

            for tup in response.topics:
                if response.API_KEY != 0:
                    err, topic, _, partitions = tup
                else:
                    err, topic, partitions = tup
                if err:
                    raise for_code(err)
                topic_data = []
                for part in partitions:
                    if metadata_version <= 4:
                        _, partition_index, leader_id, replica_nodes, isr_nodes = part
                    else:
                        _, partition_index, leader_id, replica_nodes, isr_nodes, _ = part
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
        raise UnrecognizedBrokerVersion(
            "Kafka Admin interface cannot determine the controller using MetadataRequest_v{}.".format(metadata_version)
        )

    def make_offsets_request(self, topic, partition_id, timestamp):
        # hacky way of signaling we only support v1
        v = self._matching_api_version(OffsetRequest)
        if v == 0:
            request = OffsetRequest[0](-1, list(six.iteritems({topic: [(partition_id, timestamp, 1)]})))
        elif v == 1:
            request = OffsetRequest[1](-1, list(six.iteritems({topic: [(partition_id, timestamp)]})))
        else:
            request = OffsetRequest[2](-1, 1, list(six.iteritems({topic: [(partition_id, timestamp)]})))

        future = self._send_request_to_node(self._client.least_loaded_node(), request)
        return future

    def get_offsets(self, topic, partition_id):
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
        if any(val < 0 for val in rv.values()):
            raise UnknownTopicOrPartitionError("Invalid values for offsets found")
        return rv


class KafkaRest(KarapaceBase):
    def __init__(self, config):
        super().__init__(config)
        self.kafka_timeout = 10
        self.admin_client = None
        self.producer_lock = Lock()
        self.metadata_cache = None
        self.executor = ThreadPoolExecutor(max_workers=2)
        # Brokers
        self.route("/brokers", callback=self.list_brokers, method="GET", schema_request=True)
        # Consumers
        self.route("/consumers/<group_name:path>", callback=self.placeholder, method="GET")
        self.route("/consumers/<group_name:path>/instances/<instance:path>", callback=self.placeholder, method="DELETE")
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets", callback=self.placeholder, method="POST"
        )
        self.route("/consumers/<group_name:path>/instances/<instance:path>/offsets", callback=self.placeholder, method="GET")
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription", callback=self.placeholder, method="POST"
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription", callback=self.placeholder, method="GET"
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.placeholder,
            method="DELETE"
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments", callback=self.placeholder, method="POST"
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments", callback=self.placeholder, method="GET"
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions", callback=self.placeholder, method="POST"
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/beginning",
            callback=self.placeholder,
            method="POST"
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/end", callback=self.placeholder, method="POST"
        )
        self.route("/consumers/<group_name:path>/instances/<instance:path>/records", callback=self.placeholder, method="GET")

        # Partitions
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>/offsets",
            callback=self.partition_offsets,
            method="GET",
            schema_request=True
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.partition_details,
            method="GET",
            schema_request=True
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.placeholder,
            method="POST",
            schema_request=True
        )
        self.route("/topics/<topic:path>/partitions", callback=self.list_partitions, method="GET", schema_request=True)
        # Topics
        self.route("/topics", callback=self.list_topics, method="GET", schema_request=True)
        self.route("/topics/<topic:path>", callback=self.topic_details, method="GET", schema_request=True)
        self.route("/topics/<topic:path>", callback=self.placeholder, method="POST", schema_request=True)
        self.init_admin_client()
        self._create_producer()

    def init_admin_client(self):
        self.admin_client = KafkaRestAdminClient(
            bootstrap_servers=self.config["bootstrap_uri"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            api_version=(1, 0, 0),
            metadata_max_age_ms=self.config["metadata_max_age_ms"],
        )

    def close(self):
        super().close()
        self.admin_client.close()
        self.admin_client = None

    def placeholder(self):
        raise NotImplementedError("write it !")

    def list_topics(self, content_type):
        metadata = self.admin_client.cluster_metadata()
        # hacky way to get some info for more detailed 404's ... do not use otherwise
        self.metadata_cache = metadata
        topics = list(metadata["topics"].keys())
        self.r(topics, content_type)

    def topic_details(self, content_type, *, topic):
        met_f = self.executor.submit(self.admin_client.cluster_metadata, [topic])
        config_f = self.executor.submit(self.admin_client.get_topic_config, topic)
        try:
            metadata = met_f.result()
            config = config_f.result()
            if topic not in metadata["topics"]:
                self.r(
                    body={
                        "error_code": 40401,
                        "message": "Topic %r not found" % topic
                    },
                    content_type=content_type,
                    status=404
                )

            data = metadata["topics"][topic]
            data["name"] = topic
            data["configs"] = config
            self.r(data, content_type)
        except UnknownTopicOrPartitionError:
            self.r(
                body={
                    "error_code": 40401,
                    "message": "Topic %r not found" % topic
                }, content_type=content_type, status=404
            )

    def list_partitions(self, content_type, *, topic):
        try:
            topic_details = self.admin_client.cluster_metadata([topic])
            self.r(topic_details["topics"][topic]["partitions"], content_type)
        except UnknownTopicOrPartitionError:
            self.r(
                body={
                    "error_code": 40401,
                    "message": "Topic %r not found" % topic
                }, content_type=content_type, status=404
            )

    def partition_details(self, content_type, *, topic, partition_id):
        try:
            partition_id = int(partition_id)
        except ValueError:
            self.r(
                body={
                    "error_code": 40402,
                    "message": "Partition %r not found" % partition_id
                },
                content_type=content_type,
                status=404
            )
        try:
            partitions = self.admin_client.cluster_metadata([topic])["topics"][topic]["partitions"]
            for p in partitions:
                if p["partition"] == partition_id:
                    self.r(p, content_type)
            self.r(
                body={
                    "error_code": 40402,
                    "message": "Partition %r not found" % partition_id
                },
                content_type=content_type,
                status=404
            )
        except UnknownTopicOrPartitionError:
            self.r(
                body={
                    "error_code": 40401,
                    "message": "Topic %r not found" % topic
                }, content_type=content_type, status=404
            )

    def partition_offsets(self, content_type, *, topic, partition_id):
        try:
            partition_id = int(partition_id)
        except ValueError:
            self.r(
                body={
                    "error_code": 40402,
                    "message": "Partition %r not found" % partition_id
                },
                content_type=content_type,
                status=404
            )
        try:
            self.r(self.admin_client.get_offsets(topic, partition_id), content_type)
        except UnknownTopicOrPartitionError:
            # The other option is to actually do a topics request on failure
            if not self.metadata_cache or topic not in self.metadata_cache["topics"]:
                self.r(
                    body={
                        "error_code": 40401,
                        "message": "Topic %r not found" % topic
                    },
                    content_type=content_type,
                    status=404
                )
            self.r(
                body={
                    "error_code": 40402,
                    "message": "Partition %r not found" % partition_id
                },
                content_type=content_type,
                status=404
            )

    def list_brokers(self, content_type):  # pylint: disable=unused-argument
        metadata = self.admin_client.cluster_metadata()
        metadata.pop("topics")
        self.r(metadata, content_type)


def main():
    parser = argparse.ArgumentParser(prog="karapace rest", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument("--version", action="version", help="show program version", version=karapace_version.__version__)
    parser.add_argument("config_file", help="configuration file path")
    arg = parser.parse_args()

    if not os.path.exists(arg.config_file):
        print("Config file: {} does not exist, exiting".format(arg.config_file))
        return 1

    kc = KafkaRest(arg.config_file)
    try:
        return kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise


if __name__ == "__main__":
    sys.exit(main())
