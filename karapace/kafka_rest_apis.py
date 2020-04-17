from binascii import Error as B64DecodeError
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import (
    BrokerResponseError, for_code, KafkaTimeoutError, UnknownTopicOrPartitionError, UnrecognizedBrokerVersion
)
from kafka.protocol.admin import DescribeConfigsRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from karapace import version as karapace_version
from karapace.karapace import KarapaceBase
from threading import Lock
from typing import List, Optional, Tuple

import argparse
import base64
import json
import os
import six
import sys

FILTERED_TOPICS = {"__consumer_offsets"}


class FormatError(Exception):
    pass


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
                if topic in FILTERED_TOPICS:
                    continue
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
        self._cluster_metadata = None
        self.admin_client = None
        self.producer_lock = Lock()
        self.metadata_cache = None
        self.executor = ThreadPoolExecutor(max_workers=2)
        # Brokers
        self.route("/brokers", callback=self.list_brokers, method="GET", rest_request=True)
        # Consumers
        self.route("/consumers/<group_name:path>", callback=self.placeholder, method="GET", rest_request=True)
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>",
            callback=self.placeholder,
            method="DELETE",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/offsets",
            callback=self.placeholder,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.placeholder,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/subscription",
            callback=self.placeholder,
            method="DELETE",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/assignments",
            callback=self.placeholder,
            method="GET",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/beginning",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/positions/end",
            callback=self.placeholder,
            method="POST",
            rest_request=True
        )
        self.route(
            "/consumers/<group_name:path>/instances/<instance:path>/records",
            callback=self.placeholder,
            method="GET",
            rest_request=True
        )

        # Partitions
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>/offsets",
            callback=self.partition_offsets,
            method="GET",
            rest_request=True
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.partition_details,
            method="GET",
            rest_request=True
        )
        self.route(
            "/topics/<topic:path>/partitions/<partition_id:path>",
            callback=self.partition_publish,
            method="POST",
            rest_request=True
        )
        self.route("/topics/<topic:path>/partitions", callback=self.list_partitions, method="GET", rest_request=True)
        # Topics
        self.route("/topics", callback=self.list_topics, method="GET", rest_request=True)
        self.route("/topics/<topic:path>", callback=self.topic_details, method="GET", rest_request=True)
        self.route("/topics/<topic:path>", callback=self.topic_publish, method="POST", rest_request=True)
        self.init_admin_client()
        self._create_producer()

    def cluster_metadata(self):
        # TODO -> Cache if necessary
        if not self._cluster_metadata:
            self._cluster_metadata = self.admin_client.cluster_metadata()["topics"]
        return self._cluster_metadata

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

    def placeholder(self, content_type):
        self.r(body={"error_code": 40401, "message": "Not implemented"}, content_type=content_type, status=404)

    def publish(self, topic: str, partition_id: Optional[str], content_type: str, formats: dict, data: dict):
        # TODO In order to support the other content types, the client and request need to be updated
        self.fail_for_unknown_topic(topic, content_type)
        if partition_id is not None:
            _ = self.get_partition_data_or_fail(topic, partition_id, content_type)
            # by this point the conversion should always succeed
            partition_id = int(partition_id)
        self.validate_request_format(data, formats, content_type)
        status = 200
        ser_format = formats["embedded_format"]
        response = self._prepare_publish_response_metadata(data, ser_format)
        prepared_records = []
        try:
            prepared_records = self._prepare_records(
                data=data,
                ser_format=ser_format,
                key_schema_id=response["key_schema_id"],
                value_schema_id=response["value_schema_id"],
                default_partition=partition_id
            )
        except (FormatError, B64DecodeError):
            self.r(
                body={
                    "error_code": 42205,
                    "message": "Request includes data improperly formatted given the format %r" % ser_format
                },
                content_type=content_type,
                status=422
            )
        for key, value, partition in prepared_records:
            publish_result = self.produce_message(topic=topic, key=key, value=value, partition=partition)
            if "error" in publish_result and status == 200:
                status = 500
            response["offsets"].append(publish_result)
        self.r(body=response, content_type=content_type, status=status)

    def partition_publish(self, topic, partition_id, content_type, formats, *, request):
        self.publish(topic, partition_id, content_type, formats, request.json)

    def topic_publish(self, topic, content_type, formats, *, request):
        self.publish(topic, None, content_type, formats, request.json)

    def _prepare_publish_response_metadata(self, data: dict, ser_format: str) -> dict:
        response = {
            "key_schema_id": data.get("key_schema_id"),
            "value_schema_id": data.get("value_schema_id"),
            "offsets": []
        }
        for prefix in ["key", "value"]:
            if not response["%s_schema_id" % prefix] and \
                    ser_format == "avro" and \
                    self.has_non_empty(data, prefix):
                response["%s_schema_id" % prefix] = self.get_id_for_schema(data.get("%s_schema" % prefix))
        return response

    def _prepare_records(
        self,
        data: dict,
        ser_format: str,
        key_schema_id: Optional[int],
        value_schema_id: Optional[int],
        default_partition: Optional[int] = None
    ) -> List[Tuple]:
        prepared_records = []
        for record in data["records"]:
            key = record.get("key")
            value = record.get("value")
            key = self.serialize(key, ser_format, key_schema_id)
            value = self.serialize(value, ser_format, value_schema_id)
            prepared_records.append((key, value, record.get("partition", default_partition)))
        return prepared_records

    def get_partition_data_or_fail(self, topic: str, partition: str, content_type: str) -> dict:
        try:
            partition = int(partition)
        except ValueError:
            self.r(
                body={
                    "error_code": 40402,
                    "message": "Partition id %r is badly formatted" % partition
                },
                content_type=content_type,
                status=404
            )
        try:
            partitions = self.cluster_metadata()[topic]["partitions"]
            for p in partitions:
                if p["partition"] == partition:
                    return p
            self.r(
                body={
                    "error_code": 40402,
                    "message": "Partition %r not found" % partition
                },
                content_type=content_type,
                status=404
            )
        except UnknownTopicOrPartitionError:
            self.r(
                body={
                    "error_code": 40402,
                    "message": "Partition %r not found" % partition
                },
                content_type=content_type,
                status=404
            )
        except KeyError:
            self.r(
                body={
                    "error_code": 40401,
                    "message": "Topic %r not found" % topic
                }, content_type=content_type, status=404
            )
        return {}

    def fail_for_unknown_topic(self, topic: str, content_type: str):
        if topic not in self.cluster_metadata():
            self.r(
                body={
                    "error_code": 40401,
                    "message": "Topic %s not found" % topic
                }, content_type=content_type, status=404
            )

    @staticmethod
    def has_non_empty(data: dict, key: str) -> bool:
        return any(key in item and item[key] for item in data["records"])

    def get_id_for_schema(self, schema: str) -> int:
        raise NotImplementedError("Need serializers")

    def serialize(self, obj=None, ser_format: str = None, schema_id: str = None) -> bytes:
        if not obj:
            return b''
        # not pretty
        if ser_format == "json" and isinstance(obj, (dict, list)):
            return json.dumps(obj).encode("utf8")
        if ser_format == "binary":
            return base64.b64decode(obj)
        if ser_format == "avro":
            return self.avro_serialize(obj, schema_id)
        raise FormatError("Unknown format: %r" % ser_format)

    def avro_serialize(self, obj, schema_id):
        raise NotImplementedError("Cannot serialize %r for schema %d" % (obj, schema_id))

    def validate_request_format(self, data: dict, formats: dict, content_type: str):
        if "records" not in data:
            self.r(
                body={
                    "error_code": 50001,  # Choose another code??
                    "message": "Invalid request format"
                },
                content_type=content_type,
                status=500
            )

        if "embedded_format" in formats and formats["embedded_format"] == "avro":
            for prefix, code in [("key", 42201), ("value", 42202)]:
                if self.has_non_empty(data, prefix) and \
                        ("%s_schema" % prefix not in data and "%s_schema_id" % prefix not in data):
                    self.r(
                        body={
                            "error_code": code,
                            "message": "Request includes %ss and uses a format that requires schemas but does not "
                            "include the %s_schema or %s_schema_id fields" % (prefix, prefix, prefix)
                        },
                        content_type=content_type,
                        status=422
                    )

    def produce_message(self, *, topic: str, key: bytes, value: bytes, partition: int = None) -> dict:
        try:
            with self.producer_lock:
                f = self.producer.send(topic, key=key, value=value, partition=partition)
                self.producer.flush()
            result = f.get()
            return {"offset": result.offset, "partition": result.topic_partition.partition}
        except AssertionError as e:
            self.log.exception("Invalid data")
            return {"error_code": 1, "error": str(e)}
        except KafkaTimeoutError:
            self.log.exception("Timed out waiting for publisher")
            # timeouts are retriable
            return {"error_code": 1, "error": "timed out waiting to publish message"}
        except BrokerResponseError as e:
            self.log.exception(e)
            resp = {"error_code": 1, "error": e.description}
            if hasattr(e, "retriable") and e.retriable:
                resp["error_code"] = 2
            return resp

    def list_topics(self, content_type: str):
        metadata = self.admin_client.cluster_metadata()
        # hacky way to get some info for more detailed 404's ... do not use otherwise
        topics = list(metadata["topics"].keys())
        self.r(topics, content_type)

    def topic_details(self, content_type: str, *, topic: str):
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
        p = self.get_partition_data_or_fail(topic, partition_id, content_type)
        self.r(p, content_type)

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
            # Do a topics request on failure, figure out faster ways once we get correctness down
            if topic not in self.admin_client.cluster_metadata():
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
