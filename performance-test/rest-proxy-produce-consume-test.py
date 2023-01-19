"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from locust import FastHttpUser, task
from locust.contrib.fasthttp import ResponseContextManager
from locust.env import Environment

import json
import os
import uuid

BOOTSTRAP_SERVER = os.environ.get("BOOTSTRAP_SERVER", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "test-topic")
CONSUME_TIMEOUT = os.environ.get("CONSUME_TIMEOUT", 1000)
CONSUME_MAX_BYTES = os.environ.get("CONSUME_MAX_BYTES", 128)

SCHEMA_STR = json.dumps(
    {
        "type": "record",
        "name": "CpuUsage",
        "fields": [
            {"name": "pct", "type": "int"},
        ],
    },
)
INPUT_RECORDS_COUNT = 50
RECORDS = [{"key": "MQ==", "value": "MQ=="}] * INPUT_RECORDS_COUNT

BODY = {
    "value_schema": SCHEMA_STR,
    "value_schema_id": 1,
    "key_schema": SCHEMA_STR,
    "key_schema_id": 1,
    "records": RECORDS,
}

HEADER_CONTENT_TYPE = {"Content-Type": "application/vnd.kafka.json.v2+json"}
HEADER_ACCEPT = {"Accept": "application/vnd.kafka.json.v2+json"}


class RESTProxyPublishAndConsume(FastHttpUser):
    def __init__(self, environment: Environment):
        super().__init__(environment)
        self.consumer_instance_id = f"consumer_instance_{uuid.uuid4()}"

    def on_start(self):
        admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER, client_id="locust-performance-test")

        topic_list = [NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)]
        try:
            admin_client.create_topics(topic_list)
        except TopicAlreadyExistsError:
            pass
        admin_client.close()

        # Register consumer instance and subscribe
        self.client.post(
            "/consumers/consumer-group",
            headers=HEADER_CONTENT_TYPE,
            json={"name": self.consumer_instance_id, "format": "json"},
            name="/consumers/consumer-group",
        )
        self.client.post(
            f"/consumers/consumer-group/instances/{self.consumer_instance_id}/subscription",
            headers=HEADER_CONTENT_TYPE,
            json={"topics": [TOPIC]},
            name="/consumers/consumer-group/instances/[INSTANCE]/subscription",
        )

    @task(20)
    def post_rest_proxy(self) -> None:
        with self.client.post(f"/topics/{TOPIC}", json=BODY, catch_response=True) as response:
            response: ResponseContextManager
            error_count = sum(record_response.get("error") is not None for record_response in response.json()["offsets"])
            if error_count > 0:
                response.failure(f"Response contains {error_count} errors for {INPUT_RECORDS_COUNT} input records.")

    @task(1)
    def get_consume(self):
        self.client.get(
            f"/consumers/consumer-group/instances/{self.consumer_instance_id}/records",
            headers=HEADER_ACCEPT,
            params={
                "timeout": CONSUME_TIMEOUT,
                "max_bytes": CONSUME_MAX_BYTES,
            },  # Will poll for consumer max bytes, in this case for three seconds.
            name="/consumers/consumer-group/instances/[INSTANCE]/records",
        )
