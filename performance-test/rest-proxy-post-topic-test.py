#!/bin/env python3

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from locust import HttpUser, task

import json
import os

BOOTSTRAP_SERVER = os.environ.get("BOOTSTRAP_SERVER", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "test-topic")
POST_PATH = "/topics/" + TOPIC

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


class RESTProxyPublish(HttpUser):
    def on_test_start(self):  # pylint: disable=no-self-use
        admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER, client_id="locust-performance-test")

        topic_list = [NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)]
        try:
            admin_client.create_topics(topic_list)
        except TopicAlreadyExistsError:
            pass
        admin_client.close()

    @task
    def post_rest_proxy(self) -> None:
        with self.client.post(POST_PATH, json=BODY, catch_response=True) as response:
            error_count = sum(record_response.get("error") is not None for record_response in response.json()["offsets"])
            if error_count > 0:
                response.failure(f"Response contains {error_count} errors for {INPUT_RECORDS_COUNT} input records.")
