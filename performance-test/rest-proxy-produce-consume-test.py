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
import logging
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

LOG = logging.getLogger(__name__)


class RESTProxyPublishAndConsume(FastHttpUser):
    def __init__(self, environment: Environment):
        super().__init__(environment)
        self.consumer_group = "consumer-group"
        self.consumer_instance_id = f"consumer_instance_{uuid.uuid4()}"

    def on_start(self):
        # Create topic if needed
        admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER, client_id="locust-test")
        try:
            admin_client.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
        admin_client.close()

        # Register consumer instance
        self.client.post(
            f"/consumers/{self.consumer_group}",
            headers=HEADER_CONTENT_TYPE,
            json={"name": self.consumer_instance_id, "format": "json"},
            name="/consumers/[GROUP]",
        )

        # Subscribe consumer
        self.client.post(
            f"/consumers/{self.consumer_group}/instances/{self.consumer_instance_id}/subscription",
            headers=HEADER_CONTENT_TYPE,
            json={"topics": [TOPIC]},
            name="/consumers/[GROUP]/instances/[ID]/subscription",
        )

    #
    # CLEANUP LOGIC
    #
    def on_stop(self):
        # 1. DELETE subscription
        try:
            with self.client.delete(
                f"/consumers/{self.consumer_group}/instances/{self.consumer_instance_id}/subscription",
                headers=HEADER_ACCEPT,
                name="/consumers/[GROUP]/instances/[ID]/subscription DELETE",
                catch_response=True,
            ) as resp:
                if resp.status_code >= 400:
                    msg = (
                        f"Failed to delete subscription for {self.consumer_group}/{self.consumer_instance_id}: "
                        f"{resp.status_code} {resp.text}"
                    )
                    LOG.warning(msg)
                    resp.failure(msg)
        except Exception:
            LOG.exception(
                "Exception while deleting subscription for %s/%s",
                self.consumer_group,
                self.consumer_instance_id,
            )

        # 2. DELETE consumer instance
        try:
            with self.client.delete(
                f"/consumers/{self.consumer_group}/instances/{self.consumer_instance_id}",
                headers=HEADER_ACCEPT,
                name="/consumers/[GROUP]/instances/[ID] DELETE",
                catch_response=True,
            ) as resp:
                if resp.status_code >= 400:
                    msg = (
                        f"Failed to delete consumer {self.consumer_group}/{self.consumer_instance_id}: "
                        f"{resp.status_code} {resp.text}"
                    )
                    LOG.warning(msg)
                    resp.failure(msg)
        except Exception:
            LOG.exception("Exception while deleting consumer %s/%s", self.consumer_group, self.consumer_instance_id)

        # No explicit consumer-group delete endpoint exists in your routes.
        # Karapace removes empty groups automatically.

    @task(20)
    def post_rest_proxy(self) -> None:
        with self.client.post(f"/topics/{TOPIC}", json=BODY, catch_response=True) as response:
            response: ResponseContextManager
            error_count = sum(record.get("error") is not None for record in response.json()["offsets"])
            if error_count > 0:
                response.failure(f"Response contains {error_count} errors for {INPUT_RECORDS_COUNT} input records.")

    @task(1)
    def get_consume(self):
        self.client.get(
            f"/consumers/{self.consumer_group}/instances/{self.consumer_instance_id}/records",
            headers=HEADER_ACCEPT,
            params={
                "timeout": CONSUME_TIMEOUT,
                "max_bytes": CONSUME_MAX_BYTES,
            },
            name="/consumers/[GROUP]/instances/[ID]/records",
        )
