from dataclasses import dataclass
from kafka.errors import TopicAlreadyExistsError
from karapace.utils import Client
from typing import List

import asyncio
import copy
import json
import random

consumer_valid_payload = {
    "format": "avro",
    "auto.offset.reset": "earliest",
    "consumer.request.timeout.ms": 11000,
    "fetch.min.bytes": 100000,
    "auto.commit.enable": "true"
}
schema_jsonschema_json = json.dumps({
    "type": "object",
    "properties": {
        "foo": {
            "type": "integer"
        },
    },
})

schema_avro_json = json.dumps({
    "namespace": "example.avro",
    "type": "record",
    "name": "example.avro.User",
    "fields": [{
        "name": "name",
        "type": "string"
    }, {
        "name": "favorite_number",
        "type": "int"
    }, {
        "name": "favorite_color",
        "type": "string"
    }]
})

test_objects_jsonschema = [{"foo": 100}, {"foo": 200}]

test_objects_avro = [
    {
        "name": "First Foo",
        "favorite_number": 2,
        "favorite_color": "bar"
    },
    {
        "name": "Second Foo",
        "favorite_number": 3,
        "favorite_color": "baz"
    },
    {
        "name": "Third Foo",
        "favorite_number": 5,
        "favorite_color": "quux"
    },
]

schema_data = {
    "avro": (schema_avro_json, test_objects_avro),
    "jsonschema": (schema_jsonschema_json, test_objects_jsonschema)
}

second_schema_json = json.dumps({
    "namespace": "example.avro.other",
    "type": "record",
    "name": "Dude",
    "fields": [{
        "name": "name",
        "type": "string"
    }]
})

second_obj = [{"name": "doe"}, {"name": "john"}]

REST_HEADERS = {
    "json": {
        "Content-Type": "application/vnd.kafka.json.v2+json",
        "Accept": "*/*",
    },
    "jsonschema": {
        "Content-Type": "application/vnd.kafka.jsonschema.v2+json",
        "Accept": "application/vnd.kafka.jsonschema.v2+json, application/vnd.kafka.v2+json, application/json, */*"
    },
    "binary": {
        "Content-Type": "application/vnd.kafka.binary.v2+json",
        "Accept": "application/vnd.kafka.binary.v2+json, application/vnd.kafka.v2+json, application/json, */*"
    },
    "avro": {
        "Content-Type": "application/vnd.kafka.avro.v2+json",
        "Accept": "application/vnd.kafka.avro.v2+json, application/vnd.kafka.v2+json, application/json, */*"
    },
}


@dataclass
class KafkaConfig:
    datadir: str
    kafka_keystore_password: str
    kafka_port: int
    zookeeper_port: int

    @staticmethod
    def from_dict(data: dict) -> "KafkaConfig":
        return KafkaConfig(
            data["datadir"],
            data["kafka_keystore_password"],
            data["kafka_port"],
            data["zookeeper_port"],
        )


@dataclass
class KafkaServers:
    bootstrap_servers: List[str]

    def __post_init__(self):
        is_bootstrap_uris_valid = (
            isinstance(self.bootstrap_servers, list) and len(self.bootstrap_servers) > 0
            and all(isinstance(url, str) for url in self.bootstrap_servers)
        )
        if not is_bootstrap_uris_valid:
            raise ValueError("bootstrap_servers must be a non-empty list of urls")


async def new_consumer(c, group, fmt="avro", trail=""):
    payload = copy.copy(consumer_valid_payload)
    payload["format"] = fmt
    resp = await c.post(f"/consumers/{group}{trail}", json=payload, headers=REST_HEADERS[fmt])
    assert resp.ok
    return resp.json()["instance_id"]


def new_random_name(prefix="topic"):
    suffix = hash(random.random())
    return f"{prefix}{suffix}"


def new_topic(admin_client, prefix="topic"):
    tn = f"{new_random_name(prefix)}"
    try:
        admin_client.new_topic(tn)
    except TopicAlreadyExistsError:
        pass
    return tn


async def wait_for_topics(rest_async_client: Client, topic_names: List[str], timeout: float, sleep: float) -> None:
    for topic in topic_names:
        expiration = Expiration.from_timeout(timeout=timeout)
        topic_found = False
        current_topics = None

        while not topic_found:
            await asyncio.sleep(sleep)
            expiration.raise_if_expired(msg=f"New topic {topic} must be in the result of /topics. Result={current_topics}")
            res = await rest_async_client.get("/topics")
            assert res.ok, f"Status code is not 200: {res.status_code}"
            current_topics = res.json()
            topic_found = topic in current_topics
