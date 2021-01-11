from dataclasses import dataclass
from kafka.errors import TopicAlreadyExistsError
from karapace.utils import Client
from typing import Callable
from unittest.mock import MagicMock
from urllib.parse import urlparse

import copy
import json
import os
import random

TempDirCreator = Callable[[], os.PathLike]

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
REST_URI = "REST_URI"
REGISTRY_URI = "REGISTRY_URI"


@dataclass
class KafkaConfig:
    datadir: str
    kafka_keystore_password: str
    kafka_port: int
    zookeeper_port: int


def get_broker_ip():
    if REST_URI in os.environ and REGISTRY_URI in os.environ:
        return urlparse(os.environ[REGISTRY_URI]).hostname
    return "127.0.0.1"


async def new_consumer(c, group, fmt="avro", trail=""):
    payload = copy.copy(consumer_valid_payload)
    payload["format"] = fmt
    resp = await c.post(f"/consumers/{group}{trail}", json=payload, headers=REST_HEADERS[fmt])
    assert resp.ok
    return resp.json()["instance_id"]


async def client_for(app, client_factory):
    if REST_URI in os.environ and REGISTRY_URI in os.environ:
        # least intrusive way of figuring out which client is which
        if app.type == "rest":
            return Client(server_uri=os.environ[REST_URI])
        return Client(server_uri=os.environ[REGISTRY_URI])

    client_factory = await client_factory(app.app)
    c = Client(client=client_factory)
    return c


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


def mock_factory(app_name):
    def inner():
        app = MagicMock()
        app.type = app_name
        app.serializer = MagicMock()
        app.consumer_manager = MagicMock()
        app.serializer.registry_client = MagicMock()
        app.consumer_manager.deserializer = MagicMock()
        app.consumer_manager.hostname = "http://localhost:8082"
        app.consumer_manager.deserializer.registry_client = MagicMock()
        return app, None

    return inner
