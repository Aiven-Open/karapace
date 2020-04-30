from kafka.errors import TopicAlreadyExistsError
from karapace.utils import Client
from unittest.mock import MagicMock
from urllib.parse import urlparse

import json
import os
import random

consumer_valid_payload = {
    "format": "avro",
    "auto.offset.reset": "earliest",
    "consumer.request.timeout.ms": 11000,
    "fetch.min.bytes": 100000,
    "auto.commit.enable": "true"
}

schema_json = json.dumps({
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

test_objects = [
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


def get_broker_ip():
    if REST_URI in os.environ and REGISTRY_URI in os.environ:
        return urlparse(os.environ[REGISTRY_URI]).hostname
    return "127.0.0.1"


async def client_for(app, client_factory):
    if REST_URI in os.environ and REGISTRY_URI in os.environ:
        # least intrusive way of figuring out which client is which
        if app.type == "rest":
            return Client(server_uri=os.environ[REST_URI])
        return Client(server_uri=os.environ[REGISTRY_URI])

    client_factory = await client_factory(app.app)
    c = Client(client=client_factory)
    return c


def new_topic(admin_client, prefix="topic"):
    tn = f"{prefix}{hash(random.random())}"
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
