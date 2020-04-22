from kafka.errors import TopicAlreadyExistsError
from karapace.utils import Client

import json
import random

schema_json = json.dumps({
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [{
        "name": "name",
        "type": "string"
    }, {
        "name": "favorite_number",
        "type": ["int", "null"]
    }, {
        "name": "favorite_color",
        "type": ["string", "null"]
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
        "Content-Type": "application/vnd.kafka.json.v2+json"
    },
    "binary": {
        "Content-Type": "application/vnd.kafka.binary.v2+json"
    },
    "avro": {
        "Content-Type": "application/vnd.kafka.avro.v2+json"
    },
}


async def client_for(app, client_factory):
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
