from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from dataclasses import dataclass
from kafka.errors import TopicAlreadyExistsError
from karapace.utils import Client
from typing import Callable, List
from urllib.parse import quote

import asyncio
import copy
import json
import random
import time
import uuid

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


class Timeout(Exception):
    pass


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


@dataclass(frozen=True)
class Expiration:
    deadline: float

    @classmethod
    def from_timeout(cls, timeout: float) -> "Expiration":
        return cls(time.monotonic() + timeout)

    def raise_if_expired(self, msg: str) -> None:
        if time.monotonic() > self.deadline:
            raise Timeout(msg)


@dataclass(frozen=True)
class PortRangeInclusive:
    start: int
    end: int

    PRIVILEGE_END = 2 ** 10
    MAX_PORTS = 2 ** 16 - 1

    def __post_init__(self):
        # Make sure the range is valid and that we don't need to be root
        assert self.end > self.start, "there must be at least one port available"
        assert self.end <= self.MAX_PORTS, f"end must be lower than {self.MAX_PORTS}"
        assert self.start > self.PRIVILEGE_END, "start must not be a privileged port"

    def next_range(self, number_of_ports: int) -> "PortRangeInclusive":
        next_start = self.end + 1
        next_end = next_start + number_of_ports - 1  # -1 because the range is inclusive

        return PortRangeInclusive(next_start, next_end)


# To find a good port range use the following:
#
#   curl --silent 'https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.txt' | \
#       egrep -i -e '^\s*[0-9]+-[0-9]+\s*unassigned' | \
#       awk '{print $1}'
#
KAFKA_PORT_RANGE = PortRangeInclusive(48700, 48800)
ZK_PORT_RANGE = KAFKA_PORT_RANGE.next_range(100)
REGISTRY_PORT_RANGE = ZK_PORT_RANGE.next_range(100)
TESTS_PORT_RANGE = REGISTRY_PORT_RANGE.next_range(100)


def get_random_port(*, port_range: PortRangeInclusive, blacklist: List[int]) -> int:
    """ Find a random port in the range `PortRangeInclusive`.

    Note:
        This function is *not* aware of the ports currently open in the system,
        the blacklist only prevents two services of the same type to randomly
        get the same ports for *a single test run*.

        Because of that, the port range should be chosen such that there is no
        system service in the range. Also note that running two sessions of the
        tests with the same range is not supported and will lead to flakiness.
    """
    value = random.randint(port_range.start, port_range.end)
    while value in blacklist:
        value = random.randint(port_range.start, port_range.end)
    return value


async def new_consumer(c, group, fmt="avro", trail=""):
    payload = copy.copy(consumer_valid_payload)
    payload["format"] = fmt
    resp = await c.post(f"/consumers/{group}{trail}", json=payload, headers=REST_HEADERS[fmt])
    assert resp.ok
    return resp.json()["instance_id"]


def new_random_name(prefix: str) -> str:
    suffix = str(uuid.uuid4())[:8]
    return f"{prefix}{suffix}"


def create_subject_name_factory(prefix: str) -> Callable[[], str]:
    return create_id_factory(f"subject-{prefix}")


def create_field_name_factory(prefix: str) -> Callable[[], str]:
    return create_id_factory(f"field_{prefix}")


def create_id_factory(prefix: str) -> Callable[[], str]:
    """
    Creates unique ids prefixed with prefix..
    The resulting ids are safe to embed in URLs.
    """
    index = 1

    def create_name() -> str:
        nonlocal index
        random_name = str(uuid.uuid4())[:8]
        name = f"{quote(prefix).replace('/', '_')}_{index}_{random_name}"
        return name

    return create_name


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


async def repeat_until_successful_request(
    callback, path: str, json_data, headers, error_msg: str, timeout: float, sleep: float
):
    expiration = Expiration.from_timeout(timeout=timeout)
    ok = False
    res = None

    try:
        res = await callback(path, json=json_data, headers=headers)
    # ClientOSError: Raised when the listening socket is not yet open in the server
    # ServerDisconnectedError: Wrong url
    except (ClientOSError, ServerDisconnectedError):
        pass
    else:
        ok = res.ok

    while not ok:
        await asyncio.sleep(sleep)
        expiration.raise_if_expired(msg=f"{error_msg} {res} after {timeout} secs")

        try:
            res = await callback(path, json=json_data, headers=headers)
        except (ClientOSError, ServerDisconnectedError):
            pass
        else:
            ok = res.ok

    return res
