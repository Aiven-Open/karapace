"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from aiokafka.errors import TopicAlreadyExistsError
from karapace.client import Client
from karapace.kafka.admin import KafkaAdminClient
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.utils import Expiration
from pathlib import Path
from subprocess import Popen
from typing import Any, Callable, IO, Union
from urllib.parse import quote

import asyncio
import copy
import json
import os
import ssl
import sys
import time
import uuid

consumer_valid_payload = {
    "format": "avro",
    "auto.offset.reset": "earliest",
    "consumer.request.timeout.ms": 11000,
    "fetch.min.bytes": 100000,
    "auto.commit.enable": "true",
    "topic.metadata.refresh.interval.ms": 100,
}
schema_jsonschema_json = json.dumps(
    {
        "type": "object",
        "properties": {
            "foo": {"type": "integer"},
        },
    }
)

schema_avro_json = json.dumps(
    {
        "namespace": "example.avro",
        "type": "record",
        "name": "example.avro.User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": "int"},
            {"name": "favorite_color", "type": "string"},
        ],
    }
)

schema_avro_json_evolution = json.dumps(
    {
        "namespace": "example.avro",
        "type": "record",
        "name": "example.avro.User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": "int"},
            {"name": "favorite_color", "type": "string"},
            {"name": "favorite_quark", "type": "string", "default": "def"},
        ],
    }
)


test_objects_jsonschema = [{"foo": 100}, {"foo": 200}]

test_objects_avro = [
    {"name": "First Foo", "favorite_number": 2, "favorite_color": "bar"},
    {"name": "Second Foo", "favorite_number": 3, "favorite_color": "baz"},
    {"name": "Third Foo", "favorite_number": 5, "favorite_color": "quux"},
]

test_objects_avro_evolution = [
    {"name": "First Foo", "favorite_number": 2, "favorite_color": "bar", "favorite_quark": "up"},
    {"name": "Second Foo", "favorite_number": 3, "favorite_color": "baz", "favorite_quark": "down"},
    {"name": "Third Foo", "favorite_number": 5, "favorite_color": "quux", "favorite_quark": "charm"},
]


# protobuf schemas in tests must be filtered by  trim_margin() from kotlin_wrapper module

schema_protobuf = """
|syntax = "proto3";
|
|option java_package = "com.codingharbour.protobuf";
|option java_outer_classname = "TestEnumOrder";
|
|message Message {
|  int32 query = 1;
|  Enum speed = 2;
|}
|enum Enum {
|  HIGH = 0;
|  MIDDLE = 1;
|  LOW = 2;
|}
|
"""
schema_protobuf = trim_margin(schema_protobuf)

test_objects_protobuf = [
    {"query": 5, "speed": "HIGH"},
    {"query": 10, "speed": "MIDDLE"},
]

test_fail_objects_protobuf = [
    {"query": "STR", "speed": 99},
    {"xx": 10, "bb": "MIDDLE"},
]

schema_data = {
    "avro": (schema_avro_json, test_objects_avro),
    "jsonschema": (schema_jsonschema_json, test_objects_jsonschema),
    "protobuf": (schema_protobuf, test_objects_protobuf),
}

schema_protobuf_second = """
|syntax = "proto3";
|
|option java_package = "com.codingharbour.protobuf";
|option java_outer_classname = "TestEnumOrder";
|
|message SensorInfo {
|  int32 q = 1;
|  Enu sensor_type = 2;
|  repeated int32 nums = 3;
|  Order order = 4;
|     message Order {
|        string item = 1;
|     }
|}
|enum Enu {
|  H1 = 0;
|  M1 = 1;
|  L1 = 2;
|}
|
"""
schema_protobuf_second = trim_margin(schema_protobuf_second)

test_objects_protobuf_second = [
    {"q": 1, "sensor_type": "H1", "nums": [3, 4], "order": {"item": "ABC01223"}},
    {"q": 2, "sensor_type": "M1", "nums": [2], "order": {"item": "ABC01233"}},
    {"q": 3, "sensor_type": "L1", "nums": [3, 4], "order": {"item": "ABC01223"}},
]

schema_protobuf_invalid_because_corrupted = """
|o3"
|
|opti  --  om.codingharbour.protobuf";
|option java_outer_classname = "TestEnumOrder";
|
|message Message {
|  int32
|  speed =;
|}
|Enum
|  HIGH = 0
|  MIDDLE = ;
"""
schema_protobuf_invalid_because_corrupted = trim_margin(schema_protobuf_invalid_because_corrupted)

schema_protobuf_with_invalid_ref = """
 |syntax = "proto3";
 |
 |import "Message.proto";
 |
 |message MessageWithInvalidRef {
 |    string name = 1;
 |    Message ref = 2;
 |}
 |"""
schema_protobuf_with_invalid_ref = trim_margin(schema_protobuf_with_invalid_ref)

schema_data_second = {"protobuf": (schema_protobuf_second, test_objects_protobuf_second)}

second_schema_json = json.dumps(
    {"namespace": "example.avro.other", "type": "record", "name": "Dude", "fields": [{"name": "name", "type": "string"}]}
)

second_obj = [{"name": "doe"}, {"name": "john"}]

REST_HEADERS = {
    "json": {
        "Content-Type": "application/vnd.kafka.json.v2+json",
        "Accept": "application/vnd.kafka.json.v2+json, application/vnd.kafka.v2+json, application/json, */*",
    },
    "jsonschema": {
        "Content-Type": "application/vnd.kafka.jsonschema.v2+json",
        "Accept": "application/vnd.kafka.jsonschema.v2+json, application/vnd.kafka.v2+json, application/json, */*",
    },
    "binary": {
        "Content-Type": "application/vnd.kafka.binary.v2+json",
        "Accept": "application/vnd.kafka.binary.v2+json, application/vnd.kafka.v2+json, application/json, */*",
    },
    "avro": {
        "Content-Type": "application/vnd.kafka.avro.v2+json",
        "Accept": "application/vnd.kafka.avro.v2+json, application/vnd.kafka.v2+json, application/json, */*",
    },
    "protobuf": {
        "Content-Type": "application/vnd.kafka.protobuf.v2+json",
        "Accept": "application/vnd.kafka.protobuf.v2+json, application/vnd.kafka.v2+json, application/json, */*",
    },
}


async def new_consumer(c, group, fmt="avro", trail="", payload_override=None):
    payload = copy.copy(consumer_valid_payload)
    payload["format"] = fmt
    if payload_override:
        payload.update(payload_override)
    resp = await c.post(f"/consumers/{group}{trail}", json=payload, headers=REST_HEADERS[fmt])
    assert resp.ok
    return resp.json()["instance_id"]


def new_random_name(prefix: str) -> str:
    # A hyphen is not a valid character for Avro identifiers. Use only the
    # first 8 characters of the UUID.
    suffix = str(uuid.uuid4())[:8]
    return f"{prefix}{suffix}"


def create_subject_name_factory(prefix: str) -> Callable[[], str]:
    return create_id_factory(f"subject_{prefix}")


def create_field_name_factory(prefix: str) -> Callable[[], str]:
    return create_id_factory(f"field_{prefix}")


def create_schema_name_factory(prefix: str) -> Callable[[], str]:
    return create_id_factory(f"schema_{prefix}")


def create_group_name_factory(prefix: str) -> Callable[[], str]:
    return create_id_factory(f"group_{prefix}")


def create_id_factory(prefix: str) -> Callable[[], str]:
    """
    Creates unique ids prefixed with prefix..
    The resulting ids are safe to embed in URLs.
    """
    index = 1

    def create_name() -> str:
        nonlocal index
        return new_random_name(f"{quote(prefix).replace('/', '_')}_{index}_")

    return create_name


def new_topic(admin_client: KafkaAdminClient, prefix: str = "topic", *, num_partitions: int = 1) -> str:
    topic_name = f"{new_random_name(prefix)}"
    try:
        admin_client.new_topic(topic_name, num_partitions=num_partitions)
    except TopicAlreadyExistsError:
        pass
    return topic_name


async def wait_for_topics(rest_async_client: Client, topic_names: list[str], timeout: float, sleep: float) -> None:
    for topic in topic_names:
        expiration = Expiration.from_timeout(timeout=timeout)
        topic_found = False
        current_topics = None

        while not topic_found:
            await asyncio.sleep(sleep)
            expiration.raise_timeout_if_expired(
                msg_format="New topic {topic} must be in the result of /topics. Result={current_topics}",
                topic=topic,
                current_topics=current_topics,
            )
            res = await rest_async_client.get("/topics")
            assert res.ok, f"Status code is not 200: {res.status_code}"
            current_topics = res.json()
            topic_found = topic in current_topics


async def repeat_until_successful_request(
    callback,
    path: str,
    json_data,
    headers,
    error_msg: str,
    timeout: float,
    sleep: float,
):
    expiration = Expiration.from_timeout(timeout=timeout)
    ok = False
    res = None

    while not ok:
        expiration.raise_timeout_if_expired(
            msg_format=f"{error_msg} {res} after {timeout} secs",
            error_msg=error_msg,
            res=res,
            timeout=timeout,
        )

        try:
            res = await callback(path, json=json_data, headers=headers)
        # SSLCertVerificationError: likely a configuration error, nothing to do
        except ssl.SSLCertVerificationError:
            raise
        # ClientOSError: Raised when the listening socket is not yet open in the server
        # ServerDisconnectedError: Wrong url
        except (ClientOSError, ServerDisconnectedError):
            await asyncio.sleep(sleep)
        else:
            ok = res.ok

    return res


async def repeat_until_master_is_available(client: Client) -> None:
    while True:
        res = await client.get("/master_available", json={})
        reply = res.json()
        if reply is not None and "master_available" in reply and reply["master_available"] is True:
            break
        time.sleep(1)


def write_ini(file_path: Path, ini_data: dict) -> None:
    ini_contents = (f"{key}={value}" for key, value in ini_data.items())
    file_contents = "\n".join(ini_contents)

    with file_path.open("w") as fp:
        fp.write(file_contents)


def python_exe() -> str:
    python = sys.executable
    if python is None:
        python = os.environ.get("PYTHON", "python3")
    return python


def popen_karapace_all(config_path: Union[Path, str], stdout: IO, stderr: IO, **kwargs) -> Popen:
    kwargs["stdout"] = stdout
    kwargs["stderr"] = stderr
    return Popen([python_exe(), "-m", "karapace.karapace_all", str(config_path)], **kwargs)


class StubMessage:
    """A stub to stand-in for `confluent_kafka.Message` in unittests.

    Since that class cannot be instantiated, thus this is a liberal simulation
    of its behaviour ie. its attributes are accessible via getter functions:
    `message.offset()`."""

    def __init__(self, **attrs: Any) -> None:
        self._attrs = attrs

    def __getattr__(self, key: str) -> None:
        try:
            return lambda: self._attrs[key]
        except KeyError as exc:
            raise AttributeError from exc
