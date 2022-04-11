from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from kafka.errors import TopicAlreadyExistsError
from karapace.client import Client
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.utils import Expiration
from pathlib import Path
from typing import Callable, List
from urllib.parse import quote

import asyncio
import copy
import ssl
import ujson
import uuid

consumer_valid_payload = {
    "format": "avro",
    "auto.offset.reset": "earliest",
    "consumer.request.timeout.ms": 11000,
    "fetch.min.bytes": 100000,
    "auto.commit.enable": "true",
}
schema_jsonschema_json = ujson.dumps(
    {
        "type": "object",
        "properties": {
            "foo": {"type": "integer"},
        },
    }
)

schema_avro_json = ujson.dumps(
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

test_objects_jsonschema = [{"foo": 100}, {"foo": 200}]

test_objects_avro = [
    {"name": "First Foo", "favorite_number": 2, "favorite_color": "bar"},
    {"name": "Second Foo", "favorite_number": 3, "favorite_color": "baz"},
    {"name": "Third Foo", "favorite_number": 5, "favorite_color": "quux"},
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

schema_protobuf2 = """
|syntax = "proto3";
|
|option java_package = "com.codingharbour.protobuf";
|option java_outer_classname = "TestEnumOrder";
|
|message Message {
|  int32 query = 1;
|}
|enum Enum {
|  HIGH = 0;
|  MIDDLE = 1;
|  LOW = 2;
|}
|
"""
schema_protobuf2 = trim_margin(schema_protobuf2)

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

schema_data_second = {"protobuf": (schema_protobuf_second, test_objects_protobuf_second)}

second_schema_json = ujson.dumps(
    {"namespace": "example.avro.other", "type": "record", "name": "Dude", "fields": [{"name": "name", "type": "string"}]}
)

second_obj = [{"name": "doe"}, {"name": "john"}]

REST_HEADERS = {
    "json": {
        "Content-Type": "application/vnd.kafka.json.v2+json",
        "Accept": "*/*",
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


def write_ini(file_path: Path, ini_data: dict) -> None:
    ini_contents = (f"{key}={value}" for key, value in ini_data.items())
    file_contents = "\n".join(ini_contents)

    with file_path.open("w") as fp:
        fp.write(file_contents)
