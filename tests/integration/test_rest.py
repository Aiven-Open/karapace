"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from kafka import KafkaProducer
from kafka.errors import UnknownTopicOrPartitionError
from karapace.client import Client
from karapace.kafka_rest_apis import KafkaRest, KafkaRestAdminClient, SUBJECT_VALID_POSTFIX
from karapace.version import __version__
from pytest import raises
from tests.integration.conftest import REST_PRODUCER_MAX_REQUEST_BYTES
from tests.utils import (
    new_random_name,
    new_topic,
    REST_HEADERS,
    schema_avro_json,
    schema_avro_json_evolution,
    second_obj,
    test_objects_avro,
    test_objects_avro_evolution,
    wait_for_topics,
)

import asyncio
import base64
import json
import time

NEW_TOPIC_TIMEOUT = 10


def check_successful_publish_response(success_response, objects, partition_id=None) -> None:
    assert success_response.ok
    success_response = success_response.json()
    for k in ["value_schema_id", "offsets"]:
        assert k in success_response
    assert len(success_response["offsets"]) == len(objects)
    for o in success_response["offsets"]:
        for k in ["offset", "partition"]:
            assert k in o and isinstance(o[k], int)
            if partition_id is not None:
                assert partition_id == o["partition"]


async def test_health_endpoint(rest_async_client: Client) -> None:
    res = await rest_async_client.get("/_health")
    assert res.status_code == 200
    response = res.json()
    assert "process_uptime_sec" in response
    assert "karapace_version" in response
    assert response["process_uptime_sec"] >= 0
    assert response["karapace_version"] == __version__


async def test_request_body_too_large(rest_async_client: KafkaRestAdminClient, admin_client: Client) -> None:
    tn = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[tn], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    pl = {"records": [{"value": 1_048_576 * "a"}]}
    res = await rest_async_client.post(f"/topics/{tn}", pl, headers={"Content-Type": "application/json"})
    assert res.status_code == 413


async def test_content_types(rest_async_client: KafkaRestAdminClient, admin_client: Client) -> None:
    tn = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[tn], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    valid_headers = [
        "application/vnd.kafka.v1+json",
        "application/vnd.kafka.binary.v1+json",
        "application/vnd.kafka.avro.v1+json",
        "application/vnd.kafka.json+json",
        "application/vnd.kafka.v1+json",
        "application/vnd.kafka+json",
        "application/json",
        "application/octet-stream",
    ]
    invalid_headers = [
        "application/vnd.kafka.v3+json",
        "application/vnd.kafka.binary.v1+foo",
        "application/vnd.kafka.avro.v0+json",
        "application/vnd.kafka.json+avro",
        "application/vnd.kafka",
        "application/vnd.kafka+binary",
        "application/text",
        "bar/baz",
    ]

    invalid_accept_headers = [
        "application/octet-stream",
        "application/vnd.kafka+foo",
        "application/text",
        "foo/*",
        "bar/json",
        "*/baz",
    ]

    valid_accept_headers = [
        "application/vnd.kafka.v1+json",
        "application/vnd.kafka+json",
        "application/json",
        "application/*",
        "*/json",
        "*/*",
    ]

    avro_payload = {"value_schema": schema_avro_json, "records": [{"value": o} for o in test_objects_avro]}
    json_payload = {"records": [{"value": {"foo": "bar"}}]}
    binary_payload = {"records": [{"value": "Zm9v"}]}
    valid_payloads = [
        binary_payload,
        binary_payload,
        avro_payload,
        json_payload,
        binary_payload,
        binary_payload,
        binary_payload,
        binary_payload,
    ]
    # post / put requests should get validated
    for hv, pl in zip(valid_headers, valid_payloads):
        res = await rest_async_client.post(f"topics/{tn}", pl, headers={"Content-Type": hv})
        assert res.ok
    for hv, pl in zip(invalid_headers, valid_payloads):
        res = await rest_async_client.post(f"topics/{tn}", pl, headers={"Content-Type": hv})
        assert not res.ok

    for ah in valid_accept_headers:
        res = await rest_async_client.get("/brokers", headers={"Accept": ah})
        assert res.ok

    for ah in invalid_accept_headers:
        res = await rest_async_client.get("/brokers", headers={"Accept": ah})
        assert not res.ok


async def test_avro_publish_primitive_schema(rest_async_client: KafkaRestAdminClient, admin_client: Client) -> None:
    topic_str = new_topic(admin_client)
    topic_int = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic_str, topic_int], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    for topic, value_schema, records in [
        (topic_str, '"string"', [{"value": "foobar"}]),
        (topic_str, '{"type":"string"}', [{"value": "foobar2"}]),
        (topic_int, '"int"', [{"value": 1}]),
        (topic_int, '{"type":"int"}', [{"value": 2}]),
    ]:
        url = f"/topics/{topic}"
        res = await rest_async_client.post(
            url, json={"value_schema": value_schema, "records": records}, headers=REST_HEADERS["avro"]
        )
        res_json = res.json()
        assert res.ok
        assert "offsets" in res_json
        if "partition" in url:
            for o in res_json["offsets"]:
                assert "partition" in o


async def test_avro_publish(
    rest_async_client: Client,
    registry_async_client: Client,
    admin_client: KafkaRestAdminClient,
) -> None:
    tn = new_topic(admin_client)
    other_tn = new_topic(admin_client)

    await wait_for_topics(rest_async_client, topic_names=[tn, other_tn], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    header = REST_HEADERS["avro"]
    # check succeeds with 1 record and brand new schema
    res = await registry_async_client.post(f"subjects/{other_tn}/versions", json={"schema": schema_avro_json_evolution})
    assert res.ok
    new_schema_id = res.json()["id"]

    # test checks schema id use for key and value, register schema for both with topic naming strategy
    for pl_type in SUBJECT_VALID_POSTFIX:
        res = await registry_async_client.post(
            f"subjects/{tn}-{pl_type.value}/versions", json={"schema": schema_avro_json_evolution}
        )
        assert res.ok
        assert res.json()["id"] == new_schema_id

    urls = [f"/topics/{tn}", f"/topics/{tn}/partitions/0"]
    for url in urls:
        partition_id = 0 if "partition" in url else None
        for pl_type in ["key", "value"]:
            correct_payload = {f"{pl_type}_schema": schema_avro_json, "records": [{pl_type: o} for o in test_objects_avro]}
            res = await rest_async_client.post(url, correct_payload, headers=header)
            check_successful_publish_response(res, test_objects_avro, partition_id)
            # check succeeds with prepublished schema
            pre_publish_payload = {
                f"{pl_type}_schema_id": new_schema_id,
                "records": [{pl_type: o} for o in test_objects_avro_evolution],
            }
            res = await rest_async_client.post(f"/topics/{tn}", json=pre_publish_payload, headers=header)
            check_successful_publish_response(res, test_objects_avro_evolution, partition_id)
            # unknown schema id
            unknown_payload = {f"{pl_type}_schema_id": 666, "records": [{pl_type: o} for o in second_obj]}
            res = await rest_async_client.post(url, json=unknown_payload, headers=header)
            assert res.status_code == 422
            # mismatched schema
            # TODO -> maybe this test is useless, since it tests registry behavior
            # mismatch_payload = {f"{pl_type}_schema_id": new_schema_id,"records": [{pl_type: o} for o in test_objects]}
            # res = await rest_client.post(url, json=mismatch_payload, headers=header)
            # assert res.status_code == 422, f"Expecting schema {second_schema_json} to not match records {test_objects}"


async def test_admin_client(admin_client: KafkaRestAdminClient, producer: KafkaProducer) -> None:
    topic_names = [new_topic(admin_client) for i in range(10, 13)]
    topic_info = admin_client.cluster_metadata()
    retrieved_names = list(topic_info["topics"].keys())
    assert (
        set(topic_names).difference(set(retrieved_names)) == set()
    ), "Returned value {!r} differs from written one {!r}".format(
        retrieved_names,
        topic_names,
    )
    assert len(topic_info["brokers"]) == 1, "Only one broker during tests"
    for t in topic_names:
        v = topic_info["topics"][t]
        assert len(v["partitions"]) == 1, "Should only have data for one partition"
        details = v["partitions"][0]
        assert len(details["replicas"]) == 1, "Should have only 1 replica"
    one_topic_info = admin_client.cluster_metadata(topic_names[:1])
    retrieved_names = list(one_topic_info["topics"].keys())
    assert len(retrieved_names) == 1
    assert retrieved_names[0] == topic_names[0], f"Returned value %r differs from expected {retrieved_names[0]}"
    cfg = admin_client.get_topic_config(topic_names[0])
    assert "cleanup.policy" in cfg
    for _ in range(5):
        fut = producer.send(topic_names[0], value=b"foo_val")
        producer.flush()
        _ = fut.get()
    offsets = admin_client.get_offsets(topic_names[0], 0)
    assert offsets["beginning_offset"] == 0, f"Start offset should be 0 for {topic_names[0]}, partition 0"
    assert offsets["end_offset"] == 5, f"End offset should be 0 for {topic_names[0]}, partition 0"
    # invalid requests
    with raises(UnknownTopicOrPartitionError):
        admin_client.get_offsets("invalid_topic", 0)
    with raises(UnknownTopicOrPartitionError):
        admin_client.get_offsets(topic_names[0], 10)
    with raises(UnknownTopicOrPartitionError):
        admin_client.get_topic_config("another_invalid_name")
    with raises(UnknownTopicOrPartitionError):
        admin_client.cluster_metadata(topics=["another_invalid_name"])


async def test_internal(rest_async: KafkaRest | None, admin_client: KafkaRestAdminClient) -> None:
    topic_name = new_topic(admin_client)
    prepared_records = [
        [b"key", b"value", 0],
        [b"key", b"value", None],
    ]
    rest_async_proxy = await rest_async.get_user_proxy(None)
    results = await rest_async_proxy.produce_messages(topic=topic_name, prepared_records=prepared_records)
    assert len(results) == 2
    for result in results:
        assert "error" not in result, "Valid result should not contain 'error' key"
        assert "offset" in result, "Valid result is missing 'offset' key"
        assert "partition" in result, "Valid result is missing 'partition' key"
        actual_part = result["partition"]
        assert actual_part == 0, "Returned partition id should be %d but is %d" % (0, actual_part)

    prepared_records = [
        [b"key", b"value", 100],
    ]

    results = await rest_async_proxy.produce_messages(topic=topic_name, prepared_records=prepared_records)
    assert len(results) == 1
    for result in results:
        assert "error" in result, "Invalid result missing 'error' key"
        assert result["error"] == "Unrecognized partition"
        assert "error_code" in result, "Invalid result missing 'error_code' key"
        assert result["error_code"] == 1

    assert rest_async_proxy.all_empty({"records": [{"key": {"foo": "bar"}}]}, "key") is False
    assert rest_async_proxy.all_empty({"records": [{"value": {"foo": "bar"}}]}, "value") is False
    assert rest_async_proxy.all_empty({"records": [{"value": {"foo": "bar"}}]}, "key") is True


async def test_topics(rest_async_client: Client, admin_client: KafkaRestAdminClient) -> None:
    topic_foo = "foo"
    tn = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[tn], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    res = await rest_async_client.get(f"/topics/{tn}")
    assert res.ok, "Status code is not 200: %r" % res.status_code
    data = res.json()
    assert data["name"] == tn, f"Topic name should be {tn} and is {data['name']}"
    assert "configs" in data, "'configs' key is missing : %r" % data
    assert data["configs"] != {}, "'configs' key should not be empty"
    assert "partitions" in data, "'partitions' key is missing"
    assert len(data["partitions"]) == 1, "should only have one partition"
    assert "replicas" in data["partitions"][0], "'replicas' key is missing"
    assert len(data["partitions"][0]["replicas"]) == 1, "should only have one replica"
    assert data["partitions"][0]["replicas"][0]["leader"], "Replica should be leader"
    assert data["partitions"][0]["replicas"][0]["in_sync"], "Replica should be in sync"
    res = await rest_async_client.get(f"/topics/{topic_foo}")
    assert res.status_code == 404, f"Topic {topic_foo} should not exist, status_code={res.status_code}"
    assert res.json()["error_code"] == 40403, "Error code does not match"


async def test_list_topics(rest_async_client, admin_client) -> None:
    tn1 = new_topic(admin_client)
    tn2 = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[tn1, tn2], timeout=NEW_TOPIC_TIMEOUT, sleep=1)

    # Wait until cluster_metadata cache expires
    await asyncio.sleep(3)

    res = await rest_async_client.get(f"/topics/{tn1}")
    assert res.ok, "Status code is not 200: %r" % res.status_code
    data = res.json()
    assert data["name"] == tn1, f"Topic name should be {tn1} and is {data['name']}"

    topic_list_res = await rest_async_client.get("/topics")
    assert topic_list_res.ok, "Listing topics succeeded instead of %r" % res.status_code
    topic_list = topic_list_res.json()
    assert tn1 in topic_list and tn2 in topic_list, f"Topic list contains all topics tn1={tn1} and tn2={tn2}"


async def test_publish(rest_async_client: Client, admin_client: KafkaRestAdminClient) -> None:
    topic = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    topic_url = f"/topics/{topic}"
    partition_url = f"/topics/{topic}/partitions/0"
    # Proper Json / Binary
    for url in [topic_url, partition_url]:
        for payload, h in [({"value": {"foo": "bar"}}, "json"), ({"value": "Zm9vCg=="}, "binary")]:
            res = await rest_async_client.post(url, json={"records": [payload]}, headers=REST_HEADERS[h])
            res_json = res.json()
            assert res.ok
            assert "offsets" in res_json
            if "partition" in url:
                for o in res_json["offsets"]:
                    assert "partition" in o
                    assert o["partition"] == 0


# Produce messages to a topic without key and without explicit partition to verify that
# partitioner assigns partition randomly
async def test_publish_random_partitioning(rest_async_client: Client, admin_client: KafkaRestAdminClient) -> None:
    topic = new_topic(admin_client, num_partitions=100)
    await wait_for_topics(rest_async_client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    topic_url = f"/topics/{topic}"
    partitions_seen: set[int] = set()
    do_until_time = time.monotonic() + 30
    while len(partitions_seen) < 2 and do_until_time > time.monotonic():
        res = await rest_async_client.post(
            topic_url, json={"records": [{"value": f"data{time.monotonic()}"}]}, headers=REST_HEADERS["json"]
        )
        res_json = res.json()
        assert res.ok
        assert "offsets" in res_json
        for o in res_json["offsets"]:
            assert "partition" in o
            partitions_seen.add(o["partition"])
    assert len(partitions_seen) >= 2, "Partitioner should randomly assign to different partitions if no key given"


async def test_publish_malformed_requests(rest_async_client: Client, admin_client: KafkaRestAdminClient) -> None:
    topic_name = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    for url in [f"/topics/{topic_name}", f"/topics/{topic_name}/partitions/0"]:
        # Malformed schema ++ empty records
        for js in [{"records": []}, {"foo": "bar"}, {"records": [{"valur": {"foo": "bar"}}]}]:
            res = await rest_async_client.post(url, json=js, headers=REST_HEADERS["json"])
            assert res.status_code == 422
        res = await rest_async_client.post(url, json={"records": [{"value": {"foo": "bar"}}]}, headers=REST_HEADERS["avro"])
        res_json = res.json()
        assert res.status_code == 422
        assert res_json["error_code"] == 42202
        res = await rest_async_client.post(url, json={"records": [{"key": {"foo": "bar"}}]}, headers=REST_HEADERS["avro"])
        res_json = res.json()
        assert res.status_code == 422
        assert res_json["error_code"] == 42201
        res = await rest_async_client.post(url, json={"records": [{"value": "not base64"}]}, headers=REST_HEADERS["binary"])
        res_json = res.json()
        assert res.status_code == 400
        assert res_json["error_code"] == 400

        res = await rest_async_client.post(url, json={"records": "should be an array"}, headers=REST_HEADERS["binary"])
        assert res.status_code == 400

        res = await rest_async_client.post(url, json={"records": ["object with value"]}, headers=REST_HEADERS["binary"])
        assert res.status_code == 400

        # Binary format excepts base64 encoded string as value
        res = await rest_async_client.post(url, json={"records": [{"value": {"a": 1}}]}, headers=REST_HEADERS["binary"])
        assert res.status_code == 400

        res = await rest_async_client.post(url, json={"records": [{"value": "YmFzZTY0"}]}, headers=REST_HEADERS["avro"])
        assert res.status_code == 422

        res = await rest_async_client.post(url, json={"records": [{"value": "not b64"}]}, headers=REST_HEADERS["avro"])
        assert res.status_code == 422


async def test_too_large_record(rest_async_client: Client, admin_client: KafkaRestAdminClient) -> None:
    tn = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[tn], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    # Record batch overhead is 22 bytes, reduce just above
    REDUCE_MAGIC_RECORD_BATCH_OVERHEAD = 21
    record_value_str = (REST_PRODUCER_MAX_REQUEST_BYTES - REDUCE_MAGIC_RECORD_BATCH_OVERHEAD) * "a"
    record_value = base64.b64encode(record_value_str.encode("utf8")).decode("utf8")
    pl = {"records": [{"value": record_value}]}
    res = await rest_async_client.post(f"/topics/{tn}", pl, headers={"Content-Type": "application/json"})
    assert res.status_code == 422
    assert res.json().get("offsets")[0].get("error") == (
        "The server has a configurable maximum message size to avoid unbounded memory allocation. "
        "This error is thrown if the client attempt to produce a message larger than this maximum."
    )


async def test_publish_to_nonexisting_topic(rest_async_client: Client) -> None:
    tn = new_random_name("topic-that-should-not-exist")
    header = REST_HEADERS["avro"]
    # check succeeds with 1 record and brand new schema
    urls = [f"/topics/{tn}", f"/topics/{tn}/partitions/0"]
    for url in urls:
        for pl_type in ["key", "value"]:
            correct_payload = {f"{pl_type}_schema": schema_avro_json, "records": [{pl_type: o} for o in test_objects_avro]}
            res = await rest_async_client.post(url, correct_payload, headers=header)
            assert res.status_code == 404
            assert res.json()["error_code"] == 40401, "Error code should be for topic not found"


async def test_publish_with_incompatible_data(
    rest_async_client: Client,
    registry_async_client: Client,
    admin_client: KafkaRestAdminClient,
) -> None:
    topic_name = new_topic(admin_client)
    subject_1 = f"{topic_name}-value"

    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    url = f"/topics/{topic_name}"

    schema_1 = {
        "type": "record",
        "name": "Schema1",
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
        ],
    }

    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions",
        json={"schema": json.dumps(schema_1)},
    )
    schema_1_id = res.json()["id"]

    res = await rest_async_client.post(
        url,
        json={"value_schema_id": json.dumps(schema_1_id), "records": [{"value": {"name": "Foobar"}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 200

    res = await rest_async_client.post(
        url,
        json={"value_schema_id": json.dumps(schema_1_id), "records": [{"value": {"temperature": 25}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 422
    res_json = res.json()
    assert res_json["error_code"] == 42205
    assert "message" in res_json
    assert "Object does not fit to stored schema" in res_json["message"]


async def test_publish_with_incompatible_schema(rest_async_client: Client, admin_client: KafkaRestAdminClient) -> None:
    topic_name = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    url = f"/topics/{topic_name}"

    schema_1 = {
        "type": "record",
        "name": "Schema1",
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
        ],
    }
    schema_2 = {
        "type": "record",
        "name": "Schema2",
        "fields": [
            {
                "name": "temperature",
                "type": "int",
            },
        ],
    }

    res = await rest_async_client.post(
        url,
        json={"value_schema": json.dumps(schema_1), "records": [{"value": {"name": "Foobar"}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 200

    res = await rest_async_client.post(
        url,
        json={"value_schema": json.dumps(schema_2), "records": [{"value": {"temperature": 25}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 408
    res_json = res.json()
    assert res_json["error_code"] == 40801
    assert "message" in res_json
    assert "Error when registering schema" in res_json["message"]


async def test_publish_with_schema_id_of_another_subject(
    rest_async_client: Client,
    registry_async_client: Client,
    admin_client: KafkaRestAdminClient,
) -> None:
    """
    Karapace issue 658: https://github.com/aiven/karapace/issues/658
    """
    topic_name = new_topic(admin_client)
    subject_1 = f"{topic_name}-value"
    subject_2 = "some-other-subject-value"

    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    url = f"/topics/{topic_name}"

    schema_1 = {
        "type": "record",
        "name": "Schema1",
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
        ],
    }
    schema_2 = {
        "type": "record",
        "name": "Schema2",
        "fields": [
            {
                "name": "temperature",
                "type": "int",
            },
        ],
    }

    # Register schemas to get the ids
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions",
        json={"schema": json.dumps(schema_1)},
    )
    assert res.status_code == 200
    schema_1_id = res.json()["id"]

    res = await registry_async_client.post(
        f"subjects/{subject_2}/versions",
        json={"schema": json.dumps(schema_2)},
    )
    assert res.status_code == 200
    schema_2_id = res.json()["id"]

    res = await rest_async_client.post(
        url,
        json={"value_schema_id": schema_2_id, "records": [{"value": {"temperature": 25}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 422
    res_json = res.json()
    assert res_json["error_code"] == 42205
    assert "message" in res_json
    assert "Invalid schema. format = AVRO, schema_id = 2" in res_json["message"]

    res = await rest_async_client.post(
        url,
        json={"value_schema_id": schema_1_id, "records": [{"value": {"name": "Mr. Mustache"}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 200


async def test_publish_with_schema_id_of_another_subject_novalidation(
    rest_async_novalidation_client: Client,
    registry_async_client: Client,
    admin_client: KafkaRestAdminClient,
) -> None:
    """
    Same as above but with name_strategy_validation disabled as config
    """
    topic_name = new_topic(admin_client)
    subject_1 = f"{topic_name}-value"
    subject_2 = "some-other-subject-value"

    await wait_for_topics(rest_async_novalidation_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    url = f"/topics/{topic_name}"

    schema_1 = {
        "type": "record",
        "name": "Schema1",
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
        ],
    }
    schema_2 = {
        "type": "record",
        "name": "Schema2",
        "fields": [
            {
                "name": "temperature",
                "type": "int",
            },
        ],
    }

    # Register schemas to get the ids
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions",
        json={"schema": json.dumps(schema_1)},
    )
    assert res.status_code == 200
    schema_1_id = res.json()["id"]

    res = await registry_async_client.post(
        f"subjects/{subject_2}/versions",
        json={"schema": json.dumps(schema_2)},
    )
    assert res.status_code == 200
    schema_2_id = res.json()["id"]

    res = await rest_async_novalidation_client.post(
        url,
        json={"value_schema_id": schema_2_id, "records": [{"value": {"temperature": 25}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 200  # This is fine if name_strategy_validation is disabled

    res = await rest_async_novalidation_client.post(
        url,
        json={"value_schema_id": schema_1_id, "records": [{"value": {"name": "Mr. Mustache"}}]},
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 200


async def test_brokers(rest_async_client: Client) -> None:
    res = await rest_async_client.get("/brokers")
    assert res.ok
    assert len(res.json()) == 1, "Only one broker should be running"


async def test_partitions(
    rest_async_client: Client,
    admin_client: KafkaRestAdminClient,
    producer: KafkaProducer,
) -> None:
    # TODO -> This seems to be the only combination accepted by the offsets endpoint
    topic_name = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    header = {"Accept": "*/*", "Content-Type": "application/vnd.kafka.v2+json"}
    all_partitions_res = await rest_async_client.get(f"/topics/{topic_name}/partitions")
    assert all_partitions_res.ok, "Topic should exist"
    partitions = all_partitions_res.json()
    assert len(partitions) == 1, "Only one partition should exist"
    assert len(partitions[0]["replicas"]) == 1, "Only one replica should exist"
    partition = partitions[0]
    assert partition["replicas"][0]["leader"], "Replica should be leader"
    assert partition["replicas"][0]["in_sync"], "Replica should be in sync"
    first_partition_res = await rest_async_client.get(f"/topics/{topic_name}/partitions/0")
    assert first_partition_res.ok
    partition_data = first_partition_res.json()
    assert partition_data == partition, f"Unexpected partition data: {partition_data}"

    res = await rest_async_client.get("/topics/fooo/partitions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401

    # Fill cache
    res = await rest_async_client.get("/brokers")
    assert res.ok

    res = await rest_async_client.get("/topics/fooo/partitions/0")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401

    # Clear cache
    await asyncio.sleep(3)
    res = await rest_async_client.get("/topics/fooo/partitions/0")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401

    res = await rest_async_client.get(f"/topics/{topic_name}/partitions/10")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    for _ in range(5):
        producer.send(topic_name, value=b"foo_val").get()
    offset_res = await rest_async_client.get(f"/topics/{topic_name}/partitions/0/offsets", headers=header)
    assert offset_res.ok, f"Status code {offset_res.status_code!r} is not expected: {offset_res.json()!r}"
    data = offset_res.json()
    assert data == {"beginning_offset": 0, "end_offset": 5}, "Unexpected offsets for topic {!r}: {!r}".format(
        topic_name, data
    )
    res = await rest_async_client.get("/topics/fooo/partitions/0/offsets", headers=header)
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert "Topic fooo not found" in res.json()["message"]
    res = await rest_async_client.get(f"/topics/{topic_name}/partitions/foo/offsets", headers=header)
    assert res.status_code == 404
    assert res.json()["error_code"] == 404
