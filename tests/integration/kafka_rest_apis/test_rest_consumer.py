"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import base64
import copy
import json
import logging
import random
import time

import pytest
from pytest import LogCaptureFixture

from karapace.kafka_rest_apis.consumer_manager import KNOWN_FORMATS
from tests.utils import (
    REST_HEADERS,
    consumer_valid_payload,
    new_consumer,
    new_random_name,
    new_topic,
    repeat_until_successful_request,
    schema_data,
    wait_for_topics,
)


@pytest.mark.parametrize("trail", ["", "/"])
async def test_create_and_delete(rest_async_client, trail):
    header = REST_HEADERS["json"]
    group_name = "test_group"
    resp = await rest_async_client.post(f"/consumers/{group_name}{trail}", json=consumer_valid_payload, headers=header)
    assert resp.ok
    body = resp.json()
    assert "base_uri" in body
    instance_id = body["instance_id"]
    # add with the same name fails
    with_name = copy.copy(consumer_valid_payload)
    with_name["name"] = instance_id
    resp = await rest_async_client.post(f"/consumers/{group_name}{trail}", json=with_name, headers=header)
    assert not resp.ok
    assert (
        resp.status_code == 409
    ), f"Expected conflict for instance {instance_id} and group {group_name} but got a different error: {resp.body}"
    invalid_fetch = copy.copy(consumer_valid_payload)
    # add with faulty params fails
    invalid_fetch["fetch.min.bytes"] = -10
    resp = await rest_async_client.post(f"/consumers/{group_name}{trail}", json=invalid_fetch, headers=header)
    assert not resp.ok
    assert resp.status_code == 422, f"Expected invalid fetch request value config for: {resp.body}"
    # delete followed by add succeeds
    resp = await rest_async_client.delete(f"/consumers/{group_name}/instances/{instance_id}{trail}", headers=header)
    assert resp.ok, "Could not delete "
    resp = await rest_async_client.post(f"/consumers/{group_name}{trail}", json=with_name, headers=header)
    assert resp.ok
    # delete unknown entity fails
    resp = await rest_async_client.delete(f"/consumers/{group_name}/instances/random_name{trail}")
    assert resp.status_code == 404


@pytest.mark.parametrize("trail", ["", "/"])
async def test_assignment(rest_async_client, admin_client, trail):
    header = REST_HEADERS["json"]
    instance_id = await new_consumer(rest_async_client, "assignment_group", fmt="json", trail=trail)
    assign_path = f"/consumers/assignment_group/instances/{instance_id}/assignments{trail}"
    res = await rest_async_client.get(assign_path, headers=header)
    assert res.ok, f"Expected status 200 but got {res.status_code}"
    assert "partitions" in res.json() and len(res.json()["partitions"]) == 0, "Assignment list should be empty"
    # assign one topic
    topic_name = new_topic(admin_client)
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await rest_async_client.post(assign_path, headers=header, json=assign_payload)
    assert res.ok
    assign_path = f"/consumers/assignment_group/instances/{instance_id}/assignments{trail}"
    res = await rest_async_client.get(assign_path, headers=header)
    assert res.ok, f"Expected status 200 but got {res.status_code}"
    data = res.json()
    assert "partitions" in data and len(data["partitions"]) == 1, "Should have one assignment"
    p = data["partitions"][0]
    assert p["topic"] == topic_name
    assert p["partition"] == 0


@pytest.mark.parametrize("trail", ["", "/"])
async def test_subscription(rest_async_client, admin_client, producer, trail):
    # The random name is necessary to avoid test errors, without it the second
    # parametrize test will fail. Issue: #178
    group_name = new_random_name("group")

    header = REST_HEADERS["binary"]
    topic_name = new_topic(admin_client)
    instance_id = await new_consumer(rest_async_client, group_name, fmt="binary", trail=trail)
    sub_path = f"/consumers/{group_name}/instances/{instance_id}/subscription{trail}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=5000"
    res = await rest_async_client.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 0, f"Expecting no subscription on freshly created consumer: {data}"
    # simple sub
    res = await rest_async_client.post(sub_path, json={"topics": [topic_name]}, headers=header)
    assert res.ok
    res = await rest_async_client.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert (
        "topics" in data and len(data["topics"]) == 1 and data["topics"][0] == topic_name
    ), f"expecting {topic_name} in {data}"
    for _ in range(3):
        producer.send(topic_name, value=b"foo")
    producer.flush()
    resp = await rest_async_client.get(consume_path, headers=header)
    data = resp.json()
    assert resp.ok, f"Expected a successful response: {data['message']}"
    assert len(data) == 3, f"Expected to consume 3 messages but got {data}"

    # on delete it's empty again
    res = await rest_async_client.delete(sub_path, headers=header)
    assert res.ok
    res = await rest_async_client.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 0, f"expecting {data} to be empty"
    # one pattern sub will get all 3
    # use bool(trail) to make topic prefix distinct as there has been collision when
    # test order is randomized.
    prefix = f"test_subscription_{bool(trail)}{hash(random.random())}"
    pattern_topics = [new_topic(admin_client, prefix=f"{prefix}{i}") for i in range(3)]
    await wait_for_topics(rest_async_client, topic_names=pattern_topics, timeout=20, sleep=1)
    res = await rest_async_client.post(sub_path, json={"topic_pattern": f"{prefix}.*"}, headers=REST_HEADERS["json"])
    assert res.ok

    # Consume so confluent rest reevaluates the subscription
    resp = await rest_async_client.get(consume_path, headers=header)
    assert resp.ok
    # Should we keep this behaviour

    res = await rest_async_client.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 3, "expecting subscription to 3 topics by pattern"
    subscribed_to = set(data["topics"])
    expected = set(pattern_topics)
    assert expected == subscribed_to, f"Expecting {expected} as subscribed to topics, but got {subscribed_to} instead"
    # writing to all 3 will get us results from all 3
    for t in pattern_topics:
        for _ in range(3):
            producer.send(t, value=b"bar")
        producer.flush()
    resp = await rest_async_client.get(consume_path, headers=header)
    data = resp.json()
    assert resp.ok, f"Expected a successful response: {data['message']}"
    assert len(data) == 9, f"Expected to consume 3 messages but got {data}"

    # topic name sub along with pattern will fail
    res = await rest_async_client.post(
        sub_path, json={"topics": [topic_name], "topic_pattern": "baz"}, headers=REST_HEADERS["json"]
    )
    assert res.status_code == 409, f"Invalid state error expected: {res.status_code}"
    data = res.json()
    assert data["error_code"] == 40903, f"Invalid state error expected: {data}"
    assert (
        str(data)
        == "{'error_code': 40903, 'message': 'IllegalStateError: You must choose only one way to configure your consumer:"
        " (1) subscribe to specific topics by name, (2) subscribe to topics matching a regex pattern,"
        " (3) assign itself specific topic-partitions.'}"
    )
    # assign after subscribe will fail
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await rest_async_client.post(assign_path, headers=REST_HEADERS["json"], json=assign_payload)
    assert res.status_code == 409, "Expecting status code 409 on assign after subscribe on the same consumer instance"

    # topics parameter is expected to be array, 4xx error returned
    res = await rest_async_client.post(sub_path, json={"topics": topic_name}, headers=REST_HEADERS["json"])
    assert res.status_code == 422, "Expecting status code 422 on subscription update with invalid topics param"

    # topic pattern parameter is expected to be a string, 4xx error returned
    res = await rest_async_client.post(
        sub_path, json={"topic_pattern": ["not", "a", "string"]}, headers=REST_HEADERS["json"]
    )
    assert res.status_code == 422, "Expecting status code 422 on subscription update with invalid topics param"


@pytest.mark.parametrize("trail", ["", "/"])
async def test_seek(rest_async_client, admin_client, trail):
    group = "seek_group"
    instance_id = await new_consumer(rest_async_client, group, trail=trail)
    seek_path = f"/consumers/{group}/instances/{instance_id}/positions{trail}"
    # one partition assigned, we can
    topic_name = new_topic(admin_client)
    assign_path = f"/consumers/{group}/instances/{instance_id}/assignments{trail}"
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await rest_async_client.post(assign_path, headers=REST_HEADERS["json"], json=assign_payload)
    assert res.ok
    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=20, sleep=1)
    seek_payload = {"offsets": [{"topic": topic_name, "partition": 0, "offset": 10}]}
    res = await rest_async_client.post(seek_path, json=seek_payload, headers=REST_HEADERS["json"])
    assert res.ok, f"Unexpected status for {res}"
    extreme_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    for pos in ["beginning", "end"]:
        url = f"{seek_path}/{pos}"
        res = await rest_async_client.post(url, json=extreme_payload, headers=REST_HEADERS["json"])
        assert res.ok, f"Expecting a successful response: {res}"
    # unassigned seeks should fail
    invalid_payload = {"offsets": [{"topic": "faulty", "partition": 0, "offset": 10}]}
    res = await rest_async_client.post(seek_path, json=invalid_payload, headers=REST_HEADERS["json"])
    assert res.status_code == 409, f"Expecting a failure for unassigned partition seek: {res}"


@pytest.mark.parametrize("trail", ["", "/"])
async def test_offsets(rest_async_client, admin_client, trail):
    group_name = "offset_group"
    fmt = "binary"
    header = REST_HEADERS[fmt]
    instance_id = await new_consumer(rest_async_client, group_name, fmt=fmt, trail=trail)
    topic_name = new_topic(admin_client)
    offsets_path = f"/consumers/{group_name}/instances/{instance_id}/offsets{trail}"
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
    res = await rest_async_client.post(
        assign_path, json={"partitions": [{"topic": topic_name, "partition": 0}]}, headers=header
    )
    assert res.ok, f"Unexpected response status for assignment {res}"

    await repeat_until_successful_request(
        rest_async_client.post,
        offsets_path,
        json_data={
            "offsets": [
                {
                    "topic": topic_name,
                    "partition": 0,
                    "offset": 0,
                }
            ]
        },
        headers=header,
        error_msg="Unexpected response status for offset commit",
        timeout=20,
        sleep=1,
    )

    res = await rest_async_client.get(
        offsets_path, headers=header, json={"partitions": [{"topic": topic_name, "partition": 0}]}
    )
    assert res.ok, f"Unexpected response status for {res}"
    data = res.json()
    assert "offsets" in data and len(data["offsets"]) == 1, f"Unexpected offsets response {res}"
    data = data["offsets"][0]
    assert "topic" in data and data["topic"] == topic_name, f"Unexpected topic {data}"
    assert "offset" in data and data["offset"] == 1, f"Unexpected offset {data}"
    assert "partition" in data and data["partition"] == 0, f"Unexpected partition {data}"
    res = await rest_async_client.post(
        offsets_path, json={"offsets": [{"topic": topic_name, "partition": 0, "offset": 1}]}, headers=header
    )
    assert res.ok, f"Unexpected response status for offset commit {res}"

    res = await rest_async_client.get(
        offsets_path, headers=header, json={"partitions": [{"topic": topic_name, "partition": 0}]}
    )
    assert res.ok, f"Unexpected response status for {res}"
    data = res.json()
    assert "offsets" in data and len(data["offsets"]) == 1, f"Unexpected offsets response {res}"
    data = data["offsets"][0]
    assert "topic" in data and data["topic"] == topic_name, f"Unexpected topic {data}"
    assert "offset" in data and data["offset"] == 2, f"Unexpected offset {data}"
    assert "partition" in data and data["partition"] == 0, f"Unexpected partition {data}"


@pytest.mark.parametrize("trail", ["", "/"])
async def test_offsets_no_payload(rest_async_client, admin_client, producer, trail, caplog: LogCaptureFixture):
    group_name = "offset_group_no_payload"
    fmt = "binary"
    header = REST_HEADERS[fmt]
    instance_id = await new_consumer(
        rest_async_client,
        group_name,
        fmt=fmt,
        trail=trail,
        # By default this is true
        payload_override={"auto.commit.enable": "false"},
    )
    topic_name = new_topic(admin_client)
    offsets_path = f"/consumers/{group_name}/instances/{instance_id}/offsets{trail}"
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=5000"

    res = await rest_async_client.post(
        assign_path,
        json={"partitions": [{"topic": topic_name, "partition": 0}]},
        headers=header,
    )
    assert res.ok, f"Unexpected response status for assignment {res}"

    producer.send(topic_name, value=b"message-value")
    producer.flush()

    # Commit should not throw any error, even before consuming events
    res = await rest_async_client.post(offsets_path, headers=header, json={})
    assert res.ok, f"Expected a successful response: {res}"
    with caplog.at_level(logging.WARNING, logger="karapace.kafka_rest_apis.consumer_manager"):
        assert any("Ignoring KafkaError: No offset stored" in log.message for log in caplog.records)

    resp = await rest_async_client.get(consume_path, headers=header)
    assert resp.ok, f"Expected a successful response: {resp}"

    await repeat_until_successful_request(
        rest_async_client.post,
        offsets_path,
        json_data={},
        headers=header,
        error_msg="Unexpected response status for offset commit",
        timeout=20,
        sleep=1,
    )

    res = await rest_async_client.get(
        offsets_path,
        headers=header,
        json={"partitions": [{"topic": topic_name, "partition": 0}]},
    )
    assert res.ok, f"Unexpected response status for {res}"
    data = res.json()
    assert "offsets" in data and len(data["offsets"]) == 1, f"Unexpected offsets response {res}"
    data = data["offsets"][0]
    assert "topic" in data and data["topic"] == topic_name, f"Unexpected topic {data}"
    assert "offset" in data and data["offset"] == 1, f"Unexpected offset {data}"
    assert "partition" in data and data["partition"] == 0, f"Unexpected partition {data}"
    res = await rest_async_client.post(
        offsets_path,
        json={"offsets": [{"topic": topic_name, "partition": 0, "offset": 1}]},
        headers=header,
    )
    assert res.ok, f"Unexpected response status for offset commit {res}"

    res = await rest_async_client.get(
        offsets_path,
        headers=header,
        json={"partitions": [{"topic": topic_name, "partition": 0}]},
    )
    assert res.ok, f"Unexpected response status for {res}"
    data = res.json()
    assert "offsets" in data and len(data["offsets"]) == 1, f"Unexpected offsets response {res}"
    data = data["offsets"][0]
    assert "topic" in data and data["topic"] == topic_name, f"Unexpected topic {data}"
    assert "offset" in data and data["offset"] == 2, f"Unexpected offset {data}"
    assert "partition" in data and data["partition"] == 0, f"Unexpected partition {data}"


@pytest.mark.parametrize("trail", ["", "/"])
async def test_consume(rest_async_client, admin_client, producer, trail):
    # avro to be handled in a separate testcase ??
    values = {
        "json": [json.dumps({"foo": f"bar{i}"}).encode("utf-8") for i in range(3)],
        "binary": [f"val{i}".encode() for i in range(3)],
    }
    deserializers = {"binary": base64.b64decode, "json": lambda x: json.dumps(x).encode("utf-8")}
    group_name = "consume_group"
    for fmt in ["binary", "json"]:
        header = copy.deepcopy(REST_HEADERS[fmt])
        instance_id = await new_consumer(rest_async_client, group_name, fmt=fmt, trail=trail)
        assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
        seek_path = f"/consumers/{group_name}/instances/{instance_id}/positions/beginning{trail}"
        consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=5000"
        topic_name = new_topic(admin_client)
        assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        res = await rest_async_client.post(assign_path, json=assign_payload, headers=header)
        assert res.ok
        for i in range(len(values[fmt])):
            producer.send(topic_name, value=values[fmt][i])
        producer.flush()
        seek_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        resp = await rest_async_client.post(seek_path, headers=header, json=seek_payload)
        assert resp.ok
        header["Accept"] = f"application/vnd.kafka.{fmt}.v2+json"
        resp = await rest_async_client.get(consume_path, headers=header)
        assert resp.ok, f"Expected a successful response: {resp}"
        data = resp.json()
        assert len(data) == len(values[fmt]), f"Expected {len(values[fmt])} element in response: {resp}"
        for i in range(len(values[fmt])):
            assert data[i]["topic"] == topic_name
            assert data[i]["partition"] == 0
            assert data[i]["offset"] >= 0
            assert data[i]["timestamp"] > 0
            assert (
                deserializers[fmt](data[i]["value"]) == values[fmt][i]
            ), f"Extracted data {deserializers[fmt](data[i]['value'])} does not match {values[fmt][i]} for format {fmt}"


async def test_consume_timeout(rest_async_client, admin_client, producer):
    values = {
        "json": [json.dumps({"foo": f"bar{i}"}).encode("utf-8") for i in range(3)],
        "binary": [f"val{i}".encode() for i in range(3)],
    }
    deserializers = {"binary": base64.b64decode, "json": lambda x: json.dumps(x).encode("utf-8")}
    group_name = "consume_group"
    for fmt in ["binary", "json"]:
        header = copy.deepcopy(REST_HEADERS[fmt])
        instance_id = await new_consumer(rest_async_client, group_name, fmt=fmt)
        assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments"
        seek_path = f"/consumers/{group_name}/instances/{instance_id}/positions/beginning"
        consume_path = f"/consumers/{group_name}/instances/{instance_id}/records?timeout=1000"
        topic_name = new_topic(admin_client)
        assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        res = await rest_async_client.post(assign_path, json=assign_payload, headers=header)
        assert res.ok
        for i in range(len(values[fmt])):
            producer.send(topic_name, value=values[fmt][i])
        producer.flush()
        seek_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        resp = await rest_async_client.post(seek_path, headers=header, json=seek_payload)
        assert resp.ok
        header["Accept"] = f"application/vnd.kafka.{fmt}.v2+json"
        resp = await rest_async_client.get(consume_path, headers=header)
        assert resp.ok, f"Expected a successful response: {resp}"
        data = resp.json()
        assert len(data) == len(values[fmt]), f"Expected {len(values[fmt])} element in response: {resp}"
        for i in range(len(values[fmt])):
            assert (
                deserializers[fmt](data[i]["value"]) == values[fmt][i]
            ), f"Extracted data {deserializers[fmt](data[i]['value'])} does not match {values[fmt][i]} for format {fmt}"

        # Now read more using explicit 5s timeout
        start_time = time.monotonic()
        resp = await rest_async_client.get(
            f"/consumers/{group_name}/instances/{instance_id}/records?timeout=5000", headers=header
        )
        duration = time.monotonic() - start_time
        assert resp.ok, f"Expected a successful response: {resp}"
        data = resp.json()
        assert len(data) == 0, f"Expected zero elements now in response: {resp}"
        assert 5.0 <= duration < 10.0, f"Expected duration {duration} roughly aligned with requested timeout"

        # Now read more (expect using service configure timeout)
        start_time = time.monotonic()
        resp = await rest_async_client.get(f"/consumers/{group_name}/instances/{instance_id}/records", headers=header)
        duration = time.monotonic() - start_time
        assert resp.ok, f"Expected a successful response: {resp}"
        data = resp.json()
        assert len(data) == 0, f"Expected zero elements now in response: {resp}"
        # Default consumer_request_timeout_ms is 11000 milliseconds
        assert 11.0 <= duration < 16.0, f"Expected duration {duration} roughly aligned with configured timeout"


@pytest.mark.parametrize("schema_type", ["avro"])
@pytest.mark.parametrize("trail", ["", "/"])
async def test_publish_consume_avro(rest_async_client, admin_client, trail, schema_type):
    header = REST_HEADERS[schema_type]
    group_name = "e2e_group"
    instance_id = await new_consumer(rest_async_client, group_name, fmt=schema_type, trail=trail)
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=5000"
    tn = new_topic(admin_client)
    assign_payload = {"partitions": [{"topic": tn, "partition": 0}]}
    res = await rest_async_client.post(assign_path, json=assign_payload, headers=header)
    assert res.ok
    publish_payload = schema_data[schema_type][1]
    await repeat_until_successful_request(
        rest_async_client.post,
        f"topics/{tn}{trail}",
        json_data={"value_schema": schema_data[schema_type][0], "records": [{"value": o} for o in publish_payload]},
        headers=header,
        error_msg="Unexpected response status for offset commit",
        timeout=10,
        sleep=1,
    )
    resp = await rest_async_client.get(consume_path, headers=header)
    assert resp.ok, f"Expected a successful response: {resp}"
    data = resp.json()
    assert len(data) == len(publish_payload), f"Expected to read test_objects from fetch request but got {data}"
    data_values = [x["value"] for x in data]
    for expected, actual in zip(publish_payload, data_values):
        assert expected == actual, f"Expecting {actual} to be {expected}"


@pytest.mark.parametrize("fmt", ["avro"])
async def test_consume_avro_key_deserialization_error_fallback(
    rest_async_client,
    admin_client,
    caplog: LogCaptureFixture,
    fmt,
):
    topic_name = new_topic(admin_client, prefix=f"{fmt}_")

    # Produce a record with binary key and empty value
    header = REST_HEADERS["binary"]
    binary_key = b"testkey"
    publish_key = base64.b64encode(binary_key)
    publish_payload = publish_key.decode("utf-8")
    await repeat_until_successful_request(
        rest_async_client.post,
        f"topics/{topic_name}",
        json_data={"records": [{"key": f"{publish_payload}"}]},
        headers=header,
        error_msg="Unexpected response status for offset commit",
        timeout=10,
        sleep=1,
    )

    # Test consuming the record using avro format
    headers = REST_HEADERS[fmt]
    group_name = f"e2e_group_{fmt}"
    instance_id = await new_consumer(rest_async_client, group_name, fmt=fmt)
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments"
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await rest_async_client.post(assign_path, json=assign_payload, headers=headers)
    assert res.ok, f"Expected a successful response: {res}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records?timeout=1000"
    res2 = await rest_async_client.get(consume_path, headers=headers)
    assert res2.ok, f"Expected a successful response: {res2}"

    # Key-deserialization error should automatically fallback to binary
    with caplog.at_level(logging.WARNING, logger="karapace.kafka_rest_apis.consumer_manager"):
        assert any(
            "Cannot process non-empty key using avro deserializer, falling back to binary." in log.message
            for log in caplog.records
        )
    data = res2.json()
    data_keys = [x["key"] for x in data]
    for data_key in data_keys:
        assert publish_payload == data_key, f"Expecting {data_key} to be {publish_payload}"


@pytest.mark.parametrize("fmt", sorted(KNOWN_FORMATS))
async def test_consume_grafecul_deserialization_error_handling(rest_async_client, admin_client, fmt):
    topic_name = new_topic(admin_client, prefix=f"{fmt}_")

    # Produce binary record
    headers = REST_HEADERS["binary"]
    resp = await rest_async_client.post(f"topics/{topic_name}", json={"records": [{"value": "dGVzdA=="}]}, headers=headers)
    assert resp.ok, f"Expected a successful response: {resp}"

    # Test consuming records with different formats
    headers = REST_HEADERS[fmt]
    group_name = f"e2e_group_{fmt}"
    instance_id = await new_consumer(rest_async_client, group_name, fmt=fmt)

    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments"
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await rest_async_client.post(assign_path, json=assign_payload, headers=headers)
    assert res.ok

    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records?timeout=5000"
    resp = await rest_async_client.get(consume_path, headers=headers)
    if fmt == "binary":
        assert resp.status_code == 200, f"Expected 200 response: {resp}"
    else:
        # Consuming records should fail gracefully if record can not be deserialized to the selected format
        assert resp.status_code == 422, f"Expected 422 response: {resp}"
        assert f"value deserialization error for format {fmt}" in resp.json()["message"]
