from tests.utils import consumer_valid_payload, new_consumer, new_topic, REST_HEADERS, schema_data

import base64
import copy
import json
import pytest
import random


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
    assert resp.status == 409, f"Expected conflict for instance {instance_id} and group {group_name} " \
                               f"but got a different error: {resp.body}"
    invalid_fetch = copy.copy(consumer_valid_payload)
    # add with faulty params fails
    invalid_fetch["fetch.min.bytes"] = -10
    resp = await rest_async_client.post(f"/consumers/{group_name}{trail}", json=invalid_fetch, headers=header)
    assert not resp.ok
    assert resp.status == 422, f"Expected invalid fetch request value config for: {resp.body}"
    # delete followed by add succeeds
    resp = await rest_async_client.delete(f"/consumers/{group_name}/instances/{instance_id}{trail}", headers=header)
    assert resp.ok, "Could not delete "
    resp = await rest_async_client.post(f"/consumers/{group_name}{trail}", json=with_name, headers=header)
    assert resp.ok
    # delete unknown entity fails
    resp = await rest_async_client.delete(f"/consumers/{group_name}/instances/random_name{trail}")
    assert resp.status == 404


@pytest.mark.parametrize("trail", ["", "/"])
async def test_assignment(rest_async_client, admin_client, trail):
    header = REST_HEADERS["json"]
    instance_id = await new_consumer(rest_async_client, "assignment_group", fmt="json", trail=trail)
    assign_path = f"/consumers/assignment_group/instances/{instance_id}/assignments{trail}"
    res = await rest_async_client.get(assign_path, headers=header)
    assert res.ok, f"Expected status 200 but got {res.status}"
    assert "partitions" in res.json() and len(res.json()["partitions"]) == 0, "Assignment list should be empty"
    # assign one topic
    topic_name = new_topic(admin_client)
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await rest_async_client.post(assign_path, headers=header, json=assign_payload)
    assert res.ok
    assign_path = f"/consumers/assignment_group/instances/{instance_id}/assignments{trail}"
    res = await rest_async_client.get(assign_path, headers=header)
    assert res.ok, f"Expected status 200 but got {res.status}"
    data = res.json()
    assert "partitions" in data and len(data["partitions"]) == 1, "Should have one assignment"
    p = data["partitions"][0]
    assert p["topic"] == topic_name
    assert p["partition"] == 0


@pytest.mark.parametrize("trail", ["", "/"])
async def test_subscription(rest_async_client, admin_client, producer, trail):
    header = REST_HEADERS["binary"]
    group_name = "sub_group"
    topic_name = new_topic(admin_client)
    instance_id = await new_consumer(rest_async_client, group_name, fmt="binary", trail=trail)
    sub_path = f"/consumers/{group_name}/instances/{instance_id}/subscription{trail}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=1000"
    res = await rest_async_client.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 0, \
        f"Expecting no subscription on freshly created consumer: {data}"
    # simple sub
    res = await rest_async_client.post(sub_path, json={"topics": [topic_name]}, headers=header)
    assert res.ok
    res = await rest_async_client.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 1 and data["topics"][0] == topic_name, \
        f"expecting {topic_name} in {data}"
    for _ in range(3):
        producer.send(topic_name, b"foo").get()
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
    prefix = f"{hash(random.random())}"
    pattern_topics = [new_topic(admin_client, prefix=f"{prefix}{i}") for i in range(3)]
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
            producer.send(t, b"bar").get()
    resp = await rest_async_client.get(consume_path, headers=header)
    data = resp.json()
    assert resp.ok, f"Expected a successful response: {data['message']}"
    assert len(data) == 9, f"Expected to consume 3 messages but got {data}"

    # topic name sub along with pattern will fail
    res = await rest_async_client.post(
        sub_path, json={
            "topics": [topic_name],
            "topic_pattern": "baz"
        }, headers=REST_HEADERS["json"]
    )
    assert res.status == 409, f"Invalid state error expected: {res.status}"
    data = res.json()
    assert data["error_code"] == 40903, f"Invalid state error expected: {data}"
    # assign after subscribe will fail
    assign_path = f"/consumers/sub_group/instances/{instance_id}/assignments{trail}"
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await rest_async_client.post(assign_path, headers=REST_HEADERS["json"], json=assign_payload)
    assert res.status == 409, f"Expecting status code 409 on assign after subscribe on the same consumer instance"


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
    assert res.status == 409, f"Expecting a failure for unassigned partition seek: {res}"


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
        assign_path, json={"partitions": [{
            "topic": topic_name,
            "partition": 0
        }]}, headers=header
    )
    assert res.ok, f"Unexpected response status for assignment {res}"

    res = await rest_async_client.post(
        offsets_path, json={"offsets": [{
            "topic": topic_name,
            "partition": 0,
            "offset": 0
        }]}, headers=header
    )
    assert res.ok, f"Unexpected response status for offset commit {res}"

    res = await rest_async_client.get(
        offsets_path, headers=header, json={"partitions": [{
            "topic": topic_name,
            "partition": 0
        }]}
    )
    assert res.ok, f"Unexpected response status for {res}"
    data = res.json()
    assert "offsets" in data and len(data["offsets"]) == 1, f"Unexpected offsets response {res}"
    data = data["offsets"][0]
    assert "topic" in data and data["topic"] == topic_name, f"Unexpected topic {data}"
    assert "offset" in data and data["offset"] == 1, f"Unexpected offset {data}"
    assert "partition" in data and data["partition"] == 0, f"Unexpected partition {data}"
    res = await rest_async_client.post(
        offsets_path, json={"offsets": [{
            "topic": topic_name,
            "partition": 0,
            "offset": 1
        }]}, headers=header
    )
    assert res.ok, f"Unexpected response status for offset commit {res}"

    res = await rest_async_client.get(
        offsets_path, headers=header, json={"partitions": [{
            "topic": topic_name,
            "partition": 0
        }]}
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
        "json": [json.dumps({
            "foo": f"bar{i}"
        }).encode("utf-8") for i in range(3)],
        "binary": [f"val{i}".encode('utf-8') for i in range(3)]
    }
    deserializers = {"binary": base64.b64decode, "json": lambda x: json.dumps(x).encode("utf-8")}
    group_name = "consume_group"
    for fmt in ["binary", "json"]:
        header = copy.deepcopy(REST_HEADERS[fmt])
        instance_id = await new_consumer(rest_async_client, group_name, fmt=fmt, trail=trail)
        assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
        seek_path = f"/consumers/{group_name}/instances/{instance_id}/positions/beginning{trail}"
        consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=1000"
        topic_name = new_topic(admin_client)
        assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        res = await rest_async_client.post(assign_path, json=assign_payload, headers=header)
        assert res.ok
        for i in range(len(values[fmt])):
            producer.send(topic_name, value=values[fmt][i]).get()
        seek_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        resp = await rest_async_client.post(seek_path, headers=header, json=seek_payload)
        assert resp.ok
        header["Accept"] = f"application/vnd.kafka.{fmt}.v2+json"
        resp = await rest_async_client.get(consume_path, headers=header)
        assert resp.ok, f"Expected a successful response: {resp}"
        data = resp.json()
        assert len(data) == len(values[fmt]), f"Expected {len(values[fmt])} element in response: {resp}"
        for i in range(len(values[fmt])):
            assert deserializers[fmt](data[i]["value"]) == values[fmt][i], \
                f"Extracted data {deserializers[fmt](data[i]['value'])}" \
                f" does not match {values[fmt][i]} for format {fmt}"


@pytest.mark.parametrize("schema_type", ["avro", "json"])
@pytest.mark.parametrize("trail", ["", "/"])
async def test_publish_consume_avro(rest_async_client, admin_client, trail, schema_type):
    header = REST_HEADERS[schema_type]
    group_name = "e2e_group"
    instance_id = await new_consumer(rest_async_client, group_name, fmt=schema_type, trail=trail)
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=1000"
    tn = new_topic(admin_client)
    assign_payload = {"partitions": [{"topic": tn, "partition": 0}]}
    res = await rest_async_client.post(assign_path, json=assign_payload, headers=header)
    assert res.ok
    publish_payload = schema_data[schema_type][1]
    pl = {"value_schema": schema_data[schema_type][0], "records": [{"value": o} for o in publish_payload]}
    res = await rest_async_client.post(f"topics/{tn}{trail}", json=pl, headers=header)
    assert res.ok
    resp = await rest_async_client.get(consume_path, headers=header)
    assert resp.ok, f"Expected a successful response: {resp}"
    data = resp.json()
    assert len(data) == len(publish_payload), f"Expected to read test_objects from fetch request but got {data}"
    data_values = [x["value"] for x in data]
    for expected, actual in zip(publish_payload, data_values):
        assert expected == actual, f"Expecting {actual} to be {expected}"
