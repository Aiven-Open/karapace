from kafka import KafkaProducer
from karapace.kafka_rest_apis import KafkaRestAdminClient
from tests.utils import client_for, new_topic, REST_HEADERS, schema_json, test_objects
from unittest.mock import MagicMock

import base64
import copy
import json
import os
import pytest

pytest_plugins = "aiohttp.pytest_plugin"
valid_payload = {
    "format": "avro",
    "auto.offset.reset": "earliest",
    "consumer.request.timeout.ms": 11000,
    "fetch.min.bytes": 10000,
    "auto.commit.enable": "true"
}


async def new_consumer(c, group, fmt="avro"):
    payload = copy.copy(valid_payload)
    payload["format"] = fmt
    resp = await c.post(f"/consumers/{group}", json=payload, headers=REST_HEADERS["avro"])
    assert resp.ok
    return resp.json()["instance_id"]


async def test_create_and_delete(kafka_rest, aiohttp_client):
    kafka_rest, _ = kafka_rest()
    c = await client_for(kafka_rest, aiohttp_client)
    header = REST_HEADERS["json"]
    group_name = "test_group"
    resp = await c.post(f"/consumers/{group_name}", json=valid_payload, headers=header)
    assert resp.ok
    body = resp.json()
    assert "base_uri" in body
    instance_id = body["instance_id"]
    # add with the same name fails
    with_name = copy.copy(valid_payload)
    with_name["name"] = instance_id
    resp = await c.post(f"/consumers/{group_name}", json=with_name, headers=header)
    assert not resp.ok
    assert resp.status == 409, f"Expected conflict for instance {instance_id} and group {group_name} " \
                               f"but got a different error: {resp.body}"
    invalid_fetch = copy.copy(valid_payload)
    # add with faulty params fails
    invalid_fetch["fetch.min.bytes"] = -10
    resp = await c.post(f"/consumers/{group_name}", json=invalid_fetch, headers=header)
    assert not resp.ok
    assert resp.status == 422, f"Expected invalid fetch request value config for: {resp.body}"
    # delete followed by add succeeds
    resp = await c.delete(f"/consumers/{group_name}/instances/{instance_id}", headers=header)
    assert resp.ok, "Could not delete "
    resp = await c.post(f"/consumers/{group_name}", json=with_name, headers=header)
    assert resp.ok
    # delete unknown entity fails
    resp = await c.delete(f"/consumers/{group_name}/instances/random_name")
    assert resp.status == 404
    await c.close()


async def test_assignment(kafka_rest, aiohttp_client, admin_client):
    # no assignments retrieved
    header = REST_HEADERS["json"]
    c = await client_for(kafka_rest()[0], aiohttp_client)
    instance_id = await new_consumer(c, "assignment_group", fmt="json")
    assign_path = f"/consumers/assignment_group/instances/{instance_id}/assignments"
    res = await c.get(assign_path, headers=header)
    assert res.ok, f"Expected status 200 but got {res.status}"
    assert "partitions" in res.json() and len(res.json()["partitions"]) == 0, "Assignment list should be empty"
    # assign one topic
    topic_name = new_topic(admin_client)
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await c.post(assign_path, headers=header, json=assign_payload)
    assert res.ok
    assign_path = f"/consumers/assignment_group/instances/{instance_id}/assignments"
    res = await c.get(assign_path, headers=header)
    assert res.ok, f"Expected status 200 but got {res.status}"
    data = res.json()
    assert "partitions" in data and len(data["partitions"]) == 1, "Should have one assignment"
    p = data["partitions"][0]
    assert p["topic"] == topic_name
    assert p["partition"] == 0
    await c.close()


async def test_subscription(kafka_rest, aiohttp_client, admin_client, producer):
    header = REST_HEADERS["binary"]
    kr, _ = kafka_rest()
    c = await client_for(kr, aiohttp_client)
    group_name = "sub_group"
    topic_name = new_topic(admin_client)
    instance_id = await new_consumer(c, group_name, fmt="binary")
    sub_path = f"/consumers/{group_name}/instances/{instance_id}/subscription"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records?timeout=1000"
    res = await c.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 0, \
        f"Expecting no subscription on freshly created consumer: {data}"
    # simple sub
    res = await c.post(sub_path, json={"topics": [topic_name]}, headers=header)
    assert res.ok
    res = await c.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 1 and data["topics"][0] == topic_name, \
        f"expecting {topic_name} in {data}"
    for _ in range(3):
        producer.send(topic_name, b"foo").get()
    resp = await c.get(consume_path, headers=header)
    data = resp.json()
    assert resp.ok, f"Expected a successful response: {data['message']}"
    assert len(data) == 3, f"Expected to consume 3 messages but got {data}"

    # on delete it's empty again
    res = await c.delete(sub_path, headers=header)
    assert res.ok
    res = await c.get(sub_path, headers=header)
    assert res.ok
    data = res.json()
    assert "topics" in data and len(data["topics"]) == 0, f"expecting {data} to be empty"
    # one pattern sub will get all 3
    pattern_topics = [new_topic(admin_client, prefix="subscription%d" % i) for i in range(3)]
    res = await c.post(sub_path, json={"topic_pattern": "subscription.*"}, headers=REST_HEADERS["json"])
    assert res.ok

    # Consume so confluent rest reevaluates the subscription
    resp = await c.get(consume_path, headers=header)
    assert resp.ok
    # Should we keep this behaviour

    res = await c.get(sub_path, headers=header)
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
    resp = await c.get(consume_path, headers=header)
    data = resp.json()
    assert resp.ok, f"Expected a successful response: {data['message']}"
    assert len(data) == 9, f"Expected to consume 3 messages but got {data}"

    # topic name sub along with pattern will fail
    res = await c.post(sub_path, json={"topics": [topic_name], "topic_pattern": "baz"}, headers=REST_HEADERS["json"])
    assert res.status == 409, f"Invalid state error expected: {res.status}"
    data = res.json()
    assert data["error_code"] == 40903, f"Invalid state error expected: {data}"
    # assign after subscribe will fail
    assign_path = f"/consumers/sub_group/instances/{instance_id}/assignments"
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await c.post(assign_path, headers=REST_HEADERS["json"], json=assign_payload)
    assert res.status == 409, f"Expecting status code 409 on assign after" \
                              f" subscribe on the same consumer instance: {list(kr.consumer_manager.consumers.keys())}"
    await c.close()
    producer.close()


async def test_seek(kafka_rest, aiohttp_client, admin_client):
    c = await client_for(kafka_rest()[0], aiohttp_client)
    group = "seek_group"
    instance_id = await new_consumer(c, group)
    seek_path = f"/consumers/{group}/instances/{instance_id}/positions"
    # one partition assigned, we can
    topic_name = new_topic(admin_client)
    assign_path = f"/consumers/{group}/instances/{instance_id}/assignments"
    assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    res = await c.post(assign_path, headers=REST_HEADERS["json"], json=assign_payload)
    assert res.ok
    seek_payload = {"offsets": [{"topic": topic_name, "partition": 0, "offset": 10}]}
    res = await c.post(seek_path, json=seek_payload, headers=REST_HEADERS["json"])
    assert res.ok, f"Unexpected status for {res}"
    extreme_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
    for pos in ["beginning", "end"]:
        url = f"{seek_path}/{pos}"
        res = await c.post(url, json=extreme_payload, headers=REST_HEADERS["json"])
        assert res.ok, f"Expecting a successful response: {res}"
    # unassigned seeks should fail
    invalid_payload = {"offsets": [{"topic": "faulty", "partition": 0, "offset": 10}]}
    res = await c.post(seek_path, json=invalid_payload, headers=REST_HEADERS["json"])
    assert res.status == 409, f"Expecting a failure for unassigned partition seek: {res}"
    await c.close()


async def test_offsets(kafka_rest, aiohttp_client, admin_client):
    # will also take this opportunity to use subscriptions
    kafka_rest, _ = kafka_rest()
    rest_client = await client_for(kafka_rest, aiohttp_client)
    group_name = "offset_group"
    fmt = "binary"
    header = REST_HEADERS[fmt]
    instance_id = await new_consumer(rest_client, group_name, fmt=fmt)
    topic_name = new_topic(admin_client)
    offsets_path = f"/consumers/{group_name}/instances/{instance_id}/offsets"
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments"
    res = await rest_client.post(assign_path, json={"partitions": [{"topic": topic_name, "partition": 0}]}, headers=header)
    assert res.ok, f"Unexpected response status for assignment {res}"

    res = await rest_client.post(
        offsets_path, json={"offsets": [{
            "topic": topic_name,
            "partition": 0,
            "offset": 0
        }]}, headers=header
    )
    assert res.ok, f"Unexpected response status for offset commit {res}"

    res = await rest_client.get(offsets_path, headers=header, json={"partitions": [{"topic": topic_name, "partition": 0}]})
    assert res.ok, f"Unexpected response status for {res}"
    data = res.json()
    assert "offsets" in data and len(data["offsets"]) == 1, f"Unexpected offsets response {res}"
    data = data["offsets"][0]
    assert "topic" in data and data["topic"] == topic_name, f"Unexpected topic {data}"
    assert "offset" in data and data["offset"] == 1, f"Unexpected offset {data}"
    assert "partition" in data and data["partition"] == 0, f"Unexpected partition {data}"
    res = await rest_client.post(
        offsets_path, json={"offsets": [{
            "topic": topic_name,
            "partition": 0,
            "offset": 1
        }]}, headers=header
    )
    assert res.ok, f"Unexpected response status for offset commit {res}"

    res = await rest_client.get(offsets_path, headers=header, json={"partitions": [{"topic": topic_name, "partition": 0}]})
    assert res.ok, f"Unexpected response status for {res}"
    data = res.json()
    assert "offsets" in data and len(data["offsets"]) == 1, f"Unexpected offsets response {res}"
    data = data["offsets"][0]
    assert "topic" in data and data["topic"] == topic_name, f"Unexpected topic {data}"
    assert "offset" in data and data["offset"] == 2, f"Unexpected offset {data}"
    assert "partition" in data and data["partition"] == 0, f"Unexpected partition {data}"
    await rest_client.close()


async def test_consume(kafka_rest, aiohttp_client, admin_client, producer):
    # avro to be handled in a separate testcase ??
    values = {
        "json": [json.dumps({
            "foo": f"bar{i}"
        }).encode("utf-8") for i in range(3)],
        "binary": [f"val{i}".encode('utf-8') for i in range(3)]
    }
    deserializers = {"binary": base64.b64decode, "json": lambda x: json.dumps(x).encode("utf-8")}
    kafka_rest, _ = kafka_rest()
    rest_client = await client_for(kafka_rest, aiohttp_client)
    group_name = "consume_group"
    for fmt in ["binary", "json"]:
        header = copy.deepcopy(REST_HEADERS[fmt])
        instance_id = await new_consumer(rest_client, group_name, fmt=fmt)
        assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments"
        seek_path = f"/consumers/{group_name}/instances/{instance_id}/positions/beginning"
        consume_path = f"/consumers/{group_name}/instances/{instance_id}/records?timeout=1000"
        topic_name = new_topic(admin_client)
        assign_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        res = await rest_client.post(assign_path, json=assign_payload, headers=header)
        assert res.ok
        for i in range(len(values[fmt])):
            producer.send(topic_name, value=values[fmt][i]).get()
        seek_payload = {"partitions": [{"topic": topic_name, "partition": 0}]}
        resp = await rest_client.post(seek_path, headers=header, json=seek_payload)
        assert resp.ok
        header["Accept"] = f"application/vnd.kafka.{fmt}.v2+json"
        resp = await rest_client.get(consume_path, headers=header)
        assert resp.ok, f"Expected a successful response: {resp}"
        data = resp.json()
        assert len(data) == len(values[fmt]), f"Expected {len(values[fmt])} element in response: {resp}"
        for i in range(len(values[fmt])):
            assert deserializers[fmt](data[i]["value"]) == values[fmt][i], \
                f"Extracted data {deserializers[fmt](data[i]['value'])}" \
                f" does not match {values[fmt][i]} for format {fmt}"
    await rest_client.close()


async def test_publish_consume_avro(kafka_rest, karapace, aiohttp_client, admin_client):
    header = REST_HEADERS["avro"]
    kafka_rest, _ = kafka_rest()
    rest_client = await client_for(kafka_rest, aiohttp_client)
    karapace, _ = karapace()
    registry_client = await client_for(karapace, aiohttp_client)
    kafka_rest.serializer.registry_client.client = registry_client
    kafka_rest.consumer_manager.deserializer.registry_client.client = registry_client
    group_name = "e2e_group"
    instance_id = await new_consumer(rest_client, group_name, fmt="avro")
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records?timeout=1000"
    tn = new_topic(admin_client)
    assign_payload = {"partitions": [{"topic": tn, "partition": 0}]}
    res = await rest_client.post(assign_path, json=assign_payload, headers=header)
    assert res.ok

    pl = {"value_schema": schema_json, "records": [{"value": o} for o in test_objects]}
    res = await rest_client.post(f"topics/{tn}", json=pl, headers=header)
    assert res.ok
    resp = await rest_client.get(consume_path, headers=header)
    assert resp.ok, f"Expected a successful response: {resp}"
    data = resp.json()
    assert len(data) == len(test_objects), f"Expected to read test_objects from fetch request but got {data}"
    data_values = [x["value"] for x in data]
    for expected, actual in zip(test_objects, data_values):
        assert expected == actual, f"Expecting {actual} to be {expected}"


async def test_remote():
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

    if "REST_URI" not in os.environ or "REGISTRY_URI" not in os.environ:
        pytest.skip("Cannot run remote test: REST_URI and REGISTRY_URI env vars set")
    # from here on we assume they all work on the same host
    print("Test publish consume avro")  # OK
    await test_publish_consume_avro(mock_factory("rest"), mock_factory("registry"), None, KafkaRestAdminClient())
    print("Test consume")  # OK
    await test_consume(mock_factory("rest"), None, KafkaRestAdminClient(), KafkaProducer())
    print("Test offsets")  # OK
    await test_offsets(mock_factory("rest"), None, KafkaRestAdminClient())
    print("Test seek")  # OK
    await test_seek(mock_factory("rest"), None, KafkaRestAdminClient())
    print("Test subscription")  # OK if we do another consume and force a sub recheck
    await test_subscription(mock_factory("rest"), None, KafkaRestAdminClient(), KafkaProducer())
    print("Test assignment")  # OK
    await test_assignment(mock_factory("rest"), None, KafkaRestAdminClient())
    print("Test create delete")  # OK
    await test_create_and_delete(mock_factory("rest"), None)
    # # why not
    import tests.test_rest as rs
    print("Test avro publish")  # OK , need to look at compatibility checks though
    await rs.test_avro_publish(mock_factory("rest"), mock_factory("registry"), None, KafkaRestAdminClient())
    print("Test what's left i guess")  #
    await rs.test_local(mock_factory("rest"), None, KafkaProducer(), KafkaRestAdminClient())
