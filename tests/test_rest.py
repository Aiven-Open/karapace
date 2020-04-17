from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from karapace.utils import Client
from pytest import raises
from tests.utils import schema_json, test_objects
import json

pytest_plugins = "aiohttp.pytest_plugin"
HEADERS = {
    "json": {
        "Content-Type": "application/vnd.kafka.json.v2+json"
    },
    "base64": {
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


def create_topic(admin_client, index):
    tn = f"topic_{index}"
    try:
        admin_client.new_topic(tn)
    except TopicAlreadyExistsError:
        pass
    return tn


async def test_avro_publish(kafka_rest, aiohttp_client, admin_client, karapace):
    # pylint: disable=W0612
    karapace, _ = karapace()
    kafka_rest, _ = kafka_rest()
    rest_client = await client_for(kafka_rest, aiohttp_client)
    registry_client = await client_for(karapace, aiohttp_client)
    tn = create_topic(admin_client, 5)
    header = HEADERS["avro"]
    # check succeeds with 1 record
    correct_payload = {
        "value_schema": json.dumps(schema_json),
        "records": [{"value": o} for o in test_objects[:1]]
    }
    kafka_rest.serializer.registry_client.client = registry_client
    topic_url = f"/topics/{tn}"
    res = await rest_client.post(topic_url, correct_payload, headers=header)
    assert res.ok
    success_response = res.json()
    assert "value_schema_id" in success_response
    await rest_client.close()
    await registry_client.close()


def test_admin_client(admin_client, producer):
    topic_names = [create_topic(admin_client, i) for i in range(10, 13)]
    topic_info = admin_client.cluster_metadata()
    retrieved_names = list(topic_info["topics"].keys())
    assert set(topic_names).difference(set(retrieved_names)) == set(), \
        "Returned value %r differs from written one %r" % (retrieved_names, topic_names)
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


async def test_local(kafka_rest, aiohttp_client, producer, admin_client):
    kc, _ = kafka_rest()
    client = await aiohttp_client(kc.app)
    c = Client(client=client)
    for_check = [create_topic(admin_client, 1)]
    for_publish = create_topic(admin_client, 2)
    for_partitions = create_topic(admin_client, 3)
    await check_brokers(c)
    await check_malformed_requests(c, create_topic(admin_client, 4))
    await check_topics(c, for_check)
    await check_partitions(c, for_partitions, producer)
    await check_publish(c, for_publish)


async def test_internal(kafka_rest, admin_client):
    try:
        admin_client.create_topics([NewTopic(name="topic_2", num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    kc, _ = kafka_rest()
    for p in [0, None]:
        result = kc.produce_message(topic="topic_2", key=b"key", value=b"value", partition=p)
        assert "error" not in result, "Valid result should not contain 'error' key"
        assert "offset" in result, "Valid result is missing 'offset' key"
        assert "partition" in result, "Valid result is missing 'partition' key"
        actual_part = result["partition"]
        assert actual_part == 0, "Returned partition id should be %d but is %d" % (0, actual_part)
    result = kc.produce_message(topic="topic_2", key=b"key", value=b"value", partition=100)
    assert "error" in result, "Invalid result missing 'error' key"
    assert result["error"] == "Unrecognized partition"
    assert "error_code" in result, "Invalid result missing 'error_code' key"
    assert result["error_code"] == 1
    assert kc.all_empty({"records": [{"key": {"foo": "bar"}}]}, "key") is True
    assert kc.all_empty({"records": [{"value": {"foo": "bar"}}]}, "value") is True


async def check_topics(c, topic_names):
    res = await c.get("/topics")
    assert res.ok, "Status code is not 200: %r" % res.status_code
    data = res.json()
    assert set(topic_names).difference(set(data)) == set(), "Retrieved topic names do not match: %r" % data
    res = await c.get(f"/topics/{topic_names[0]}")
    assert res.ok, "Status code is not 200: %r" % res.status_code
    data = res.json()
    assert data["name"] == topic_names[0], f"Topic name should be {topic_names[0]} and is {data['name']}"
    assert "configs" in data, "'configs' key is missing : %r" % data
    assert data["configs"] != {}, "'configs' key should not be empty"
    assert "partitions" in data, "'partitions' key is missing"
    assert len(data["partitions"]) == 1, "should only have one partition"
    assert "replicas" in data["partitions"][0], "'replicas' key is missing"
    assert len(data["partitions"][0]["replicas"]) == 1, "should only have one replica"
    assert data["partitions"][0]["replicas"][0]["leader"], "Replica should be leader"
    assert data["partitions"][0]["replicas"][0]["in_sync"], "Replica should be in sync"
    res = await c.get("/topics/foo")
    assert res.status_code == 404, "Topic should not exist"
    assert res.json()["error_code"] == 40401, "Error code does not match"
    # publish


async def check_publish(c, topic):
    topic_url = f"/topics/{topic}"
    partition_url = f"/topics/{topic}/partitions/0"
    # Proper Json / Binary
    for url in [topic_url, partition_url]:
        for payload, h in [({"value": {"foo": "bar"}}, "json"), ({"value": "Zm9vCg=="}, "base64")]:
            res = await c.post(url, json={"records": [payload]}, headers=HEADERS[h])
            json = res.json()
            assert res.ok
            assert "offsets" in json
            if "partition" in url:
                for o in json["offsets"]:
                    assert "partition" in o
                    assert o["partition"] == 0


async def check_malformed_requests(c, topic_name):
    for url in [f"/topics/{topic_name}", f"/topics/{topic_name}/partitions/0"]:
        # Malformed schema
        res = await c.post(url, json={"foo": "bar"}, headers=HEADERS["json"])
        json = res.json()
        assert res.status == 500
        assert json["message"] == "Invalid request format"
        res = await c.post(url, json={"records": [{"value": {"foo": "bar"}}]}, headers=HEADERS["avro"])
        json = res.json()
        assert res.status == 422
        assert json["error_code"] == 42202
        res = await c.post(url, json={"records": [{"key": {"foo": "bar"}}]}, headers=HEADERS["avro"])
        json = res.json()
        assert res.status == 422
        assert json["error_code"] == 42201
        res = await c.post(url, json={"records": [{"value": "not base64"}]}, headers=HEADERS["base64"])
        json = res.json()
        assert res.status == 422
        assert json["error_code"] == 42205
        res = await c.post(url, json={"records": [{"value": "not json"}]}, headers=HEADERS["json"])
        json = res.json()
        assert res.status == 422
        assert json["error_code"] == 42205


async def check_brokers(c):
    res = await c.get("/brokers")
    assert res.ok
    assert len(res.json()) == 1, "Only one broker should be running"


async def check_partitions(c, topic_name, producer):
    all_partitions_res = await c.get(f"/topics/{topic_name}/partitions")
    assert all_partitions_res.ok, "Topic should exist"
    partitions = all_partitions_res.json()
    assert len(partitions) == 1, "Only one partition should exist"
    assert len(partitions[0]["replicas"]) == 1, "Only one replica should exist"
    partition = partitions[0]
    assert partition["replicas"][0]["leader"], "Replica should be leader"
    assert partition["replicas"][0]["in_sync"], "Replica should be in sync"
    first_partition_res = await c.get(f"/topics/{topic_name}/partitions/0")
    assert first_partition_res.ok
    partition_data = first_partition_res.json()
    assert partition_data == partition, f"Unexpected partition data: {partition_data}"

    res = await c.get("/topics/fooo/partitions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    res = await c.get("/topics/fooo/partitions/0")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401

    res = await c.get(f"/topics/{topic_name}/partitions/10")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    for _ in range(5):
        producer.send(topic_name, value=b"foo_val").get()
    offset_res = await c.get(f"/topics/{topic_name}/partitions/0/offsets")
    assert offset_res.ok, "Status code %r is not expected: %r" % (offset_res.status_code, offset_res.json())
    data = offset_res.json()
    assert data == {"beginning_offset": 0, "end_offset": 5}, "Unexpected offsets for topic %r: %r" % (topic_name, data)
    res = await c.get("/topics/fooo/partitions/0/offsets")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    res = await c.get(f"/topics/{topic_name}/partitions/foo/offsets")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
