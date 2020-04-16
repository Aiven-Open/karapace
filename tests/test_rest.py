from .utils import Client
from kafka.admin import NewTopic
from kafka.cluster import TopicPartition
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from pytest import raises

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


def test_admin_client(admin_client, producer):
    topic_names = ["topic_%d" % i for i in range(3)]
    topics = [NewTopic(name=tn, num_partitions=1, replication_factor=1) for tn in topic_names]
    try:
        admin_client.create_topics(topics)
    except TopicAlreadyExistsError:
        pass
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
    one_topic_info = admin_client.cluster_metadata(["topic_0"])
    retrieved_names = list(one_topic_info["topics"].keys())
    assert len(retrieved_names) == 1
    assert retrieved_names[0] == "topic_0", "Returned value %r differs from expected topic_0" % retrieved_names[0]
    cfg = admin_client.get_topic_config("topic_0")
    assert "cleanup.policy" in cfg
    for _ in range(5):
        fut = producer.send("topic_0", value=b"foo_val")
        producer.flush()
        _ = fut.get()
    offsets = admin_client.get_offsets("topic_0", 0)
    assert offsets["beginning_offset"] == 0, "Start offset should be 0 for topic_0, partition 0"
    assert offsets["end_offset"] == 5, "End offset should be 0 for topic_0, partition 0"
    # invalid requests
    with raises(UnknownTopicOrPartitionError):
        admin_client.get_offsets("invalid_topic", 0)
    with raises(UnknownTopicOrPartitionError):
        admin_client.get_offsets("topic_0", 10)
    with raises(UnknownTopicOrPartitionError):
        admin_client.get_topic_config("another_invalid_name")
    with raises(UnknownTopicOrPartitionError):
        admin_client.cluster_metadata(topics=["another_invalid_name"])


async def test_local(kafka_rest, aiohttp_client, producer, admin_client):
    kc, _ = kafka_rest()
    client = await aiohttp_client(kc.app)
    c = Client(client=client)
    topic_names = ["topic_%d" % i for i in range(3)]
    topics = [NewTopic(name=tn, num_partitions=1, replication_factor=1) for tn in topic_names]
    try:
        admin_client.create_topics(topics)
    except TopicAlreadyExistsError:
        pass
    await check_brokers(c)
    await check_malformed_requests(c)
    await check_topics(c, topic_names)
    await check_partitions(c, "topic_1", producer)


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
        expected_part = TopicPartition("topic_2", 0)
        actual_part = result["partition"]
        assert actual_part == expected_part, "Missing partition publish should go to %r but went to %r" % \
                                             (expected_part, actual_part)
    result = kc.produce_message(topic="topic_2", key=b"key", value=b"value", partition=100)
    assert "error" in result, "Invalid result missing 'error' key"
    assert result["error"] == "Unrecognized partition"
    assert "error_code" in result, "Invalid result missing 'error_code' key"
    assert result["error_code"] == 1
    assert kc.has_non_empty({"records": [{"key": {"foo": "bar"}}]}, "key") is True
    assert kc.has_non_empty({"records": [{"value": {"foo": "bar"}}]}, "value") is True


async def check_topics(c, topic_names):
    res = await c.get("/topics")
    assert res.status_code == 200, "Status code is not 200: %r" % res.status_code
    data = res.json()
    assert set(topic_names).difference(set(data)) == set(), "Retrieved topic names do not match: %r" % data
    res = await c.get("/topics/topic_0")
    assert res.status_code == 200, "Status code is not 200: %r" % res.status_code
    data = res.json()
    assert data["name"] == "topic_0", "Topic name should be topic_0 and is %r" % data["name"]
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
    # Proper Json / binary
    res = await c.post("/topics/topic_0", json={"records": [{"value": {"foo": "bar"}}]}, headers=HEADERS["json"])
    json = res.json()
    assert res.status == 200
    assert "offsets" in json


async def check_malformed_requests(c):
    # Malformed schema
    res = await c.post("/topics/topic_0", json={"foo": "bar"}, headers=HEADERS["json"])
    json = res.json()
    assert res.status == 500
    assert json["message"] == "Invalid request format"
    res = await c.post("/topics/topic_0", json={"records": [{"value": {"foo": "bar"}}]}, headers=HEADERS["avro"])
    json = res.json()
    assert res.status == 422
    assert json["error_code"] == 42202
    res = await c.post("/topics/topic_0", json={"records": [{"key": {"foo": "bar"}}]}, headers=HEADERS["avro"])
    json = res.json()
    assert res.status == 422
    assert json["error_code"] == 42201
    res = await c.post("/topics/topic_0", json={"records": [{"value": "not base64"}]}, headers=HEADERS["base64"])
    json = res.json()
    assert res.status == 422
    assert json["error_code"] == 42205
    res = await c.post("/topics/topic_0", json={"records": [{"value": "not json"}]}, headers=HEADERS["json"])
    json = res.json()
    assert res.status == 422
    assert json["error_code"] == 42205


async def check_brokers(c):
    res = await c.get("/brokers")
    assert res.status_code == 200
    assert len(res.json()) == 1, "Only one broker should be running"


async def check_partitions(c, topic_name, producer):
    res = await c.get("/topics/{}/partitions".format(topic_name))
    assert res.status_code == 200, "Topic should exist"
    partitions = res.json()
    assert len(partitions) == 1, "Only one partition should exist"
    assert len(partitions[0]["replicas"]) == 1, "Only one replica should exist"
    partition = partitions[0]
    assert partition["replicas"][0]["leader"], "Replica should be leader"
    assert partition["replicas"][0]["in_sync"], "Replica should be in sync"
    res = await c.get("/topics/fooo/partitions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    res = await c.get("/topics/{}/partitions/0".format(topic_name))
    assert res.status_code == 200
    data = res.json()
    assert data == partition, "Unexpected partition data: %r" % data
    res = await c.get("/topics/fooo/partitions/0")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    res = await c.get("/topics/{}/partitions/10".format(topic_name))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    for _ in range(5):
        producer.send(topic_name, value=b"foo_val").get()
    offset_res = await c.get("/topics/{}/partitions/0/offsets".format(topic_name))
    assert offset_res.status_code == 200, "Status code %r is not expected: %r" % (offset_res.status_code, offset_res.json())
    data = offset_res.json()
    assert data == {"beginning_offset": 0, "end_offset": 5}, "Unexpected offsets for topic %r: %r" % (topic_name, data)
    res = await c.get("/topics/fooo/partitions/0/offsets")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    res = await c.get("/topics/{}/partitions/foo/offsets".format(topic_name))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
