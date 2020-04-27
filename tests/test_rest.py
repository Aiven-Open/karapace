from kafka.errors import UnknownTopicOrPartitionError
from pytest import raises
from tests.utils import client_for, new_topic, REST_HEADERS, schema_json, second_obj, second_schema_json, test_objects

pytest_plugins = "aiohttp.pytest_plugin"


async def check_successful_publish_response(success_response, objects, partition_id=None):
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


async def test_content_types(kafka_rest, aiohttp_client, admin_client, karapace):
    karapace, _ = karapace()
    kafka_rest, _ = kafka_rest()
    rest_client = await client_for(kafka_rest, aiohttp_client)
    tn = new_topic(admin_client)
    registry_client = await client_for(karapace, aiohttp_client)
    kafka_rest.serializer.registry_client.client = registry_client
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

    avro_payload = {"value_schema": schema_json, "records": [{"value": o} for o in test_objects]}
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
        res = await rest_client.post(f"topics/{tn}", pl, headers={"Content-Type": hv})
        assert res.ok
    for hv, pl in zip(invalid_headers, valid_payloads):
        res = await rest_client.post(f"topics/{tn}", pl, headers={"Content-Type": hv})
        assert not res.ok
        # get requests should succeed with bogus content type headers?
        res = await rest_client.get("/brokers", headers={"Content-Type": hv})
        assert res.ok

    for ah in valid_accept_headers:
        res = await rest_client.get("/brokers", headers={"Accept": ah})
        assert res.ok

    for ah in invalid_accept_headers:
        res = await rest_client.get("/brokers", headers={"Accept": ah})
        assert not res.ok

    for c in [registry_client, rest_client]:
        await c.close()


async def test_avro_publish(kafka_rest, aiohttp_client, admin_client, karapace):
    # pylint: disable=W0612
    karapace, _ = karapace()
    kafka_rest, _ = kafka_rest()
    rest_client = await client_for(kafka_rest, aiohttp_client)
    registry_client = await client_for(karapace, aiohttp_client)
    kafka_rest.serializer.registry_client.client = registry_client
    tn = new_topic(admin_client)
    other_tn = new_topic(admin_client)
    header = REST_HEADERS["avro"]
    # check succeeds with 1 record and brand new schema
    res = await registry_client.post(f"subjects/{other_tn}/versions", json={"schema": second_schema_json})
    assert res.ok
    new_schema_id = res.json()["id"]
    urls = [f"/topics/{tn}", f"/topics/{tn}/partitions/0"]
    for url in urls:
        partition_id = 0 if "partition" in url else None
        for pl_type in ["key", "value"]:
            correct_payload = {f"{pl_type}_schema": schema_json, "records": [{pl_type: o} for o in test_objects]}
            res = await rest_client.post(url, correct_payload, headers=header)
            await check_successful_publish_response(res, test_objects, partition_id)
            # check succeeds with prepublished schema
            res = await rest_client.post(
                f"/topics/{tn}", {
                    f"{pl_type}_schema_id": new_schema_id,
                    "records": [{
                        pl_type: o
                    } for o in second_obj]
                },
                headers=header
            )
            await check_successful_publish_response(res, second_obj, partition_id)
            # unknown schema id
            res = await rest_client.post(
                url, {
                    f"{pl_type}_schema_id": 666,
                    "records": [{
                        pl_type: o
                    } for o in second_obj]
                }, headers=header
            )
            assert res.status == 422
            # mismatched schema
            res = await rest_client.post(
                url, {
                    f"{pl_type}_schema_id": new_schema_id,
                    "records": [{
                        pl_type: o
                    } for o in test_objects]
                },
                headers=header
            )
            assert res.status == 422

    await rest_client.close()
    await registry_client.close()


def test_admin_client(admin_client, producer):
    topic_names = [new_topic(admin_client) for i in range(10, 13)]
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
    c = await client_for(kc, aiohttp_client)
    for_check = [new_topic(admin_client)]
    for_publish = new_topic(admin_client)
    for_partitions = new_topic(admin_client)
    for_requests = new_topic(admin_client)
    await check_brokers(c)
    await check_malformed_requests(c, for_requests)
    await check_topics(c, for_check)
    await check_partitions(c, for_partitions, producer)
    await check_publish(c, for_publish)


async def test_internal(kafka_rest, admin_client):
    topic_name = new_topic(admin_client)
    kc, _ = kafka_rest()
    for p in [0, None]:
        result = kc.produce_message(topic=topic_name, key=b"key", value=b"value", partition=p)
        assert "error" not in result, "Valid result should not contain 'error' key"
        assert "offset" in result, "Valid result is missing 'offset' key"
        assert "partition" in result, "Valid result is missing 'partition' key"
        actual_part = result["partition"]
        assert actual_part == 0, "Returned partition id should be %d but is %d" % (0, actual_part)
    result = kc.produce_message(topic=topic_name, key=b"key", value=b"value", partition=100)
    assert "error" in result, "Invalid result missing 'error' key"
    assert result["error"] == "Unrecognized partition"
    assert "error_code" in result, "Invalid result missing 'error_code' key"
    assert result["error_code"] == 1
    assert kc.all_empty({"records": [{"key": {"foo": "bar"}}]}, "key") is False
    assert kc.all_empty({"records": [{"value": {"foo": "bar"}}]}, "value") is False
    assert kc.all_empty({"records": [{"value": {"foo": "bar"}}]}, "key") is True


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
        for payload, h in [({"value": {"foo": "bar"}}, "json"), ({"value": "Zm9vCg=="}, "binary")]:
            res = await c.post(url, json={"records": [payload]}, headers=REST_HEADERS[h])
            res_json = res.json()
            assert res.ok
            assert "offsets" in res_json
            if "partition" in url:
                for o in res_json["offsets"]:
                    assert "partition" in o
                    assert o["partition"] == 0


async def check_malformed_requests(c, topic_name):
    for url in [f"/topics/{topic_name}", f"/topics/{topic_name}/partitions/0"]:
        # Malformed schema
        for js in [{"foo": "bar"}, {"records": [{"valur": {"foo": "bar"}}]}, {"records": []}]:
            res = await c.post(url, json=js, headers=REST_HEADERS["json"])
            res_json = res.json()
            assert res.status == 500
            assert res_json["message"] == "Invalid request format"
        res = await c.post(url, json={"records": [{"value": {"foo": "bar"}}]}, headers=REST_HEADERS["avro"])
        res_json = res.json()
        assert res.status == 422
        assert res_json["error_code"] == 42202
        res = await c.post(url, json={"records": [{"key": {"foo": "bar"}}]}, headers=REST_HEADERS["avro"])
        res_json = res.json()
        assert res.status == 422
        assert res_json["error_code"] == 42201
        res = await c.post(url, json={"records": [{"value": "not base64"}]}, headers=REST_HEADERS["binary"])
        res_json = res.json()
        assert res.status == 422
        assert res_json["error_code"] == 42205
        res = await c.post(url, json={"records": [{"value": "not json"}]}, headers=REST_HEADERS["json"])
        res_json = res.json()
        assert res.status == 422
        assert res_json["error_code"] == 42205


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
