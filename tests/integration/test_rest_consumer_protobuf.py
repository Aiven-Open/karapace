"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from tests.utils import (
    new_consumer,
    new_topic,
    repeat_until_successful_request,
    REST_HEADERS,
    schema_data,
    schema_data_second,
)

import pytest


@pytest.mark.parametrize("schema_type", ["protobuf"])
@pytest.mark.parametrize("trail", ["", "/"])
async def test_publish_consume_protobuf(rest_async_client, admin_client, trail, schema_type):
    header = REST_HEADERS[schema_type]
    group_name = "e2e_protobuf_group"
    instance_id = await new_consumer(rest_async_client, group_name, fmt=schema_type, trail=trail)
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=1000"
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


@pytest.mark.parametrize("schema_type", ["protobuf"])
@pytest.mark.parametrize("trail", ["", "/"])
async def test_publish_consume_protobuf_second(rest_async_client, admin_client, trail, schema_type):
    header = REST_HEADERS[schema_type]
    group_name = "e2e_proto_second"
    instance_id = await new_consumer(rest_async_client, group_name, fmt=schema_type, trail=trail)
    assign_path = f"/consumers/{group_name}/instances/{instance_id}/assignments{trail}"
    consume_path = f"/consumers/{group_name}/instances/{instance_id}/records{trail}?timeout=1000"
    tn = new_topic(admin_client)
    assign_payload = {"partitions": [{"topic": tn, "partition": 0}]}
    res = await rest_async_client.post(assign_path, json=assign_payload, headers=header)
    assert res.ok
    publish_payload = schema_data_second[schema_type][1]
    await repeat_until_successful_request(
        rest_async_client.post,
        f"topics/{tn}{trail}",
        json_data={"value_schema": schema_data_second[schema_type][0], "records": [{"value": o} for o in publish_payload]},
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
