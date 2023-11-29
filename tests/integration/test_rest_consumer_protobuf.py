"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.client import Client
from karapace.kafka_admin import KafkaAdminClient
from karapace.protobuf.kotlin_wrapper import trim_margin
from tests.integration.test_rest import NEW_TOPIC_TIMEOUT
from tests.utils import (
    new_consumer,
    new_random_name,
    new_topic,
    repeat_until_successful_request,
    REST_HEADERS,
    schema_data,
    schema_data_second,
    wait_for_topics,
)

import pytest


@pytest.mark.parametrize("schema_type", ["protobuf"])
@pytest.mark.parametrize("trail", ["", "/"])
async def test_publish_consume_protobuf(
    rest_async_client: Client,
    admin_client: KafkaAdminClient,
    trail: str,
    schema_type: str,
):
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
async def test_publish_consume_protobuf_second(
    rest_async_client: Client,
    admin_client: KafkaAdminClient,
    trail: str,
    schema_type: str,
):
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


async def test_publish_protobuf_with_references(
    rest_async_client: Client,
    admin_client: KafkaAdminClient,
    registry_async_client: Client,
):
    topic_name = new_topic(admin_client)
    subject_reference = "reference"
    subject_topic = f"{topic_name}-value"

    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)

    reference_schema = trim_margin(
        """
        |syntax = "proto3";
        |message Reference {
        |     string name = 1;
        |}
        |"""
    )

    topic_schema = trim_margin(
        """
        |syntax = "proto3";
        |import "Reference.proto";
        |message Example {
        |     Reference example = 1;
        |}
        |"""
    )

    res = await registry_async_client.post(
        f"subjects/{subject_reference}/versions", json={"schemaType": "PROTOBUF", "schema": reference_schema}
    )
    assert "id" in res.json()

    res = await registry_async_client.post(
        f"subjects/{subject_topic}/versions",
        json={
            "schemaType": "PROTOBUF",
            "schema": topic_schema,
            "references": [
                {
                    "name": "Reference.proto",
                    "subject": subject_reference,
                    "version": 1,
                }
            ],
        },
    )
    topic_schema_id = res.json()["id"]

    example_message = {"value_schema_id": topic_schema_id, "records": [{"value": {"example": {"name": "myname"}}}]}

    res = await rest_async_client.post(
        f"/topics/{topic_name}",
        json=example_message,
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 200


async def test_publish_and_consume_protobuf_with_recursive_references(
    rest_async_client: Client,
    admin_client: KafkaAdminClient,
    registry_async_client: Client,
):
    topic_name = new_topic(admin_client)
    subject_meta_reference = "meta-reference"
    subject_inner_reference = "inner-reference"
    subject_topic = f"{topic_name}-value"

    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)

    meta_reference = trim_margin(
        """
        |syntax = "proto3";
        |message MetaReference {
        |     string name = 1;
        |}
        |"""
    )

    inner_reference = trim_margin(
        """
        |syntax = "proto3";
        |import "MetaReference.proto";
        |message InnerReference {
        |     MetaReference reference = 1;
        }
        |"""
    )

    topic_schema = trim_margin(
        """
        |syntax = "proto3";
        |import "InnerReference.proto";
        |message Example {
        |     InnerReference example = 1;
        |}
        |"""
    )

    res = await registry_async_client.post(
        f"subjects/{subject_meta_reference}/versions", json={"schemaType": "PROTOBUF", "schema": meta_reference}
    )
    assert "id" in res.json()
    res = await registry_async_client.post(
        f"subjects/{subject_inner_reference}/versions",
        json={
            "schemaType": "PROTOBUF",
            "schema": inner_reference,
            "references": [
                {
                    "name": "MetaReference.proto",
                    "subject": subject_meta_reference,
                    "version": 1,
                }
            ],
        },
    )
    assert "id" in res.json()

    res = await registry_async_client.post(
        f"subjects/{subject_topic}/versions",
        json={
            "schemaType": "PROTOBUF",
            "schema": topic_schema,
            "references": [
                {
                    "name": "InnerReference.proto",
                    "subject": subject_inner_reference,
                    "version": 1,
                }
            ],
        },
    )
    topic_schema_id = res.json()["id"]

    produced_message = {"example": {"reference": {"name": "myname"}}}
    example_message = {
        "value_schema_id": topic_schema_id,
        "records": [{"value": produced_message}],
    }

    res = await rest_async_client.post(
        f"/topics/{topic_name}",
        json=example_message,
        headers=REST_HEADERS["avro"],
    )
    assert res.status_code == 200

    group = new_random_name("protobuf_recursive_reference_message")
    instance_id = await new_consumer(rest_async_client, group)

    subscribe_path = f"/consumers/{group}/instances/{instance_id}/subscription"

    consume_path = f"/consumers/{group}/instances/{instance_id}/records?timeout=1000"

    res = await rest_async_client.post(subscribe_path, json={"topics": [topic_name]}, headers=REST_HEADERS["binary"])
    assert res.ok

    resp = await rest_async_client.get(consume_path, headers=REST_HEADERS["avro"])
    data = resp.json()

    assert isinstance(data, list)
    assert len(data) == 1

    msg = data[0]

    assert "key" in msg
    assert "offset" in msg
    assert "topic" in msg
    assert "value" in msg
    assert "timestamp" in msg

    assert msg["key"] is None, "no key defined in production"
    assert msg["offset"] == 0 and msg["partition"] == 0, "first message of the only partition available"
    assert msg["topic"] == topic_name
    assert msg["value"] == produced_message
