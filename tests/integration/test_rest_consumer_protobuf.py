"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.kafka_rest_apis import KafkaRestAdminClient
from karapace.protobuf.kotlin_wrapper import trim_margin
from tests.integration.test_rest import NEW_TOPIC_TIMEOUT
from tests.utils import (
    create_subject_name_factory,
    new_consumer,
    new_random_name,
    new_topic,
    repeat_until_successful_request,
    REST_HEADERS,
    schema_data,
    schema_data_second,
    wait_for_topics,
)
from typing import Generator

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


async def test_publish_protobuf_with_references(
    rest_async_client: Client,
    admin_client: KafkaRestAdminClient,
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
        headers=REST_HEADERS["protobuf"],
    )
    assert res.status_code == 200


async def test_publish_and_consume_protobuf_with_recursive_references(
    rest_async_client: Client,
    admin_client: KafkaRestAdminClient,
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
        headers=REST_HEADERS["protobuf"],
    )
    assert res.status_code == 200

    group = new_random_name("protobuf_recursive_reference_message")
    instance_id = await new_consumer(rest_async_client, group)

    subscribe_path = f"/consumers/{group}/instances/{instance_id}/subscription"

    consume_path = f"/consumers/{group}/instances/{instance_id}/records?timeout=1000"

    res = await rest_async_client.post(subscribe_path, json={"topics": [topic_name]}, headers=REST_HEADERS["protobuf"])
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


@pytest.mark.parametrize("google_library_included", [True, False])
async def test_produce_and_retrieve_protobuf(
    registry_async_client: Client,
    rest_async_client: Client,
    admin_client: KafkaRestAdminClient,
    google_library_included: bool,
) -> None:
    topic_name = new_topic(admin_client)
    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    subject = create_subject_name_factory("test_produce_and_retrieve_protobuf")()
    subject_topic = f"{topic_name}-value"

    base_schema_subject = f"{subject}_base_schema_subject"
    google_postal_address_schema_subject = f"{subject}_google_address_schema_subject"

    CUSTOMER_PLACE_PROTO = """
    syntax = "proto3";
    package a1;
    message Place {
            string city = 1;
            int32 zone = 2;
    }
    """

    body = {"schemaType": "PROTOBUF", "schema": CUSTOMER_PLACE_PROTO}
    res = await registry_async_client.post(f"subjects/{base_schema_subject}/versions", json=body)
    assert res.status_code == 200

    if not google_library_included:
        GOOGLE_POSTAL_ADDRESS_PROTO = """
        syntax = "proto3";

        package google.type;

        option cc_enable_arenas = true;
        option go_package = "google.golang.org/genproto/googleapis/type/postaladdress;postaladdress";
        option java_multiple_files = true;
        option java_outer_classname = "PostalAddressProto";
        option java_package = "com.google.type";
        option objc_class_prefix = "GTP";
        message PostalAddress {
          int32 revision = 1;
          string region_code = 2;
          string language_code = 3;
          string postal_code = 4;
          string sorting_code = 5;
          string administrative_area = 6;
          string locality = 7;
          string sublocality = 8;
          repeated string address_lines = 9;
          repeated string recipients = 10;
          string organization = 11;
        }
        """

        body = {"schemaType": "PROTOBUF", "schema": GOOGLE_POSTAL_ADDRESS_PROTO}
        res = await registry_async_client.post(f"subjects/{google_postal_address_schema_subject}/versions", json=body)
        assert res.status_code == 200

    postal_address_import = (
        'import "google/type/postal_address.proto";' if google_library_included else 'import "postal_address.proto";'
    )

    CUSTOMER_PROTO = f"""
    syntax = "proto3";
    package a1;
    import "Place.proto";

    {postal_address_import}

    // @producer: another comment
    message Customer {{
            string name = 1;
            int32 code = 2;
            Place place = 3;
            google.type.PostalAddress address = 4;
    }}
    """

    def references() -> Generator[str, None, None]:
        yield {"name": "Place.proto", "subject": base_schema_subject, "version": 1}

        if not google_library_included:
            yield {"name": "postal_address.proto", "subject": google_postal_address_schema_subject, "version": 1}

    body = {
        "schemaType": "PROTOBUF",
        "schema": CUSTOMER_PROTO,
        "references": list(references()),
    }
    res = await registry_async_client.post(f"subjects/{subject_topic}/versions", json=body)

    assert res.status_code == 200
    topic_schema_id = res.json()["id"]

    message_to_produce = [
        {
            "name": "John Doe",
            "code": 123456,
            "place": {"city": "New York", "zone": 5},
            "address": {
                "revision": 1,
                "region_code": "US",
                "postal_code": "10001",
                "address_lines": ["123 Main St", "Apt 4"],
            },
        },
        {
            "name": "Sophie Smith",
            "code": 987654,
            "place": {"city": "London", "zone": 3},
            "address": {
                "revision": 2,
                "region_code": "UK",
                "postal_code": "SW1A 1AA",
                "address_lines": ["10 Downing Street"],
            },
        },
        {
            "name": "Pierre Dupont",
            "code": 246813,
            "place": {"city": "Paris", "zone": 1},
            "address": {"revision": 1, "region_code": "FR", "postal_code": "75001", "address_lines": ["1 Rue de Rivoli"]},
        },
    ]

    res = await rest_async_client.post(
        f"/topics/{topic_name}",
        json={"value_schema_id": topic_schema_id, "records": [{"value": m} for m in message_to_produce]},
        headers=REST_HEADERS["protobuf"],
    )
    assert res.status_code == 200

    group = new_random_name("protobuf_recursive_reference_message")
    instance_id = await new_consumer(rest_async_client, group)

    subscribe_path = f"/consumers/{group}/instances/{instance_id}/subscription"

    consume_path = f"/consumers/{group}/instances/{instance_id}/records?timeout=1000"

    res = await rest_async_client.post(subscribe_path, json={"topics": [topic_name]}, headers=REST_HEADERS["protobuf"])
    assert res.ok

    resp = await rest_async_client.get(consume_path, headers=REST_HEADERS["avro"])
    data = resp.json()

    assert isinstance(data, list)
    assert len(data) == 3

    for i in range(0, 3):
        msg = data[i]
        expected_message = message_to_produce[i]

        assert "key" in msg
        assert "offset" in msg
        assert "topic" in msg
        assert "value" in msg
        assert "timestamp" in msg

        assert msg["key"] is None, "no key defined in production"
        assert msg["topic"] == topic_name

        for key in expected_message.keys():
            if key == "address":
                for address_key in expected_message["address"].keys():
                    assert expected_message["address"][address_key] == msg["value"]["address"][address_key]
            else:
                assert msg["value"][key] == expected_message[key]
