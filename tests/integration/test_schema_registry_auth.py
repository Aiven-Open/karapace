"""
karapace - schema registry authentication and authorization tests

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import asyncio
from urllib.parse import quote

import aiohttp

from karapace.core.kafka.admin import KafkaAdminClient
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema
from tests.integration.utils.rest_client import RetryRestClient
from tests.utils import (
    new_random_name,
    new_topic,
    schema_avro_json,
    schema_jsonschema_json,
    test_objects_avro,
    wait_for_topics,
)

NEW_TOPIC_TIMEOUT = 10

admin = aiohttp.BasicAuth("admin", "admin")
aladdin = aiohttp.BasicAuth("aladdin", "opensesame")
reader = aiohttp.BasicAuth("reader", "secret")


async def test_sr_auth(registry_async_retry_client_auth: RetryRestClient) -> None:
    subject = new_random_name("cave-")

    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(subject)}/versions", json={"schema": schema_avro_json}, expected_response_code=401
    )
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(subject)}/versions", json={"schema": schema_avro_json}, auth=aladdin
    )
    assert res.status_code == 200
    sc_id = res.json()["id"]
    assert sc_id >= 0

    res = await registry_async_retry_client_auth.get(
        f"subjects/{quote(subject)}/versions/latest", expected_response_code=401
    )
    assert res.status_code == 401
    res = await registry_async_retry_client_auth.get(f"subjects/{quote(subject)}/versions/latest", auth=aladdin)
    assert res.status_code == 200
    assert sc_id == res.json()["id"]
    assert ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json) == ValidatedTypedSchema.parse(
        SchemaType.AVRO, res.json()["schema"]
    )


async def test_sr_auth_endpoints(registry_async_retry_client_auth: RetryRestClient) -> None:
    """Test endpoints for authorization"""

    subject = new_random_name("any-")

    res = await registry_async_retry_client_auth.post(
        f"compatibility/subjects/{quote(subject)}/versions/1",
        json={"schema": schema_avro_json},
        expected_response_code=401,
    )
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get(f"config/{quote(subject)}", expected_response_code=401)
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.put(
        f"config/{quote(subject)}",
        json={"compatibility": "NONE"},
        expected_response_code=401,
    )
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get("config", expected_response_code=401)
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.put("config", json={"compatibility": "NONE"}, expected_response_code=401)
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get("schemas/ids/1/versions", expected_response_code=401)
    assert res.status_code == 401

    # This is an exception that does not require authorization
    res = await registry_async_retry_client_auth.get("schemas/types")
    assert res.status_code == 200

    # but let's verify it answers normally if sending authorization header
    res = await registry_async_retry_client_auth.get("schemas/types", auth=admin)
    assert res.status_code == 200

    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(subject)}/versions", json={"schema": schema_avro_json}, expected_response_code=401
    )
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.delete(f"subjects/{quote(subject)}/versions/1", expected_response_code=401)
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get(
        f"subjects/{quote(subject)}/versions/1/schema", expected_response_code=401
    )
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get(
        f"subjects/{quote(subject)}/versions/1/referencedby", expected_response_code=401
    )
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.delete(f"subjects/{quote(subject)}", expected_response_code=401)
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get("mode", expected_response_code=401)
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get(f"mode/{quote(subject)}", expected_response_code=401)
    assert res.status_code == 401


async def test_sr_list_subjects(registry_async_retry_client_auth: RetryRestClient) -> None:
    cavesubject = new_random_name("cave-")
    carpetsubject = new_random_name("carpet-")

    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(cavesubject)}/versions", json={"schema": schema_avro_json}, auth=aladdin
    )
    assert res.status_code == 200
    sc_id = res.json()["id"]
    assert sc_id >= 0

    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(carpetsubject)}/versions", json={"schema": schema_avro_json}, auth=admin
    )
    assert res.status_code == 200

    res = await registry_async_retry_client_auth.get("subjects", auth=admin)
    assert res.status_code == 200
    assert [cavesubject, carpetsubject] == res.json()

    res = await registry_async_retry_client_auth.get(f"subjects/{quote(carpetsubject)}/versions", expected_response_code=401)
    assert res.status_code == 401

    res = await registry_async_retry_client_auth.get(f"subjects/{quote(carpetsubject)}/versions", auth=admin)
    assert res.status_code == 200
    assert [sc_id] == res.json()

    res = await registry_async_retry_client_auth.get("subjects", auth=aladdin)
    assert res.status_code == 200
    assert [cavesubject] == res.json()

    # Subject-scoped authZ denials respond with the same 404 body as
    # genuinely-missing subjects so an authenticated caller cannot probe for
    # the existence of subjects they are not allowed to read.
    res = await registry_async_retry_client_auth.get(
        f"subjects/{quote(carpetsubject)}/versions", auth=aladdin, expected_response_code=404
    )
    assert res.status_code == 404
    assert res.json() == {
        "error_code": 40401,
        "message": f"Subject '{carpetsubject}' not found.",
    }

    res = await registry_async_retry_client_auth.get("subjects", auth=reader)
    assert res.status_code == 200
    assert [carpetsubject] == res.json()

    res = await registry_async_retry_client_auth.get(f"subjects/{quote(carpetsubject)}/versions", auth=reader)
    assert res.status_code == 200
    assert [1] == res.json()


async def test_sr_unauthorized_subject_indistinguishable_from_missing(
    registry_async_retry_client_auth: RetryRestClient,
) -> None:
    """An authenticated caller probing a forbidden subject must get the same
    404 response as for a non-existent subject — same status, same body.

    aladdin's ACL only covers 'Subject:cave-.*'. Hitting a 'carpet-' subject
    that exists (created by admin) and a 'carpet-' subject that does not
    exist must be indistinguishable to aladdin.
    """
    existing_forbidden = new_random_name("carpet-")
    nonexistent = new_random_name("carpet-")

    # admin creates the existing forbidden subject.
    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(existing_forbidden)}/versions", json={"schema": schema_avro_json}, auth=admin
    )
    assert res.status_code == 200

    # Iterate every subject-scoped endpoint hit by an unauthorized caller.
    # For each, the existing-but-forbidden response must match the
    # genuinely-missing response byte-for-byte (status, body, content-type).
    probes = [
        ("GET", "subjects/{s}/versions", None),
        ("GET", "subjects/{s}/versions/latest", None),
        ("GET", "subjects/{s}/versions/1/schema", None),
        ("GET", "subjects/{s}/versions/1/referencedby", None),
        ("POST", "subjects/{s}", {"schema": schema_avro_json}),
        ("POST", "subjects/{s}/versions", {"schema": schema_avro_json}),
        ("DELETE", "subjects/{s}", None),
        ("DELETE", "subjects/{s}/versions/1", None),
        ("GET", "config/{s}", None),
        ("PUT", "config/{s}", {"compatibility": "NONE"}),
        ("DELETE", "config/{s}", None),
        ("GET", "mode/{s}", None),
        ("POST", "compatibility/subjects/{s}/versions/latest", {"schema": schema_avro_json}),
    ]

    for method, path_tpl, body in probes:
        forbidden_path = path_tpl.format(s=quote(existing_forbidden))
        missing_path = path_tpl.format(s=quote(nonexistent))

        kwargs: dict = {"auth": aladdin, "expected_response_code": 404}
        if body is not None:
            kwargs["json"] = body

        if method == "GET":
            forbidden_res = await registry_async_retry_client_auth.get(forbidden_path, **kwargs)
            missing_res = await registry_async_retry_client_auth.get(missing_path, **kwargs)
        elif method == "POST":
            forbidden_res = await registry_async_retry_client_auth.post(forbidden_path, **kwargs)
            missing_res = await registry_async_retry_client_auth.post(missing_path, **kwargs)
        elif method == "PUT":
            forbidden_res = await registry_async_retry_client_auth.put(forbidden_path, **kwargs)
            missing_res = await registry_async_retry_client_auth.put(missing_path, **kwargs)
        else:  # DELETE
            forbidden_res = await registry_async_retry_client_auth.delete(forbidden_path, **kwargs)
            missing_res = await registry_async_retry_client_auth.delete(missing_path, **kwargs)

        assert forbidden_res.status_code == 404, f"{method} {path_tpl} forbidden: {forbidden_res.status_code}"
        assert missing_res.status_code == 404, f"{method} {path_tpl} missing: {missing_res.status_code}"

        # Bodies are not strictly identical (subject name differs), but the
        # error_code and message template must match — that is what removes
        # the existence signal.
        assert forbidden_res.json() == {
            "error_code": 40401,
            "message": f"Subject '{existing_forbidden}' not found.",
        }, f"{method} {path_tpl} forbidden body mismatch"
        assert missing_res.json() == {
            "error_code": 40401,
            "message": f"Subject '{nonexistent}' not found.",
        }, f"{method} {path_tpl} missing body mismatch"


async def test_sr_ids(registry_async_retry_client_auth: RetryRestClient) -> None:
    cavesubject = new_random_name("cave-")
    carpetsubject = new_random_name("carpet-")

    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(cavesubject)}/versions", json={"schema": schema_avro_json}, auth=aladdin
    )
    assert res.status_code == 200
    avro_sc_id = res.json()["id"]
    assert avro_sc_id >= 0

    res = await registry_async_retry_client_auth.post(
        f"subjects/{quote(carpetsubject)}/versions",
        json={"schemaType": "JSON", "schema": schema_jsonschema_json},
        auth=admin,
    )
    assert res.status_code == 200
    jsonschema_sc_id = res.json()["id"]
    assert jsonschema_sc_id >= 0

    res = await registry_async_retry_client_auth.get(f"schemas/ids/{avro_sc_id}", auth=aladdin)
    assert res.status_code == 200

    res = await registry_async_retry_client_auth.get(
        f"schemas/ids/{jsonschema_sc_id}", auth=aladdin, expected_response_code=404
    )
    assert res.status_code == 404
    assert {"error_code": 40403, "message": "Schema not found"} == res.json()

    res = await registry_async_retry_client_auth.get(f"schemas/ids/{avro_sc_id}", auth=reader, expected_response_code=404)
    assert res.status_code == 404
    assert {"error_code": 40403, "message": "Schema not found"} == res.json()

    res = await registry_async_retry_client_auth.get(f"schemas/ids/{jsonschema_sc_id}", auth=reader)
    assert res.status_code == 200


async def test_sr_auth_forwarding(
    registry_async_auth_pair: list[str], registry_async_retry_client_auth: RetryRestClient
) -> None:
    auth = aiohttp.BasicAuth("admin", "admin")

    # Test primary/replica forwarding with global config setting
    primary_url, replica_url = registry_async_auth_pair
    max_tries = 5
    wait_time = 10
    for compat in ["FULL", "BACKWARD", "FORWARD", "NONE"]:
        counter = 0
        resp = await registry_async_retry_client_auth.put(f"{replica_url}/config", json={"compatibility": compat}, auth=auth)
        assert resp.ok
        while True:
            assert counter < max_tries, "Compat update not propagated"
            resp = await registry_async_retry_client_auth.get(f"{primary_url}/config", auth=auth)
            assert resp.ok
            data = resp.json()
            if "compatibilityLevel" not in data:
                counter += 1
                await asyncio.sleep(wait_time)
                continue
            if data["compatibilityLevel"] != compat:
                counter += 1
                await asyncio.sleep(wait_time)
                continue
            break


# Test that Kafka REST API works when configured with Schema Registry requiring authorization
async def test_rest_api_with_sr_auth(
    rest_async_client_registry_auth: RetryRestClient, admin_client: KafkaAdminClient
) -> None:
    client = rest_async_client_registry_auth

    topic = new_topic(admin_client, prefix="cave-rest-")
    await wait_for_topics(client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)

    payload = {"value_schema": schema_avro_json, "records": [{"value": o} for o in test_objects_avro]}
    res = await client.post(f"topics/{topic}", payload, headers={"Content-Type": "application/vnd.kafka.avro.v1+json"})
    assert res.ok
