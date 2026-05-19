"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

End-to-end tests for OIDC bearer token forwarding from the REST Proxy to the
Schema Registry.

Topology (compose profile: e2e):
- karapace-schema-registry-authn-only:8281  — SR with sasl_oauthbearer_authentication_enabled=true
- karapace-rest-proxy-oidc:8382              — REST Proxy with the forwarding gate ON
- karapace-rest-proxy-no-forward:8482        — REST Proxy pointing at the same SR with the gate OFF
                                              (used to prove the gate keeps existing deployments unchanged)
- karapace-schema-registry-basic:8581        — SR with Basic auth (registry_authfile)
- karapace-rest-proxy-basic:8682             — REST Proxy with registry_user/password + gate OFF
                                              (covers the most common existing customer shape — issue #1274 baseline)

Schema-aware request flow (e.g. POST /topics/<t> with vnd.kafka.avro.v2+json):
1. Client → REST Proxy with `Authorization: Bearer <token>`.
2. Proxy enters publish() / fetch(); the gate copies the inbound header into
   `sr_authorization_ctx` only when the flag is True.
3. SchemaRegistryClient reads the contextvar and attaches it as a per-request
   header on the outgoing aiohttp call to SR.
4. SR's OIDCMiddleware validates the token → 200 (happy) or 401 (unhappy).

These tests prove the wire path actually carries the token (happy paths) and
that SR is actually validating it (unhappy paths). The flag-OFF regression test
guards every existing deployment against an accidental forwarding leak.

Note on topic creation: the Kafka broker in compose has
KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, and the proxy's publish() resolves topic
metadata BEFORE calling into SR. Tests must create the topic up front via
admin_client + wait_for_topics so the request actually reaches the schema
fetch — otherwise every call short-circuits with a 40401 "Topic not found"
and the SR auth path is never exercised.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from karapace.core.client import Client
from karapace.core.kafka.admin import KafkaAdminClient
from karapace.kafka_rest_apis.error_codes import RESTErrorCodes
from tests.utils import REST_HEADERS, new_topic, wait_for_topics

NEW_TOPIC_TIMEOUT = 10
PRIMARY_TIMEOUT = 30.0

pytestmark = pytest.mark.asyncio


async def _wait_for_sr_primary(sr_client: Client, timeout: float = PRIMARY_TIMEOUT) -> None:
    """Block until the OIDC SR has elected itself primary.

    Without this guard, schema POSTs hit a non-primary node which then forwards
    them via SR's internal forward_client. That client uses strict TLS verification
    and the shared dev cert has no SAN for karapace-schema-registry-authn-only,
    so the forward fails with a hostname mismatch and SR responds 500 — masking
    the actual token-forwarding behavior we are trying to test.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        res = await sr_client.get("master_available")
        if res.status_code == 200 and res.json_result and res.json_result.get("master_available") is True:
            return
        await asyncio.sleep(1.0)
    raise TimeoutError("Schema registry did not elect a primary within timeout")


@pytest.fixture(name="oidc_sr_primary_ready")
async def fixture_oidc_sr_primary_ready(registry_async_client_oidc_authn_only: Client) -> None:
    """Block until the OIDC SR primary is elected. Tests opt in by listing this fixture."""
    await _wait_for_sr_primary(registry_async_client_oidc_authn_only)


@pytest.fixture(name="basic_sr_primary_ready")
async def fixture_basic_sr_primary_ready(registry_async_client_basic: Client) -> None:
    """Block until the Basic-auth SR primary is elected."""
    await _wait_for_sr_primary(registry_async_client_basic)


async def _ensure_topic(rest_client: Client, admin_client: KafkaAdminClient) -> str:
    """Create a fresh topic and wait until the given proxy can see it."""
    topic = new_topic(admin_client)
    await wait_for_topics(rest_client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    return topic


# ---------------------------------------------------------------------------
# Happy paths: gate ON, valid Bearer reaches SR, schema is registered.
# ---------------------------------------------------------------------------


async def test_avro_publish_forwards_bearer(
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """AVRO produce with a valid Bearer succeeds end-to-end through the OIDC SR."""
    topic = await _ensure_topic(rest_async_client_oidc_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "record", "name": "Simple", "fields": [{"name": "n", "type": "string"}]}),
        "records": [{"value": {"n": "hello"}}],
    }
    res = await rest_async_client_oidc_proxy.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 200, res.json()
    body = res.json()
    assert "value_schema_id" in body
    assert "offsets" in body and len(body["offsets"]) == 1


async def test_jsonschema_publish_forwards_bearer(
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """JSON Schema produce with a valid Bearer succeeds (covers the JSON-schema serializer path)."""
    topic = await _ensure_topic(rest_async_client_oidc_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "object", "properties": {"n": {"type": "string"}}}),
        "records": [{"value": {"n": "hello"}}],
    }
    res = await rest_async_client_oidc_proxy.post(f"/topics/{topic}", payload, headers=REST_HEADERS["jsonschema"])

    assert res.status_code == 200, res.json()
    assert "value_schema_id" in res.json()


async def test_consumer_fetch_forwards_bearer(
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """Schema-aware consume path: register, produce, then fetch records — all under a valid Bearer.

    Exercises the fetch() gate at karapace/kafka_rest_apis/__init__.py: deserialization in
    consumer_manager pulls the schema from SR and must succeed because the contextvar carries
    the inbound bearer through asyncio's per-task context copy.
    """
    topic = await _ensure_topic(rest_async_client_oidc_proxy, admin_client)
    group = "group-" + topic
    instance = "instance-" + topic

    produce = await rest_async_client_oidc_proxy.post(
        f"/topics/{topic}",
        {
            "value_schema": json.dumps({"type": "record", "name": "Simple", "fields": [{"name": "n", "type": "string"}]}),
            "records": [{"value": {"n": "hi"}}],
        },
        headers=REST_HEADERS["avro"],
    )
    assert produce.status_code == 200, produce.json()

    create = await rest_async_client_oidc_proxy.post(
        f"/consumers/{group}",
        {"name": instance, "format": "avro", "auto.offset.reset": "earliest"},
        headers=REST_HEADERS["avro"],
    )
    assert create.status_code == 200, create.json()

    sub = await rest_async_client_oidc_proxy.post(
        f"/consumers/{group}/instances/{instance}/subscription",
        {"topics": [topic]},
        headers=REST_HEADERS["avro"],
    )
    assert sub.status_code == 204, sub.text if hasattr(sub, "text") else sub

    fetch = await rest_async_client_oidc_proxy.get(
        f"/consumers/{group}/instances/{instance}/records",
        headers=REST_HEADERS["avro"],
    )
    assert fetch.status_code == 200, fetch.json()
    records = fetch.json()
    assert any(r.get("value") == {"n": "hi"} for r in records), records


# ---------------------------------------------------------------------------
# Unhappy paths: gate ON, but the token fails at SR.
#
# The proxy currently maps any SR error during schema registration to error_code
# RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR (40801) at HTTP 408 — see __init__.py:1022-1031.
# We assert on that envelope; what matters is that the failure is forced by SR's
# token validation, not by the proxy itself bypassing SR.
#
# Topic must be created first so the proxy doesn't bail with 40401 before
# reaching the schema fetch.
# ---------------------------------------------------------------------------


async def test_avro_publish_invalid_bearer_is_rejected(
    rest_async_client_oidc_proxy_invalid: Client,
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """Garbage Bearer → SR's OIDCMiddleware returns 401 → proxy surfaces it as 40801."""
    # Topic creation needs to happen via the proxy with a VALID token, otherwise we'd
    # still be testing the no-topic path. The invalid-token client only drives publish.
    topic = await _ensure_topic(rest_async_client_oidc_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "string"}),
        "records": [{"value": "leak.if.broken"}],
    }
    res = await rest_async_client_oidc_proxy_invalid.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code != 200
    assert res.json().get("error_code") == RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value


async def test_avro_publish_no_auth_header_is_rejected(
    rest_async_client_oidc_proxy_no_auth_header: Client,
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """No inbound Authorization → contextvar set to None → SR sees no token → 401.

    With the gate ON, the proxy explicitly clears the contextvar when the inbound header
    is missing. The SR client does not fall back to any session_auth here because no
    registry_user is configured for this proxy, so SR's OIDCMiddleware rejects the request.
    """
    topic = await _ensure_topic(rest_async_client_oidc_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "string"}),
        "records": [{"value": "no-token"}],
    }
    res = await rest_async_client_oidc_proxy_no_auth_header.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code != 200
    assert res.json().get("error_code") == RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value


# ---------------------------------------------------------------------------
# Backwards-compat regression: gate OFF (the default for every existing
# deployment). Even with a valid inbound Bearer, the proxy must NOT forward it,
# so a schema write to the OIDC-protected SR fails. This is the unit-test
# `test_publish_does_not_set_ctxvar_when_flag_disabled` proven on the wire.
# ---------------------------------------------------------------------------


async def test_avro_publish_with_gate_off_does_not_forward_bearer(
    rest_async_client_oidc_proxy_no_forward: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """Flag-OFF proxy + valid Bearer + OIDC SR → schema write must fail.

    This guards every existing customer deployment: turning on SR-side OIDC without
    explicitly enabling the proxy flag must not silently leak the inbound token.
    """
    topic = await _ensure_topic(rest_async_client_oidc_proxy_no_forward, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "string"}),
        "records": [{"value": "gate-off"}],
    }
    res = await rest_async_client_oidc_proxy_no_forward.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    # Flag is OFF, so the proxy did not forward the token; SR rejects the call.
    assert res.status_code != 200
    assert res.json().get("error_code") == RESTErrorCodes.SCHEMA_RETRIEVAL_ERROR.value


# ---------------------------------------------------------------------------
# Scenario 7 — backwards-compat for the most common existing customer shape:
# Basic-auth SR + REST Proxy with registry_user/password configured + gate OFF.
#
# This is the deployment that was already working before issue #1274 was filed.
# The contextvar change must NOT regress it: with the gate OFF, the proxy keeps
# using its static BasicAuth on the SR client, so produce succeeds even though
# no inbound Authorization header is provided.
# ---------------------------------------------------------------------------


async def test_basic_auth_proxy_to_basic_sr_unchanged_by_contextvar(
    rest_async_client_basic_proxy: Client,
    admin_client: KafkaAdminClient,
    basic_sr_primary_ready: None,
) -> None:
    """Gate OFF + registry_user/password set + Basic SR → produce works as before."""
    topic = await _ensure_topic(rest_async_client_basic_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "record", "name": "Simple", "fields": [{"name": "n", "type": "string"}]}),
        "records": [{"value": {"n": "basic"}}],
    }
    res = await rest_async_client_basic_proxy.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 200, res.json()
    assert "value_schema_id" in res.json()
