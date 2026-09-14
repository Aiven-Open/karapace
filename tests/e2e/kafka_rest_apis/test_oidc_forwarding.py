"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

End-to-end tests for OIDC bearer forwarding from the REST Proxy to the Schema Registry.

Topology (compose profile: e2e):
- karapace-schema-registry-authn-only:8281   — OIDC SR (forwarding gate target).
- karapace-rest-proxy-oidc:8382              — REST Proxy with the gate ON.
- karapace-rest-proxy-oidc-with-creds:8582   — gate ON *and* registry_user/password set (coexistence).
- karapace-rest-proxy-no-forward:8482        — REST Proxy with the gate OFF (regression guard).
- karapace-schema-registry-basic:8581        — Basic-auth SR.
- karapace-rest-proxy-basic:8682             — Basic-auth proxy paired with it (issue #1274 baseline).
- karapace-schema-registry-oidc-basic:8681   — OIDC+authfile SR (scheme dispatch).
- karapace-rest-proxy-oidc-basic:8782        — proxy that forwards a client's Basic header to it.

Topics are created up front because the broker has auto-create disabled; otherwise
publish() short-circuits with 40401 before the SR auth path is exercised.
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
    """Wait until SR has elected a primary; otherwise writes hit a non-primary and
    SR's internal forward_client masks the auth path under test."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        res = await sr_client.get("master_available")
        if res.status_code == 200 and res.json_result and res.json_result.get("master_available") is True:
            return
        await asyncio.sleep(1.0)
    raise TimeoutError("Schema registry did not elect a primary within timeout")


@pytest.fixture(name="oidc_sr_primary_ready")
async def fixture_oidc_sr_primary_ready(registry_async_client_oidc_authn_only: Client) -> None:
    await _wait_for_sr_primary(registry_async_client_oidc_authn_only)


@pytest.fixture(name="basic_sr_primary_ready")
async def fixture_basic_sr_primary_ready(registry_async_client_basic: Client) -> None:
    await _wait_for_sr_primary(registry_async_client_basic)


@pytest.fixture(name="oidc_basic_sr_primary_ready")
async def fixture_oidc_basic_sr_primary_ready(registry_async_client_oidc_basic_bearer: Client) -> None:
    # master_available is auth-gated on the OIDC SR; use the authenticated bearer client to poll.
    await _wait_for_sr_primary(registry_async_client_oidc_basic_bearer)


async def _ensure_topic(rest_client: Client, admin_client: KafkaAdminClient) -> str:
    """Create a fresh topic and wait until the proxy sees it."""
    topic = new_topic(admin_client)
    await wait_for_topics(rest_client, topic_names=[topic], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    return topic


# Happy paths: gate ON, valid Bearer reaches SR.


async def test_avro_publish_forwards_bearer(
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """AVRO produce with a valid Bearer succeeds through the OIDC SR."""
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


async def test_avro_publish_forwards_bearer_with_registry_creds_set(
    rest_async_client_oidc_proxy_with_creds: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """Coexistence regression guard: the proxy has registry_user/password configured AND
    forwards a valid Bearer. The forwarded token must win over the basic credentials.

    Before the per-request auth fix, this combination raised aiohttp's
    "Cannot combine AUTHORIZATION header with AUTH argument" and surfaced as a 500.
    """
    topic = await _ensure_topic(rest_async_client_oidc_proxy_with_creds, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "record", "name": "Simple", "fields": [{"name": "n", "type": "string"}]}),
        "records": [{"value": {"n": "coexist"}}],
    }
    res = await rest_async_client_oidc_proxy_with_creds.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 200, res.json()
    body = res.json()
    assert "value_schema_id" in body
    assert "offsets" in body and len(body["offsets"]) == 1


async def test_jsonschema_publish_forwards_bearer(
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """JSON Schema produce with a valid Bearer succeeds."""
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
    """Exercises the fetch() gate end-to-end: consumer deserialization pulls the schema
    from SR under the inbound bearer."""
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


# Coexistence: a forwarded *Basic* header is relayed to an OIDC+authfile SR and validated there.


async def test_avro_publish_forwards_basic_credentials(
    rest_async_client_oidc_basic_fwd_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_basic_sr_primary_ready: None,
) -> None:
    """Client sends Basic admin:admin to a proxy whose own registry_user/password are invalid.

    The proxy (gate ON) forwards the Basic header to the OIDC+authfile SR, which validates it
    against the authfile. Success proves the forwarded header — not the invalid fallback creds —
    was used, and that a forwarded Basic header coexists with configured session credentials.
    """
    topic = await _ensure_topic(rest_async_client_oidc_basic_fwd_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "record", "name": "Simple", "fields": [{"name": "n", "type": "string"}]}),
        "records": [{"value": {"n": "basic-fwd"}}],
    }
    res = await rest_async_client_oidc_basic_fwd_proxy.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 200, res.json()
    body = res.json()
    assert "value_schema_id" in body
    assert "offsets" in body and len(body["offsets"]) == 1


# Unhappy paths: SR rejects the credentials/token; proxy surfaces a precise 401.


async def test_avro_publish_invalid_bearer_is_rejected(
    rest_async_client_oidc_proxy_invalid: Client,
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """Garbage Bearer is rejected by SR (401); proxy surfaces a precise 401, not a generic error."""
    # Topic creation uses a valid-token client so we exercise the SR auth path, not 40401.
    topic = await _ensure_topic(rest_async_client_oidc_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "string"}),
        "records": [{"value": "leak.if.broken"}],
    }
    res = await rest_async_client_oidc_proxy_invalid.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 401, res.json()
    assert res.json().get("error_code") == RESTErrorCodes.HTTP_UNAUTHORIZED.value


async def test_avro_publish_no_auth_header_is_rejected(
    rest_async_client_oidc_proxy_no_auth_header: Client,
    rest_async_client_oidc_proxy: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """Gate ON, no inbound Authorization, no registry_user fallback: SR rejects with 401."""
    topic = await _ensure_topic(rest_async_client_oidc_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "string"}),
        "records": [{"value": "no-token"}],
    }
    res = await rest_async_client_oidc_proxy_no_auth_header.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 401, res.json()
    assert res.json().get("error_code") == RESTErrorCodes.HTTP_UNAUTHORIZED.value


# Backwards-compat: gate OFF must not forward the inbound Bearer.


async def test_avro_publish_with_gate_off_does_not_forward_bearer(
    rest_async_client_oidc_proxy_no_forward: Client,
    admin_client: KafkaAdminClient,
    oidc_sr_primary_ready: None,
) -> None:
    """Gate OFF + valid Bearer + OIDC SR: write fails because nothing is forwarded (SR 401)."""
    topic = await _ensure_topic(rest_async_client_oidc_proxy_no_forward, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "string"}),
        "records": [{"value": "gate-off"}],
    }
    res = await rest_async_client_oidc_proxy_no_forward.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 401, res.json()
    assert res.json().get("error_code") == RESTErrorCodes.HTTP_UNAUTHORIZED.value


# Issue #1274 baseline: Basic proxy + Basic SR + gate OFF must keep working.


async def test_basic_auth_proxy_to_basic_sr_unchanged_by_contextvar(
    rest_async_client_basic_proxy: Client,
    admin_client: KafkaAdminClient,
    basic_sr_primary_ready: None,
) -> None:
    """Gate OFF + registry_user/password + Basic SR: produce still works as before."""
    topic = await _ensure_topic(rest_async_client_basic_proxy, admin_client)

    payload = {
        "value_schema": json.dumps({"type": "record", "name": "Simple", "fields": [{"name": "n", "type": "string"}]}),
        "records": [{"value": {"n": "basic"}}],
    }
    res = await rest_async_client_basic_proxy.post(f"/topics/{topic}", payload, headers=REST_HEADERS["avro"])

    assert res.status_code == 200, res.json()
    assert "value_schema_id" in res.json()
