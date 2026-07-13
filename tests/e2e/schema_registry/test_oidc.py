"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import asyncio
import json

from karapace.core.client import Client
from karapace.core.schema_reader import SchemaType
from tests.utils import new_random_name


async def _wait_for_primary(client: Client, timeout: float = 30.0) -> None:
    """Wait until the schema registry has elected a primary (master).

    Without this, write requests may be forwarded to a node that hasn't
    become primary yet, causing a forwarding loop and a timeout.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        res = await client.get("master_available")
        if res.status_code == 200 and res.json_result and res.json_result.get("master_available") is True:
            return
        await asyncio.sleep(1.0)
    raise TimeoutError("Schema registry did not elect a primary within timeout")


async def test_schema_registry_oidc(
    registry_async_client_oidc: Client,
) -> None:
    subject = new_random_name("subject")

    # Wait for the registry to elect a primary before attempting writes.
    await _wait_for_primary(registry_async_client_oidc)

    # sanity check.
    subject_res = await registry_async_client_oidc.get(f"subjects/{subject}/versions")
    assert subject_res.status_code == 404, "random subject should no exist {subject}"

    subject_res = await registry_async_client_oidc.post(
        f"subjects/{subject}/versions",
        json={
            "schema": json.dumps({"type": "string"}),
            "schemaType": SchemaType.JSONSCHEMA.value,
        },
    )
    assert subject_res.status_code == 200


async def test_schema_registry_oidc_invalid_token(
    registry_async_client_oidc_invalid: Client,
) -> None:
    subject = new_random_name("subject")

    subject_res = await registry_async_client_oidc_invalid.get(f"subjects/{subject}/versions")

    assert subject_res.status_code == 401
    assert subject_res.json_result["error"] == "Unauthorized"
    assert subject_res.json_result["reason"] == "Invalid token/payload"


async def test_integration_oidc_enabled_no_auth_header_fails(
    registry_async_client_oidc_no_auth_header: Client,
) -> None:
    subject = new_random_name("subject")

    subject_res = await registry_async_client_oidc_no_auth_header.get(f"subjects/{subject}/versions")

    assert subject_res.status_code == 401
    assert subject_res.json_result["error"] == "Unauthorized"
    assert subject_res.json_result["reason"] == "Missing or invalid Authorization header"


async def test_integration_oidc_enabled_no_auth_header_skipped_endpoints_success(
    registry_async_client_oidc_no_auth_header: Client,
) -> None:
    # _health should not require auth
    res = await registry_async_client_oidc_no_auth_header.get("_health")
    assert res.status_code == 200

    # metrics should not require auth
    res = await registry_async_client_oidc_no_auth_header.get("metrics", json_response=False)
    assert res.status_code == 200


# ---------------------------------------------------------------------------
# AuthN-only mode: sasl_oauthbearer_authentication_enabled=true with
# sasl_oauthbearer_authorization_enabled=false. Backed by the
# karapace-schema-registry-authn-only service in compose.yml.
#
# Expected behavior:
#   - Valid token  → 200 (no role check)
#   - Invalid token → 401
#   - No header   → 401
#   - Skip paths  → 200 even without a token
# ---------------------------------------------------------------------------


async def test_schema_registry_oidc_authn_only_valid_token(
    registry_async_client_oidc_authn_only: Client,
) -> None:
    """A valid token must succeed even though the user has no roles configured for the resource."""
    subject = new_random_name("subject")

    await _wait_for_primary(registry_async_client_oidc_authn_only)

    subject_res = await registry_async_client_oidc_authn_only.get(f"subjects/{subject}/versions")
    assert subject_res.status_code == 404

    subject_res = await registry_async_client_oidc_authn_only.post(
        f"subjects/{subject}/versions",
        json={
            "schema": json.dumps({"type": "string"}),
            "schemaType": SchemaType.JSONSCHEMA.value,
        },
    )
    # Authorization is disabled — the write must succeed for any valid token.
    assert subject_res.status_code == 200


async def test_schema_registry_oidc_authn_only_invalid_token(
    registry_async_client_oidc_authn_only_invalid: Client,
) -> None:
    subject = new_random_name("subject")

    subject_res = await registry_async_client_oidc_authn_only_invalid.get(f"subjects/{subject}/versions")

    assert subject_res.status_code == 401
    assert subject_res.json_result["error"] == "Unauthorized"
    assert subject_res.json_result["reason"] == "Invalid token/payload"


async def test_schema_registry_oidc_authn_only_no_auth_header_fails(
    registry_async_client_oidc_authn_only_no_auth_header: Client,
) -> None:
    subject = new_random_name("subject")

    subject_res = await registry_async_client_oidc_authn_only_no_auth_header.get(f"subjects/{subject}/versions")

    assert subject_res.status_code == 401
    assert subject_res.json_result["error"] == "Unauthorized"
    assert subject_res.json_result["reason"] == "Missing or invalid Authorization header"


async def test_schema_registry_oidc_authn_only_skip_paths(
    registry_async_client_oidc_authn_only_no_auth_header: Client,
) -> None:
    res = await registry_async_client_oidc_authn_only_no_auth_header.get("_health")
    assert res.status_code == 200

    res = await registry_async_client_oidc_authn_only_no_auth_header.get("metrics", json_response=False)
    assert res.status_code == 200


# Pins the OIDC-side 404 body shape; full forbidden-vs-missing parity is
# exercised in tests/integration/test_schema_registry_auth.py.
async def test_schema_registry_oidc_missing_subject_returns_canonical_404(
    registry_async_client_oidc: Client,
) -> None:
    subject = new_random_name("missing-")
    await _wait_for_primary(registry_async_client_oidc)

    res = await registry_async_client_oidc.get(f"subjects/{subject}/versions")
    assert res.status_code == 404
    assert res.json_result == {
        "error_code": 40401,
        "message": f"Subject '{subject}' not found.",
    }


# ---------------------------------------------------------------------------
# RBAC denial: karapace-schema-registry-oidc runs with authorization enabled.
# The karapace-client-limited token carries only karapace.schema:read +
# schema:write, so methods whose method_roles require other roles are 403:
#   PUT    -> requires config_subject:update / config_global:update  (not granted)
#   DELETE -> requires schema:delete / subject:delete                (not granted)
# GET/POST succeed and are covered by test_schema_registry_oidc above.
# ---------------------------------------------------------------------------


async def test_schema_registry_oidc_put_config_denied_403(
    registry_async_client_oidc_limited: Client,
) -> None:
    subject = new_random_name("subject")
    await _wait_for_primary(registry_async_client_oidc_limited)

    res = await registry_async_client_oidc_limited.put(
        f"config/{subject}",
        json={"compatibility": "NONE"},
    )
    assert res.status_code == 403
    assert res.json_result == {"error": "Authorization error", "reason": "Forbidden"}


async def test_schema_registry_oidc_delete_subject_denied_403(
    registry_async_client_oidc_limited: Client,
) -> None:
    subject = new_random_name("subject")
    await _wait_for_primary(registry_async_client_oidc_limited)

    # POST (schema:write) is granted, so registering succeeds.
    post_res = await registry_async_client_oidc_limited.post(
        f"subjects/{subject}/versions",
        json={
            "schema": json.dumps({"type": "string"}),
            "schemaType": SchemaType.JSONSCHEMA.value,
        },
    )
    assert post_res.status_code == 200

    # DELETE requires schema:delete / subject:delete, which the token lacks.
    del_res = await registry_async_client_oidc_limited.delete(f"subjects/{subject}")
    assert del_res.status_code == 403
    assert del_res.json_result == {"error": "Authorization error", "reason": "Forbidden"}


# ---------------------------------------------------------------------------
# Existence non-leakage. The OIDC gate is middleware that runs before routing,
# so an unauthenticated probe of an EXISTING subject must be indistinguishable
# from a probe of a MISSING one — both 401, identical body, no 404 that would
# leak existence. (OIDC authz is per-HTTP-method, not per-subject; there is no
# per-subject boundary to probe, unlike basic-auth.)
# ---------------------------------------------------------------------------


async def test_schema_registry_oidc_unauthenticated_probe_does_not_leak_existence(
    registry_async_client_oidc: Client,
    registry_async_client_oidc_no_auth_header: Client,
) -> None:
    existing = new_random_name("existing-")
    missing = new_random_name("missing-")

    await _wait_for_primary(registry_async_client_oidc)

    # Create `existing` with an authorized token.
    created = await registry_async_client_oidc.post(
        f"subjects/{existing}/versions",
        json={"schema": json.dumps({"type": "string"}), "schemaType": SchemaType.JSONSCHEMA.value},
    )
    assert created.status_code == 200

    # Unauthenticated probes of both must be identical 401s — no existence leak.
    existing_res = await registry_async_client_oidc_no_auth_header.get(f"subjects/{existing}/versions")
    missing_res = await registry_async_client_oidc_no_auth_header.get(f"subjects/{missing}/versions")

    assert existing_res.status_code == 401
    assert missing_res.status_code == 401
    assert existing_res.json_result == missing_res.json_result
    assert existing_res.json_result == {
        "error": "Unauthorized",
        "reason": "Missing or invalid Authorization header",
    }


# Complements the PUT/DELETE 403 tests: the read/write token CAN read an existing
# subject via GET, making the per-method RBAC model explicit end-to-end.
async def test_schema_registry_oidc_read_role_can_get_existing_subject(
    registry_async_client_oidc_limited: Client,
) -> None:
    subject = new_random_name("subject")
    await _wait_for_primary(registry_async_client_oidc_limited)

    created = await registry_async_client_oidc_limited.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps({"type": "string"}), "schemaType": SchemaType.JSONSCHEMA.value},
    )
    assert created.status_code == 200

    # GET requires schema:read / subject:read, which the token has.
    res = await registry_async_client_oidc_limited.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
