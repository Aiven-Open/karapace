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

    # master availability is used as a readiness probe and must not depend on OIDC/JWKS reachability.
    res = await registry_async_client_oidc_no_auth_header.get("master_available")
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

    res = await registry_async_client_oidc_authn_only_no_auth_header.get("master_available")
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
