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
