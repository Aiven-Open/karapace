"""
Scheme-based auth dispatch e2e: one SR with OIDC authentication AND file-based basic auth.
A `Bearer` request is validated as OIDC; a `Basic` request is checked against the authfile
(users + ACL). Bearer takes priority; there is no fallback between schemes.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

import asyncio
import json

from aiohttp import BasicAuth

from karapace.core.client import Client
from karapace.core.schema_reader import SchemaType
from tests.utils import new_random_name

_SCHEMA = {"schema": json.dumps({"type": "string"}), "schemaType": SchemaType.JSONSCHEMA.value}

# Credentials from tests/integration/config/karapace.auth.json:
#   admin/admin      -> Write .*            (full access)
#   aladdin/opensesame -> Write Subject:cave-.*
_ADMIN = BasicAuth("admin", "admin")
_ALADDIN = BasicAuth("aladdin", "opensesame")


async def _wait_for_primary(client: Client, auth: BasicAuth | None = None, timeout: float = 30.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        res = await client.get("master_available", auth=auth)
        if res.status_code == 200 and res.json_result and res.json_result.get("master_available") is True:
            return
        await asyncio.sleep(1.0)
    raise TimeoutError("Schema registry did not elect a primary within timeout")


async def test_bearer_request_dispatches_to_oidc(
    registry_async_client_oidc_basic_bearer: Client,
) -> None:
    await _wait_for_primary(registry_async_client_oidc_basic_bearer)
    subject = new_random_name("subject")

    res = await registry_async_client_oidc_basic_bearer.post(f"subjects/{subject}/versions", json=_SCHEMA)

    assert res.status_code == 200


async def test_basic_admin_request_dispatches_to_authfile(
    registry_async_client_oidc_basic: Client,
) -> None:
    await _wait_for_primary(registry_async_client_oidc_basic, auth=_ADMIN)
    subject = new_random_name("subject")

    # admin has Write on any resource, so the basic-auth ACL allows the write.
    res = await registry_async_client_oidc_basic.post(f"subjects/{subject}/versions", json=_SCHEMA, auth=_ADMIN)

    assert res.status_code == 200


async def test_basic_acl_is_enforced_for_authfile_user(
    registry_async_client_oidc_basic: Client,
) -> None:
    await _wait_for_primary(registry_async_client_oidc_basic, auth=_ADMIN)

    # aladdin may write cave-* subjects.
    cave = new_random_name("cave-")
    allowed = await registry_async_client_oidc_basic.post(f"subjects/{cave}/versions", json=_SCHEMA, auth=_ALADDIN)
    assert allowed.status_code == 200

    # aladdin has no permission on carpet-*, which looks identical to a missing subject (404).
    carpet = new_random_name("carpet-")
    denied = await registry_async_client_oidc_basic.post(f"subjects/{carpet}/versions", json=_SCHEMA, auth=_ALADDIN)
    assert denied.status_code == 404


async def test_basic_wrong_password_is_rejected(
    registry_async_client_oidc_basic: Client,
) -> None:
    subject = new_random_name("subject")

    res = await registry_async_client_oidc_basic.get(
        f"subjects/{subject}/versions", auth=BasicAuth("admin", "wrong-password")
    )

    assert res.status_code == 401
