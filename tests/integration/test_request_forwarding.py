"""
karapace - test request forwarding

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import asyncio
import json
from typing import AsyncGenerator
from urllib.parse import quote_plus

import pytest
from _pytest.fixtures import SubRequest

from karapace.core.client import Client
from tests.integration.utils.rest_client import RetryRestClient
from tests.utils import (
    create_subject_name_factory,
    repeat_until_master_is_available,
    repeat_until_successful_request,
)


@pytest.fixture(scope="function", name="request_forwarding_retry_client")
async def fixture_registry_async_client(
    request: SubRequest,
    registry_async_pair: list[str],
) -> AsyncGenerator[RetryRestClient, None]:
    registry_async_pair[0]
    client = Client(
        server_uri=registry_async_pair[0],
        server_ca=request.config.getoption("server_ca"),
    )

    try:
        # wait until the server is listening, otherwise the tests may fail
        await repeat_until_successful_request(
            client.get,
            "subjects",
            json_data=None,
            headers=None,
            error_msg=f"Registry API {client.server_uri} is unreachable",
            timeout=10,
            sleep=0.3,
        )
        await repeat_until_master_is_available(client)
        yield RetryRestClient(client)
    finally:
        await client.close()


@pytest.mark.parametrize("subject", ["test_forwarding", "test_forw/arding"])
async def test_schema_request_forwarding(
    registry_async_pair: list[str],
    request_forwarding_retry_client: RetryRestClient,
    subject: str,
) -> None:
    master_url, slave_url = registry_async_pair

    max_tries, counter = 5, 0
    wait_time = 0.5
    subject = create_subject_name_factory(subject)()
    schema = {"type": "string"}
    other_schema = {"type": "int"}
    # Config updates
    for subj_path in [None, subject]:
        if subj_path:
            path = f"config/{quote_plus(subject)}"
        else:
            path = "config"
        for compat in ["FULL", "BACKWARD", "FORWARD", "NONE"]:
            resp = await request_forwarding_retry_client.put(f"{slave_url}/{path}", json={"compatibility": compat})
            assert resp.ok
            while True:
                assert counter < max_tries, "Compat update not propagated"
                resp = await request_forwarding_retry_client.get(f"{master_url}/{path}")
                if not resp.ok:
                    print(f"Invalid http status code: {resp.status_code}")
                    continue
                data = resp.json()
                if "compatibilityLevel" not in data:
                    print(f"Invalid response: {data}")
                    counter += 1
                    await asyncio.sleep(wait_time)
                    continue
                if data["compatibilityLevel"] != compat:
                    print(f"Bad compatibility: {data}")
                    counter += 1
                    await asyncio.sleep(wait_time)
                    continue
                break

    # New schema updates, last compatibility is None
    for s in [schema, other_schema]:
        resp = await request_forwarding_retry_client.post(
            f"{slave_url}/subjects/{quote_plus(subject)}/versions", json={"schema": json.dumps(s)}
        )
    assert resp.ok
    data = resp.json()
    assert "id" in data, data
    counter = 0
    while True:
        assert counter < max_tries, "Subject schema data not propagated yet"
        resp = await request_forwarding_retry_client.get(f"{master_url}/subjects/{quote_plus(subject)}/versions")
        if not resp.ok:
            print(f"Invalid http status code: {resp.status_code}")
            counter += 1
            continue
        data = resp.json()
        if not data:
            print(f"No versions registered for subject {subject} yet")
            counter += 1
            continue
        assert len(data) == 2, data
        assert data[0] == 1, data
        print("Subject schema data propagated")
        break

    # Schema deletions
    resp = await request_forwarding_retry_client.delete(f"{slave_url}/subjects/{quote_plus(subject)}/versions/1")
    assert resp.ok
    counter = 0
    while True:
        assert counter < max_tries, "Subject version deletion not propagated yet"
        resp = await request_forwarding_retry_client.get(
            f"{master_url}/subjects/{quote_plus(subject)}/versions/1", expected_response_code=404
        )
        if resp.ok:
            print(f"Subject {subject} still has version 1 on master")
            counter += 1
            continue
        assert resp.status_code == 404
        print(f"Subject {subject} no longer has version 1")
        break

    # Subject deletion
    resp = await request_forwarding_retry_client.get(f"{master_url}/subjects/")
    assert resp.ok
    data = resp.json()
    assert subject in data
    resp = await request_forwarding_retry_client.delete(f"{slave_url}/subjects/{quote_plus(subject)}")
    assert resp.ok
    counter = 0
    while True:
        assert counter < max_tries, "Subject deletion not propagated yet"
        resp = await request_forwarding_retry_client.get(f"{master_url}/subjects/")
        if not resp.ok:
            print("Could not retrieve subject list on master")
            counter += 1
            continue
        data = resp.json()
        assert subject not in data
        break
