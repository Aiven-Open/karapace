"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.typing import Mode
from tests.utils import create_schema_name_factory, create_subject_name_factory

import json
import pytest


@pytest.mark.parametrize("trail", ["", "/"])
async def test_global_mode(registry_async_client: Client, trail: str) -> None:
    res = await registry_async_client.get(f"/mode{trail}")
    assert res.status_code == 200
    json_res = res.json()
    assert json_res == {"mode": str(Mode.readwrite)}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_subject_mode(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_schema_same_subject_{trail}")
    schema_name = create_schema_name_factory(f"test_schema_same_subject_{trail}")()

    schema_str = json.dumps(
        {
            "type": "record",
            "name": schema_name,
            "fields": [
                {
                    "name": "f",
                    "type": "string",
                }
            ],
        }
    )
    subject = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": schema_str},
    )
    assert res.status_code == 200

    res = await registry_async_client.get(f"/mode/{subject}{trail}")
    assert res.status_code == 200
    json_res = res.json()
    assert json_res == {"mode": str(Mode.readwrite)}

    res = await registry_async_client.get(f"/mode/unknown_subject{trail}")
    assert res.status_code == 404
    json_res = res.json()
    assert json_res == {"error_code": 40401, "message": "Subject 'unknown_subject' not found."}
