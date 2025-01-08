"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.client import Client
from karapace.typing import Mode
from tests.utils import create_schema_name_factory, create_subject_name_factory

import json


async def test_global_mode(registry_async_client: Client) -> None:
    res = await registry_async_client.get("/mode")
    assert res.status_code == 200
    json_res = res.json()
    assert json_res == {"mode": str(Mode.readwrite)}


async def test_subject_mode(registry_async_client: Client) -> None:
    subject_name_factory = create_subject_name_factory("test_schema_same_subject")
    schema_name = create_schema_name_factory("test_schema_same_subject")()

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

    res = await registry_async_client.get(f"/mode/{subject}")
    assert res.status_code == 200
    json_res = res.json()
    assert json_res == {"mode": str(Mode.readwrite)}

    res = await registry_async_client.get("/mode/unknown_subject")
    assert res.status_code == 404
    json_res = res.json()
    assert json_res == {"error_code": 40401, "message": "Subject 'unknown_subject' not found."}
