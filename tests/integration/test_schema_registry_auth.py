"""
karapace - schema authorization tests

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.schema_models import SchemaType, ValidatedTypedSchema
from tests.utils import new_random_name, schema_avro_json
from urllib.parse import quote

import aiohttp


async def test_remote_client_auth(registry_async_client_auth: Client) -> None:
    aladdin = aiohttp.BasicAuth("aladdin", "opensesame")

    subject = new_random_name("cave-")

    res = await registry_async_client_auth.post(f"subjects/{quote(subject)}/versions", json={"schema": schema_avro_json})
    assert res.status_code == 401

    res = await registry_async_client_auth.post(
        f"subjects/{quote(subject)}/versions", json={"schema": schema_avro_json}, auth=aladdin
    )
    assert res.status_code == 200
    sc_id = res.json()["id"]
    assert sc_id >= 0

    res = await registry_async_client_auth.get(f"subjects/{quote(subject)}/versions/latest")
    assert res.status_code == 401
    res = await registry_async_client_auth.get(f"subjects/{quote(subject)}/versions/latest", auth=aladdin)
    assert res.status_code == 200
    assert sc_id == res.json()["id"]
    assert ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json) == ValidatedTypedSchema.parse(
        SchemaType.AVRO, res.json()["schema"]
    )
