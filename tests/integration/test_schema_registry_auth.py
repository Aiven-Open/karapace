"""
karapace - schema registry authentication and authorization tests

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.schema_models import SchemaType, ValidatedTypedSchema
from tests.utils import new_random_name, schema_avro_json, schema_jsonschema_json
from urllib.parse import quote

import aiohttp

admin = aiohttp.BasicAuth("admin", "admin")
aladdin = aiohttp.BasicAuth("aladdin", "opensesame")
reader = aiohttp.BasicAuth("reader", "secret")


async def test_sr_auth(registry_async_client_auth: Client) -> None:
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


async def test_sr_list_subjects(registry_async_client_auth: Client) -> None:
    cavesubject = new_random_name("cave-")
    carpetsubject = new_random_name("carpet-")

    res = await registry_async_client_auth.post(
        f"subjects/{quote(cavesubject)}/versions", json={"schema": schema_avro_json}, auth=aladdin
    )
    assert res.status_code == 200
    sc_id = res.json()["id"]
    assert sc_id >= 0

    res = await registry_async_client_auth.post(
        f"subjects/{quote(carpetsubject)}/versions", json={"schema": schema_avro_json}, auth=admin
    )
    assert res.status_code == 200

    res = await registry_async_client_auth.get("subjects", auth=admin)
    assert res.status_code == 200
    assert [cavesubject, carpetsubject] == res.json()

    res = await registry_async_client_auth.get("subjects", auth=aladdin)
    assert res.status_code == 200
    assert [cavesubject] == res.json()

    res = await registry_async_client_auth.get("subjects", auth=reader)
    assert res.status_code == 200
    assert [carpetsubject] == res.json()


async def test_sr_ids(registry_async_client_auth: Client) -> None:

    cavesubject = new_random_name("cave-")
    carpetsubject = new_random_name("carpet-")

    res = await registry_async_client_auth.post(
        f"subjects/{quote(cavesubject)}/versions", json={"schema": schema_avro_json}, auth=aladdin
    )
    assert res.status_code == 200
    avro_sc_id = res.json()["id"]
    assert avro_sc_id >= 0

    res = await registry_async_client_auth.post(
        f"subjects/{quote(carpetsubject)}/versions",
        json={"schemaType": "JSON", "schema": schema_jsonschema_json},
        auth=admin,
    )
    assert res.status_code == 200
    jsonschema_sc_id = res.json()["id"]
    assert jsonschema_sc_id >= 0

    res = await registry_async_client_auth.get(f"schemas/ids/{avro_sc_id}", auth=aladdin)
    assert res.status_code == 200

    res = await registry_async_client_auth.get(f"schemas/ids/{jsonschema_sc_id}", auth=aladdin)
    assert res.status_code == 404
    assert {"error_code": 40403, "message": "Schema not found"} == res.json()

    res = await registry_async_client_auth.get(f"schemas/ids/{avro_sc_id}", auth=reader)
    assert res.status_code == 404
    assert {"error_code": 40403, "message": "Schema not found"} == res.json()

    res = await registry_async_client_auth.get(f"schemas/ids/{jsonschema_sc_id}", auth=reader)
    assert res.status_code == 200
