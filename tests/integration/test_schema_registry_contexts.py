"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from urllib.parse import quote_plus

import json

from karapace.core.client import Client
from tests.utils import create_subject_name_factory, new_random_name


def _avro_schema(name: str, field: str) -> str:
    return json.dumps({"type": "record", "name": name, "fields": [{"name": field, "type": "string"}]})


def _schema_name(result_json: dict) -> str:
    return json.loads(result_json["schema"])["name"]


async def test_same_schema_id_in_two_contexts(registry_async_client: Client) -> None:
    """A schema mirrored into a context keeps its id, which names another schema in the default context.

    The subject sent with the id selects the context; an unqualified subject unknown in the default
    context is looked up by its name in the other contexts.
    """
    context = "." + new_random_name("ctx")
    subject = create_subject_name_factory("test_same_schema_id_in_two_contexts")()
    mirrored = f"A.{subject}"
    qualified = f":{context}:{mirrored}"

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": _avro_schema("Local", "id")})
    assert res.status_code == 200
    schema_id = res.json()["id"]

    # Mirror into the context with the same id, through the context path as a mirroring tool would.
    res = await registry_async_client.put(f"/contexts/{context}/mode/{quote_plus(mirrored)}", json={"mode": "IMPORT"})
    assert res.status_code == 200
    res = await registry_async_client.post(
        f"/contexts/{context}/subjects/{quote_plus(mirrored)}/versions",
        json={"schema": _avro_schema("Mirror", "orderId"), "id": schema_id, "version": 1},
    )
    assert res.status_code == 200, res.json()
    assert res.json() == {"id": schema_id}

    res = await registry_async_client.get_subjects_subject_version(subject=qualified, version=1)
    assert res.status_code == 200
    assert res.json()["id"] == schema_id

    expected = [
        ({}, "Local"),
        ({"subject": subject}, "Local"),
        ({"subject": qualified}, "Mirror"),
        ({"subject": f":{context}:"}, "Mirror"),
        # Unqualified, unknown in the default context: found by its name in the context.
        ({"subject": mirrored}, "Mirror"),
    ]
    for params, name in expected:
        res = await registry_async_client.get_schema_by_id(schema_id=schema_id, params=params)
        assert res.status_code == 200, params
        assert _schema_name(res.json()) == name, params

    res = await registry_async_client.get(f"/contexts/{context}/schemas/ids/{schema_id}")
    assert res.status_code == 200
    assert _schema_name(res.json()) == "Mirror"

    res = await registry_async_client.get("/contexts")
    assert res.status_code == 200
    assert "." in res.json()
    assert context in res.json()


async def test_context_has_its_own_id_counter(registry_async_client: Client) -> None:
    """Ids of a new context start at 1, whatever the ids already used in the default context."""
    context = "." + new_random_name("ctx")
    subject = create_subject_name_factory("test_context_has_its_own_id_counter")()

    for name, expected_id in (("First", 1), ("Second", 2)):
        res = await registry_async_client.post(
            f"/contexts/{context}/subjects/{quote_plus(subject)}-{name}/versions",
            json={"schema": _avro_schema(name, "f")},
        )
        assert res.status_code == 200, res.json()
        assert res.json() == {"id": expected_id}

    res = await registry_async_client.get(f"/contexts/{context}/schemas/ids/2")
    assert res.status_code == 200
    assert _schema_name(res.json()) == "Second"
