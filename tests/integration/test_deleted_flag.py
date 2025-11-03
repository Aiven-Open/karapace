"""
karapace - deleted flag tests

Integration tests for deleted flag in schema version lookup

These tests verify that when querying a subject version with deleted=true, the
response payload includes a boolean 'deleted' field indicating whether the
returned version is soft-deleted.

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from http import HTTPStatus
from karapace.core.client import Client
from tests.utils import create_subject_name_factory

import json


async def test_deleted_flag_present_for_soft_deleted_version(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_deleted_flag_soft")()

    schema_v1 = {
        "type": "record",
        "name": "softDeletedRecord",
        "fields": [
            {"name": "field1", "type": "string"},
        ],
    }

    # Register schema version 1
    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": json.dumps(schema_v1)})
    assert res.status_code == HTTPStatus.OK

    # Soft delete version 1
    res = await registry_async_client.delete_subjects_version(subject=subject, version=1)
    assert res.status_code == HTTPStatus.OK
    # Accept flexible delete responses: many SR implementations return an integer version,
    # others may wrap it in an object or list. Be tolerant while ensuring version 1 was deleted.
    del_payload = res.json()
    if isinstance(del_payload, int):
        assert del_payload == 1
    elif isinstance(del_payload, list):
        assert 1 in del_payload
    elif isinstance(del_payload, dict):
        assert 1 in del_payload.values() or del_payload.get("data") == 1
    else:
        raise AssertionError(f"Unexpected delete response payload: {del_payload}")

    # Fetch the soft-deleted version with deleted=true and assert deleted flag is present and True
    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=1, params={"deleted": "true"})
    assert res.status_code == HTTPStatus.OK
    payload = res.json()
    # Unwrap envelope responses (some deployments return {"data": {...}})
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]
    assert payload.get("subject") == subject
    assert payload.get("version") == 1
    assert "deleted" in payload, "deleted flag should be present when deleted=true is requested"
    assert (
        isinstance(payload["deleted"], bool) and payload["deleted"] is True
    ), "deleted flag should be True for soft-deleted versions"


async def test_deleted_flag_present_and_false_for_live_version(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_deleted_flag_live")()

    schema_v1 = {
        "type": "record",
        "name": "liveRecord",
        "fields": [
            {"name": "field1", "type": "string"},
        ],
    }

    # Register schema version 1 (live)
    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": json.dumps(schema_v1)})
    assert res.status_code == HTTPStatus.OK

    # Fetch the live version with deleted=true and assert deleted flag is present and False
    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=1, params={"deleted": "true"})
    assert res.status_code == HTTPStatus.OK
    payload = res.json()
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]
    assert payload.get("subject") == subject
    assert payload.get("version") == 1
    assert "deleted" in payload, "deleted flag should be present when deleted=true is requested"
    assert (
        isinstance(payload["deleted"], bool) and payload["deleted"] is False
    ), "deleted flag should be False for live versions when deleted=true is requested"
