"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from karapace.client import Client
from karapace.typing import JsonObject, Subject
from tests.base_testcase import BaseTestCase
from typing import Any, Final

import json
import logging
import pytest

SchemaRegitrationFunc = Callable[[Client, Subject], Coroutine[Any, Any, None]]

LOG = logging.getLogger(__name__)

schema_int: Final[JsonObject] = {"type": "record", "name": "schema_name", "fields": [{"type": "int", "name": "field_name"}]}
schema_long: Final[JsonObject] = {
    "type": "record",
    "name": "schema_name",
    "fields": [{"type": "long", "name": "field_name"}],
}
schema_string: Final[JsonObject] = {
    "type": "record",
    "name": "schema_name",
    "fields": [{"type": "string", "name": "field_name"}],
}
schema_double: Final[JsonObject] = {
    "type": "record",
    "name": "schema_name",
    "fields": [{"type": "double", "name": "field_name"}],
}


@dataclass
class SchemaCompatibilityTestCase(BaseTestCase):
    new_schema: str
    compatibility_mode: str
    register_baseline_schemas: SchemaRegitrationFunc
    expected_is_compatible: bool | None
    expected_status_code: int
    expected_incompatibilities: list[str] | None


async def _register_baseline_schemas_no_incompatibilities(registry_async_client: Client, subject: Subject) -> None:
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(schema_int)},
    )
    assert res.status_code == 200

    # Changing type from int to long is compatible
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(schema_long)},
    )
    assert res.status_code == 200


async def _register_baseline_schemas_with_incompatibilities(registry_async_client: Client, subject: Subject) -> None:
    # Allow registering non backward compatible schemas
    await _set_compatibility_mode(registry_async_client, subject, "NONE")

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(schema_string)},
    )
    assert res.status_code == 200

    # Changing type from string to double is incompatible
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(schema_double)},
    )
    assert res.status_code == 200


async def _register_baseline_schemas_with_incompatibilities_and_a_deleted_schema(
    registry_async_client: Client, subject: Subject
) -> None:
    await _register_baseline_schemas_with_incompatibilities(registry_async_client, subject)

    # Register schema
    # Changing type from double to long is incompatible
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(schema_long)},
    )
    assert res.status_code == 200

    # And delete it
    res = await registry_async_client.delete(f"subjects/{subject}/versions/latest")
    assert res.status_code == 200
    assert res.json() == 3


async def _register_no_baseline_schemas(
    registry_async_client: Client, subject: Subject  # pylint: disable=unused-argument
) -> None:
    pass


async def _set_compatibility_mode(registry_async_client: Client, subject: Subject, compatibility_mode: str) -> None:
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility_mode})
    assert res.status_code == 200


@pytest.mark.parametrize(
    "test_case",
    [
        # Case 0
        # New schema compatible with all baseline ones (int --> long, long --> long)
        # Transitive mode
        #   --> No incompatibilities are found
        SchemaCompatibilityTestCase(
            test_name="case0",
            compatibility_mode="BACKWARD",
            register_baseline_schemas=_register_baseline_schemas_no_incompatibilities,
            new_schema=json.dumps(schema_long),
            expected_is_compatible=True,
            expected_status_code=200,
            expected_incompatibilities=None,
        ),
        # Case 1
        # Same as previous case, but in non-transitive mode
        #   --> No incompatibilities are found
        SchemaCompatibilityTestCase(
            test_name="case1",
            compatibility_mode="BACKWARD_TRANSITIVE",
            register_baseline_schemas=_register_baseline_schemas_no_incompatibilities,
            new_schema=json.dumps(schema_long),
            expected_is_compatible=True,
            expected_status_code=200,
            expected_incompatibilities=None,
        ),
        # Case 2
        # New schema incompatible with both baseline schemas (string --> int, double --> int)
        # Non-transitive mode
        #   --> Incompatibilies are found only against last baseline schema (double --> int)
        SchemaCompatibilityTestCase(
            test_name="case2",
            compatibility_mode="BACKWARD",
            register_baseline_schemas=_register_baseline_schemas_with_incompatibilities,
            new_schema=json.dumps(schema_int),
            expected_is_compatible=False,
            expected_status_code=200,
            expected_incompatibilities=["reader type: int not compatible with writer type: double"],
        ),
        # Case 3
        # Same as previous case, but in non-transitive mode
        #   --> Incompatibilies are found in the first baseline schema (string --> int)
        SchemaCompatibilityTestCase(
            test_name="case3",
            compatibility_mode="BACKWARD_TRANSITIVE",
            register_baseline_schemas=_register_baseline_schemas_with_incompatibilities,
            new_schema=json.dumps(schema_int),
            expected_is_compatible=False,
            expected_status_code=200,
            expected_incompatibilities=["reader type: int not compatible with writer type: string"],
        ),
        # Case 4
        # Same as case 2, but with a deleted schema among baseline ones
        # Non-transitive mode
        #   --> The delete schema is ignored
        #   --> Incompatibilies are found only against last baseline schema (double --> int)
        SchemaCompatibilityTestCase(
            test_name="case4",
            compatibility_mode="BACKWARD",
            register_baseline_schemas=_register_baseline_schemas_with_incompatibilities_and_a_deleted_schema,
            new_schema=json.dumps(schema_int),
            expected_is_compatible=False,
            expected_status_code=200,
            expected_incompatibilities=["reader type: int not compatible with writer type: double"],
        ),
        # Case 5
        # Same as case 3, but with a deleted schema among baseline ones
        #   --> The delete schema is ignored
        #   --> Incompatibilies are found in the first baseline schema (string --> int)
        SchemaCompatibilityTestCase(
            test_name="case5",
            compatibility_mode="BACKWARD_TRANSITIVE",
            register_baseline_schemas=_register_baseline_schemas_with_incompatibilities_and_a_deleted_schema,
            new_schema=json.dumps(schema_int),
            expected_is_compatible=False,
            expected_status_code=200,
            expected_incompatibilities=["reader type: int not compatible with writer type: string"],
        ),
        # Case 6
        # A new schema and no baseline schemas
        # Non-transitive mode
        #   --> No incompatibilities are found
        #   --> Status code is 404 because `latest` version to check against does not exists
        SchemaCompatibilityTestCase(
            test_name="case6",
            compatibility_mode="BACKWARD",
            register_baseline_schemas=_register_no_baseline_schemas,
            new_schema=json.dumps(schema_int),
            expected_is_compatible=None,
            expected_status_code=404,
            expected_incompatibilities=None,
        ),
        # Case 7
        # Same as previous case, but in non-transitive mode
        #   --> No incompatibilities are found
        #   --> Status code is 404 because `latest` version to check against does not exists
        SchemaCompatibilityTestCase(
            test_name="case7",
            compatibility_mode="BACKWARD_TRANSITIVE",
            register_baseline_schemas=_register_no_baseline_schemas,
            new_schema=json.dumps(schema_int),
            expected_is_compatible=None,
            expected_status_code=404,
            expected_incompatibilities=None,
        ),
    ],
)
async def test_schema_compatibility(test_case: SchemaCompatibilityTestCase, registry_async_client: Client) -> None:
    subject = Subject(f"subject_{test_case.test_name}")

    await test_case.register_baseline_schemas(registry_async_client, subject)
    await _set_compatibility_mode(registry_async_client, subject, test_case.compatibility_mode)

    LOG.info("Validating new schema: %s", test_case.new_schema)
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest", json={"schema": test_case.new_schema}
    )

    assert res.status_code == test_case.expected_status_code
    assert res.json().get("is_compatible") == test_case.expected_is_compatible
    assert res.json().get("messages") == test_case.expected_incompatibilities
