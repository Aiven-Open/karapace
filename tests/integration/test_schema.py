"""
karapace - schema tests

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from http import HTTPStatus
from kafka import KafkaProducer
from karapace.client import Client
from karapace.rapu import is_success
from karapace.schema_registry_apis import SchemaErrorMessages
from karapace.utils import json_encode
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import (
    create_field_name_factory,
    create_schema_name_factory,
    create_subject_name_factory,
    repeat_until_successful_request,
)
from typing import List, Tuple

import json
import os
import pytest
import requests

baseurl = "http://localhost:8081"


@pytest.mark.parametrize("trail", ["", "/"])
async def test_union_to_union(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_union_to_union-{trail}")

    subject_1 = subject_name_factory()
    res = await registry_async_client.put(f"config/{subject_1}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    init_schema = {"name": "init", "type": "record", "fields": [{"name": "inner", "type": ["string", "int"]}]}
    evolved = {"name": "init", "type": "record", "fields": [{"name": "inner", "type": ["null", "string"]}]}
    evolved_compatible = {
        "name": "init",
        "type": "record",
        "fields": [
            {
                "name": "inner",
                "type": [
                    "int",
                    "string",
                    {"type": "record", "name": "foobar_fields", "fields": [{"name": "foo", "type": "string"}]},
                ],
            }
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject_1}/versions{trail}", json={"schema": json.dumps(init_schema)})
    assert res.status_code == 200
    assert "id" in res.json()
    res = await registry_async_client.post(f"subjects/{subject_1}/versions{trail}", json={"schema": json.dumps(evolved)})
    assert res.status_code == 409
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions{trail}", json={"schema": json.dumps(evolved_compatible)}
    )
    assert res.status_code == 200
    # fw compat check
    subject_2 = subject_name_factory()
    res = await registry_async_client.put(f"config/{subject_2}{trail}", json={"compatibility": "FORWARD"})
    assert res.status_code == 200
    res = await registry_async_client.post(
        f"subjects/{subject_2}/versions{trail}", json={"schema": json.dumps(evolved_compatible)}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    res = await registry_async_client.post(f"subjects/{subject_2}/versions{trail}", json={"schema": json.dumps(evolved)})
    assert res.status_code == 409
    res = await registry_async_client.post(f"subjects/{subject_2}/versions{trail}", json={"schema": json.dumps(init_schema)})
    assert res.status_code == 200


@pytest.mark.parametrize("trail", ["", "/"])
async def test_missing_subject_compatibility(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_missing_subject_compatibility-{trail}")()

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": json.dumps({"type": "string"})}
    )
    assert res.status_code == 200, f"{res} {subject}"
    res = await registry_async_client.get(f"config/{subject}{trail}")
    assert res.status_code == 404, f"{res} {subject}"
    res = await registry_async_client.get(f"config/{subject}{trail}?defaultToGlobal=false")
    assert res.status_code == 404, f"subject should have no compatibility when not defaulting to global: {res.json()}"
    res = await registry_async_client.get(f"config/{subject}{trail}?defaultToGlobal=true")
    assert res.status_code == 200, f"subject should have a compatibility when not defaulting to global: {res.json()}"

    assert "compatibilityLevel" in res.json(), res.json()


async def test_subject_allowed_chars(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("test_subject_allowed_chars-")()

    for suffix in ['"', "{", ":", "}", "'"]:
        subject = f"{subject_prefix}{suffix}"
        res = await registry_async_client.post(
            f"subjects/{subject}/versions", json={"schema": json.dumps({"type": "string"})}
        )
        assert res.status_code == 200, f"{res} {subject}"


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_union_schema_compatibility(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_record_union_schema_compatibility-{trail}")()

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    original_schema = {
        "name": "bar",
        "namespace": "foo",
        "type": "record",
        "fields": [
            {
                "name": "foobar",
                "type": [
                    {
                        "type": "array",
                        "name": "foobar_items",
                        "items": {"type": "record", "name": "foobar_fields", "fields": [{"name": "foo", "type": "string"}]},
                    }
                ],
            }
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(original_schema)}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = {
        "name": "bar",
        "namespace": "foo",
        "type": "record",
        "fields": [
            {
                "name": "foobar",
                "type": [
                    {
                        "type": "array",
                        "name": "foobar_items",
                        "items": {
                            "type": "record",
                            "name": "foobar_fields",
                            "fields": [
                                {"name": "foo", "type": "string"},
                                {"name": "bar", "type": ["null", "string"], "default": None},
                            ],
                        },
                    }
                ],
            }
        ],
    }
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": json.dumps(evolved_schema)},
    )
    assert res.status_code == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(evolved_schema)}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    # Check that we can delete the field as well
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": json.dumps(original_schema)},
    )
    assert res.status_code == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(original_schema)}
    )
    assert res.status_code == 200
    assert "id" in res.json()


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_nested_schema_compatibility(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_record_nested_schema_compatibility-{trail}")()

    res = await registry_async_client.put("config", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    schema = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
            {
                "name": "nested_record_name",
                "type": {
                    "name": "first_name_record",
                    "type": "record",
                    "fields": [
                        {
                            "name": "first_name",
                            "type": "string",
                        },
                    ],
                },
            },
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    # change string to integer in the nested record, should fail
    schema["fields"][1]["type"]["fields"][0]["type"] = "int"
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 409


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_schema_subject_compatibility(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory("test_record_schema_subject_compatibility")()

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    original_schema = {
        "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
        "name": "user",
        "type": "record",
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(original_schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    evolved_schema = {
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"default": "green", "name": "color", "type": "string"},
        ],
        "name": "user",
        "type": "record",
    }
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": json.dumps(evolved_schema)},
    )
    assert res.status_code == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(evolved_schema)}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    result = {"id": 1, "schema": json.dumps(original_schema), "subject": subject, "version": 1}

    res = await registry_async_client.get(f"subjects/{subject}/versions/1")
    assert res.status_code == 200
    assert res.json() == result
    result = {"id": 2, "schema": json.dumps(evolved_schema), "subject": subject, "version": 2}

    res = await registry_async_client.get(f"subjects/{subject}/versions/latest")
    assert res.status_code == 200
    assert res.json() == result


@pytest.mark.parametrize("trail", ["", "/"])
async def test_compatibility_endpoint(registry_async_client: Client, trail: str) -> None:
    """
    Creates a subject with a schema.
    Calls compatibility/subjects/{subject}/versions/latest endpoint
    and checks it return is_compatible true for a compatible new schema
    and false for incompatible schema.
    """
    subject = create_subject_name_factory(f"test_compatibility_endpoint-{trail}")()
    schema_name = create_schema_name_factory(f"test_compatibility_endpoint_{trail}")()

    schema = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "name": "age",
                "type": "int",
            },
        ],
    }

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    # replace int with long
    schema["fields"] = [{"type": "long", "name": "age"}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": True}

    # replace int with string
    schema["fields"] = [{"type": "string", "name": "age"}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": False}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_regression_compatibility_should_not_give_internal_server_error_on_invalid_schema_type(
    registry_async_client: Client,
    trail: str,
) -> None:
    test_name = "test_regression_compatibility_should_not_give_internal_server_error_on_invalid_schema_type"
    subject = create_subject_name_factory(test_name)()
    schema_name = create_schema_name_factory(test_name)()

    schema = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "name": "age",
                "type": "int",
            },
        ],
    }

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200

    # replace int with long
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": json.dumps(schema), "schemaType": "AVROO"},
    )
    assert res.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert res.json()["error_code"] == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_compatibility_to_non_existent_schema_version_returns_404(registry_async_client: Client) -> None:
    """The subject must be found and the subject must not hold any schema versions.

    First create the subject and schema version, soft delete and hard delete to empty the subject.
    """
    test_name = "test_compatibility_to_non_existent_schema_version_returns_404"
    subject = create_subject_name_factory(test_name)()
    schema_name = create_schema_name_factory(test_name)()

    schema = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "name": "age",
                "type": "int",
            },
        ],
    }

    # Test compatibility returns 404 not found for non-existing subject
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/1",
        json={"schema": json.dumps(schema), "schemaType": "AVRO"},
    )
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200

    # Soft delete
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1")
    assert res.status_code == 200
    assert res.json() == 1

    # Check compatibility after subject has only soft-deleted version schemas
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/1",
        json={"schema": json.dumps(schema), "schemaType": "AVRO"},
    )
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402

    # Hard delete
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1?permanent=true")
    assert res.status_code == 200

    # Test compatibility returns 404 again
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/1",
        json={"schema": json.dumps(schema), "schemaType": "AVRO"},
    )
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402


@pytest.mark.parametrize("trail", ["", "/"])
async def test_regression_invalid_schema_type_should_not_give_internal_server_error(
    registry_async_client: Client,
    trail: str,
) -> None:
    test_name = "test_regression_invalid_schema_type_should_not_give_internal_server_error"
    subject = create_subject_name_factory(test_name)()
    schema_name = create_schema_name_factory(test_name)()

    schema = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "name": "age",
                "type": "int",
            },
        ],
    }

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema), "schemaType": "AVROO"},
    )
    assert res.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert res.json()["error_code"] == HTTPStatus.UNPROCESSABLE_ENTITY


@pytest.mark.parametrize("trail", ["", "/"])
async def test_type_compatibility(registry_async_client: Client, trail: str) -> None:
    def _test_cases():
        # Generate FORWARD, BACKWARD and FULL tests for primitive types
        _CONVERSIONS = {
            "int": {
                "int": (True, True),
                "long": (False, True),
                "float": (False, True),
                "double": (False, True),
            },
            "bytes": {
                "bytes": (True, True),
                "string": (True, True),
            },
            "boolean": {
                "boolean": (True, True),
            },
        }
        _INVALID_CONVERSIONS = [
            ("int", "boolean"),
            ("int", "string"),
            ("int", "bytes"),
            ("long", "boolean"),
            ("long", "string"),
            ("long", "bytes"),
            ("float", "boolean"),
            ("float", "string"),
            ("float", "bytes"),
            ("double", "boolean"),
            ("double", "string"),
            ("double", "bytes"),
        ]

        for source, targets in _CONVERSIONS.items():
            for target, (forward, backward) in targets.items():
                yield "FORWARD", source, target, forward
                yield "BACKWARD", source, target, backward
                yield "FULL", target, source, forward and backward
                if source != target:
                    yield "FORWARD", target, source, backward
                    yield "BACKWARD", target, source, forward
                    yield "FULL", source, target, forward and backward

        for source, target in _INVALID_CONVERSIONS:
            yield "FORWARD", source, target, False
            yield "FORWARD", target, source, False
            yield "BACKWARD", source, target, False
            yield "BACKWARD", target, source, False
            yield "FULL", target, source, False
            yield "FULL", source, target, False

    subject_name_factory = create_subject_name_factory(f"test_type_compatibility-{trail}")
    for compatibility, source_type, target_type, expected in _test_cases():
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        schema = {
            "type": "record",
            "name": "Objct",
            "fields": [
                {
                    "name": "field",
                    "type": source_type,
                },
            ],
        }
        res = await registry_async_client.post(
            f"subjects/{subject}/versions{trail}",
            json={"schema": json.dumps(schema)},
        )
        assert res.status_code == 200

        schema["fields"][0]["type"] = target_type
        res = await registry_async_client.post(
            f"compatibility/subjects/{subject}/versions/latest{trail}",
            json={"schema": json.dumps(schema)},
        )
        assert res.status_code == 200
        assert res.json() == {"is_compatible": expected}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_schema_compatibility_forward(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_record_schema_compatibility_forward_{trail}")
    subject = subject_name_factory()
    schema_name = create_schema_name_factory(f"test_record_schema_compatibility_forward_{trail}")()

    schema_1 = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema_1)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    res = await registry_async_client.put(f"/config/{subject}{trail}", json={"compatibility": "FORWARD"})
    assert res.status_code == 200

    schema_2 = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "age", "type": "int"},
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema_2)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schema_id2 = res.json()["id"]
    assert schema_id != schema_id2

    schema_3a = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "last_name", "type": "string"},
            {"name": "third_name", "type": "string", "default": "foodefaultvalue"},
            {"name": "age", "type": "int"},
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema_3a)},
    )
    # Fails because field removed
    assert res.status_code == 409
    res_json = res.json()
    assert res_json["error_code"] == 409

    schema_3b = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "age", "type": "long"},
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema_3b)},
    )
    # Fails because incompatible type change
    assert res.status_code == 409
    res_json = res.json()
    assert res_json["error_code"] == 409

    schema_4 = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "third_name", "type": "string", "default": "foodefaultvalue"},
            {"name": "age", "type": "int"},
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": json.dumps(schema_4)},
    )
    assert res.status_code == 200


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_schema_compatibility_backward(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_record_schema_compatibility_backward_{trail}")
    subject_1 = subject_name_factory()
    schema_name = create_schema_name_factory(f"test_record_schema_compatibility_backward_{trail}")()

    schema_1 = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "third_name", "type": "string", "default": "foodefaultvalue"},
            {"name": "age", "type": "int"},
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions{trail}",
        json={"schema": json.dumps(schema_1)},
    )
    assert res.status_code == 200

    res = await registry_async_client.put(f"config/{subject_1}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    # adds fourth_name w/o default, invalid
    schema_2 = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "third_name", "type": "string", "default": "foodefaultvalue"},
            {"name": "fourth_name", "type": "string"},
            {"name": "age", "type": "int"},
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions{trail}",
        json={"schema": json.dumps(schema_2)},
    )
    assert res.status_code == 409

    # Add a default value for the field
    schema_2["fields"][3] = {"name": "fourth_name", "type": "string", "default": "foof"}
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions{trail}",
        json={"schema": json.dumps(schema_2)},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    # Try to submit schema with a different definition
    schema_2["fields"][3] = {"name": "fourth_name", "type": "int", "default": 2}
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions{trail}",
        json={"schema": json.dumps(schema_2)},
    )
    assert res.status_code == 409

    subject_2 = subject_name_factory()
    res = await registry_async_client.put(f"config/{subject_2}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    schema_1 = {"type": "record", "name": schema_name, "fields": [{"name": "first_name", "type": "string"}]}
    res = await registry_async_client.post(f"subjects/{subject_2}/versions{trail}", json={"schema": json.dumps(schema_1)})
    assert res.status_code == 200
    schema_1["fields"].append({"name": "last_name", "type": "string"})
    res = await registry_async_client.post(f"subjects/{subject_2}/versions{trail}", json={"schema": json.dumps(schema_1)})
    assert res.status_code == 409


@pytest.mark.parametrize("trail", ["", "/"])
async def test_enum_schema_field_add_compatibility(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_enum_schema_field_add_compatibility-{trail}")
    expected_results = [("BACKWARD", 200), ("FORWARD", 200), ("FULL", 200)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status_code == 200
        schema = {"type": "enum", "name": "Suit", "symbols": ["SPADES", "HEARTS", "DIAMONDS"]}
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Add a field
        schema["symbols"].append("CLUBS")
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == status_code


@pytest.mark.parametrize("trail", ["", "/"])
async def test_array_schema_field_add_compatibility(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_array_schema_field_add_compatibility-{trail}")
    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status_code == 200
        schema = {"type": "array", "items": "int"}
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Modify the items type
        schema["items"] = "long"
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == status_code


@pytest.mark.parametrize("trail", ["", "/"])
async def test_array_nested_record_compatibility(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_array_nested_record_compatibility-{trail}")
    expected_results = [("BACKWARD", 409), ("FORWARD", 200), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status_code == 200
        schema = {
            "type": "array",
            "items": {"type": "record", "name": "object", "fields": [{"name": "first_name", "type": "string"}]},
        }
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Add a second field to the record
        schema["items"]["fields"].append({"name": "last_name", "type": "string"})
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == status_code


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_nested_array_compatibility(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_record_nested_array_compatibility-{trail}")
    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status_code == 200
        schema = {
            "type": "record",
            "name": "object",
            "fields": [{"name": "simplearray", "type": {"type": "array", "items": "int"}}],
        }
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Modify the array items type
        schema["fields"][0]["type"]["items"] = "long"
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": json.dumps(schema)})
        assert res.status_code == status_code


async def test_map_schema_field_add_compatibility(
    registry_async_client: Client,
) -> None:  # TODO: Rename to pålain check map schema and add additional steps
    subject_name_factory = create_subject_name_factory("test_map_schema_field_add_compatibility")
    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status_code == 200
        schema = {"type": "map", "values": "int"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Modify the items type
        schema["values"] = "long"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == status_code


async def test_enum_schema(registry_async_client: Client) -> None:
    subject_name_factory = create_subject_name_factory("test_enum_schema")
    for compatibility in ["BACKWARD", "FORWARD", "FULL"]:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status_code == 200
        schema = {"type": "enum", "name": "testenum", "symbols": ["first"]}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})

        # Add a symbol.
        schema["symbols"].append("second")
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Remove a symbol
        schema["symbols"].pop(1)
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Change the name
        schema["name"] = "another"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 409

        # Inside record
        subject = subject_name_factory()
        schema = {
            "type": "record",
            "name": "object",
            "fields": [{"name": "enumkey", "type": {"type": "enum", "name": "testenum", "symbols": ["first"]}}],
        }
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})

        # Add a symbol.
        schema["fields"][0]["type"]["symbols"].append("second")
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Remove a symbol
        schema["fields"][0]["type"]["symbols"].pop(1)
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200

        # Change the name
        schema["fields"][0]["type"]["name"] = "another"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 409


@pytest.mark.parametrize("compatibility", ["BACKWARD", "FORWARD", "FULL"])
async def test_fixed_schema(registry_async_client: Client, compatibility: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_fixed_schema-{compatibility}")
    status_code_allowed = 200
    status_code_denied = 409
    subject_1 = subject_name_factory()
    res = await registry_async_client.put(f"config/{subject_1}", json={"compatibility": compatibility})
    assert res.status_code == 200
    schema = {"type": "fixed", "size": 16, "name": "md5", "aliases": ["testalias"]}
    res = await registry_async_client.post(f"subjects/{subject_1}/versions", json={"schema": json.dumps(schema)})

    # Add new alias
    schema["aliases"].append("anotheralias")
    res = await registry_async_client.post(f"subjects/{subject_1}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == status_code_allowed

    # Try to change size
    schema["size"] = 32
    res = await registry_async_client.post(f"subjects/{subject_1}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == status_code_denied

    # Try to change name
    schema["size"] = 16
    schema["name"] = "denied"
    res = await registry_async_client.post(f"subjects/{subject_1}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == status_code_denied

    # In a record
    subject_2 = subject_name_factory()
    schema = {
        "type": "record",
        "name": "object",
        "fields": [{"name": "fixedkey", "type": {"type": "fixed", "size": 16, "name": "md5", "aliases": ["testalias"]}}],
    }
    res = await registry_async_client.post(f"subjects/{subject_2}/versions", json={"schema": json.dumps(schema)})

    # Add new alias
    schema["fields"][0]["type"]["aliases"].append("anotheralias")
    res = await registry_async_client.post(f"subjects/{subject_2}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == status_code_allowed

    # Try to change size
    schema["fields"][0]["type"]["size"] = 32
    res = await registry_async_client.post(f"subjects/{subject_2}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == status_code_denied

    # Try to change name
    schema["fields"][0]["type"]["size"] = 16
    schema["fields"][0]["type"]["name"] = "denied"
    res = await registry_async_client.post(f"subjects/{subject_2}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == status_code_denied


async def test_primitive_schema(registry_async_client: Client) -> None:
    subject_name_factory = create_subject_name_factory("test_primitive_schema")
    expected_results = [("BACKWARD", 200), ("FORWARD", 200), ("FULL", 200)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status_code == 200

        # Transition from string to bytes
        schema = {"type": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200
        schema["type"] = "bytes"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == status_code

    expected_results = [("BACKWARD", 409), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status_code == 200

        # Transition from string to int
        schema = {"type": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200
        schema["type"] = "int"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})


async def test_union_comparing_to_other_types(registry_async_client: Client) -> None:
    subject_name_factory = create_subject_name_factory("test_primitive_schema")
    expected_results = [("BACKWARD", 409), ("FORWARD", 200), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status_code == 200

        # Union vs non-union with the same schema
        schema = [{"type": "array", "name": "listofstrings", "items": "string"}, "string"]
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200
        plain_schema = {"type": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(plain_schema)})
        assert res.status_code == status_code

    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status_code == 200

        # Non-union first
        schema = {"type": "array", "name": "listofstrings", "items": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200
        union_schema = [{"type": "array", "name": "listofstrings", "items": "string"}, "string"]
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(union_schema)})
        assert res.status_code == status_code

    expected_results = [("BACKWARD", 409), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = subject_name_factory()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status_code == 200

        # Union to a completely different schema
        schema = [{"type": "array", "name": "listofstrings", "items": "string"}, "string"]
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
        assert res.status_code == 200
        plain_wrong_schema = {"type": "int"}
        res = await registry_async_client.post(
            f"subjects/{subject}/versions", json={"schema": json.dumps(plain_wrong_schema)}
        )
        assert res.status_code == status_code


async def test_transitive_compatibility(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_transitive_compatibility")()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD_TRANSITIVE"})
    assert res.status_code == 200

    schema0 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {"name": "age", "type": "int"},
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema0)},
    )
    assert res.status_code == 200

    schema1 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {"name": "age", "type": "int"},
            {
                "name": "first_name",
                "type": "string",
                "default": "John",
            },
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema1)},
    )
    assert res.status_code == 200

    schema2 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {"name": "age", "type": "int"},
            {
                "name": "first_name",
                "type": "string",
            },
            {
                "name": "last_name",
                "type": "string",
                "default": "Doe",
            },
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema2)},
    )
    assert res.status_code == 409
    res_json = res.json()
    assert res_json["error_code"] == 409


async def assert_schema_versions(client: Client, trail: str, schema_id: int, expected: List[Tuple[str, int]]) -> None:
    """
    Calls /schemas/ids/{schema_id}/versions and asserts the expected results were in the response.
    """
    res = await client.get(f"/schemas/ids/{schema_id}/versions{trail}")
    assert res.status_code == 200
    registered_schemas = res.json()

    # Schema Registry doesn't return an ordered list, Karapace does.
    # Need to check equality ignoring ordering.
    result = [(schema["subject"], schema["version"]) for schema in registered_schemas]
    assert set(result) == set(expected)


async def assert_schema_versions_failed(client: Client, trail: str, schema_id: int, response_code: int = 404) -> None:
    """
    Calls /schemas/ids/{schema_id}/versions and asserts the response code is the expected.
    """
    res = await client.get(f"/schemas/ids/{schema_id}/versions{trail}")
    assert res.status_code == response_code


async def register_schema(registry_async_client: Client, trail, subject: str, schema_str: str) -> Tuple[int, int]:
    # Register to get the id
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": schema_str},
    )
    assert res.status_code == 200
    schema_id = res.json()["id"]

    # Get version
    res = await registry_async_client.post(
        f"subjects/{subject}{trail}",
        json={"schema": schema_str},
    )
    assert res.status_code == 200
    assert res.json()["id"] == schema_id
    return schema_id, res.json()["version"]


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_versions_multiple_subjects_same_schema(registry_async_client: Client, trail: str) -> None:
    """
    Tests case where there are multiple subjects with the same schema.
    The schema/versions endpoint returns all these subjects.
    """
    subject_name_factory = create_subject_name_factory(f"test_schema_versions_multiple_subjects_same_schema-{trail}")
    schema_name_factory = create_schema_name_factory(f"test_schema_versions_multiple_subjects_same_schema_{trail}")

    schema_1 = {
        "type": "record",
        "name": schema_name_factory(),
        "fields": [
            {
                "name": "f1",
                "type": "string",
            },
            {
                "name": "f2",
                "type": "string",
            },
        ],
    }
    schema_str_1 = json.dumps(schema_1)
    schema_2 = {
        "type": "record",
        "name": schema_name_factory(),
        "fields": [
            {
                "name": "f1",
                "type": "string",
            }
        ],
    }
    schema_str_2 = json.dumps(schema_2)

    subject_1 = subject_name_factory()
    schema_id_1, version_1 = await register_schema(registry_async_client, trail, subject_1, schema_str_1)
    schema_1_versions = [(subject_1, version_1)]
    await assert_schema_versions(registry_async_client, trail, schema_id_1, schema_1_versions)

    subject_2 = subject_name_factory()
    schema_id_2, version_2 = await register_schema(registry_async_client, trail, subject_2, schema_str_1)
    schema_1_versions = [(subject_1, version_1), (subject_2, version_2)]
    assert schema_id_1 == schema_id_2
    await assert_schema_versions(registry_async_client, trail, schema_id_1, schema_1_versions)

    subject_3 = subject_name_factory()
    schema_id_3, version_3 = await register_schema(registry_async_client, trail, subject_3, schema_str_1)
    schema_1_versions = [(subject_1, version_1), (subject_2, version_2), (subject_3, version_3)]
    assert schema_id_1 == schema_id_3
    await assert_schema_versions(registry_async_client, trail, schema_id_1, schema_1_versions)

    # subject_4 with different schema to check there are no side effects
    subject_4 = subject_name_factory()
    schema_id_4, version_4 = await register_schema(registry_async_client, trail, subject_4, schema_str_2)
    schema_2_versions = [(subject_4, version_4)]
    assert schema_id_1 != schema_id_4
    await assert_schema_versions(registry_async_client, trail, schema_id_1, schema_1_versions)
    await assert_schema_versions(registry_async_client, trail, schema_id_4, schema_2_versions)


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_versions_deleting(registry_async_client: Client, trail: str) -> None:
    """
    Tests getting schema versions when removing a schema version and eventually the subject.
    """
    subject = create_subject_name_factory(f"test_schema_versions_deleting_{trail}")()
    schema_name = create_schema_name_factory(f"test_schema_versions_deleting_{trail}")()

    schema_1 = {
        "type": "record",
        "name": schema_name,
        "fields": [{"name": "field_1", "type": "string"}, {"name": "field_2", "type": "string"}],
    }
    schema_str_1 = json.dumps(schema_1)
    schema_2 = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "field_1", "type": "string"},
        ],
    }
    schema_str_2 = json.dumps(schema_2)

    schema_id_1, version_1 = await register_schema(registry_async_client, trail, subject, schema_str_1)
    schema_1_versions = [(subject, version_1)]
    await assert_schema_versions(registry_async_client, trail, schema_id_1, schema_1_versions)

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    schema_id_2, version_2 = await register_schema(registry_async_client, trail, subject, schema_str_2)
    schema_2_versions = [(subject, version_2)]
    await assert_schema_versions(registry_async_client, trail, schema_id_2, schema_2_versions)

    # Deleting one version, the other still found
    res = await registry_async_client.delete("subjects/{}/versions/{}".format(subject, version_1))
    assert res.status_code == 200
    assert res.json() == version_1

    await assert_schema_versions(registry_async_client, trail, schema_id_1, [])
    await assert_schema_versions(registry_async_client, trail, schema_id_2, schema_2_versions)

    # Deleting the subject, the schema version 2 cannot be found anymore
    res = await registry_async_client.delete("subjects/{}".format(subject))
    assert res.status_code == 200
    assert res.json() == [version_2]

    await assert_schema_versions(registry_async_client, trail, schema_id_1, [])
    await assert_schema_versions(registry_async_client, trail, schema_id_2, [])


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_delete_latest_version(registry_async_client: Client, trail: str) -> None:
    """
    Tests deleting schema with `latest` version.
    """
    subject = create_subject_name_factory(f"test_schema_delete_latest_version_{trail}")()
    schema_name = create_schema_name_factory(f"test_schema_delete_latest_version_{trail}")()

    schema_1 = {
        "type": "record",
        "name": schema_name,
        "fields": [{"name": "field_1", "type": "string"}, {"name": "field_2", "type": "string"}],
    }
    schema_str_1 = json.dumps(schema_1)
    schema_2 = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {"name": "field_1", "type": "string"},
        ],
    }
    schema_str_2 = json.dumps(schema_2)

    schema_id_1, version_1 = await register_schema(registry_async_client, trail, subject, schema_str_1)
    schema_1_versions = [(subject, version_1)]
    await assert_schema_versions(registry_async_client, trail, schema_id_1, schema_1_versions)

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    schema_id_2, version_2 = await register_schema(registry_async_client, trail, subject, schema_str_2)
    schema_2_versions = [(subject, version_2)]
    await assert_schema_versions(registry_async_client, trail, schema_id_2, schema_2_versions)

    # Deleting latest version, the other still found
    res = await registry_async_client.delete("subjects/{}/versions/latest".format(subject))
    assert res.status_code == 200
    assert res.json() == version_2

    await assert_schema_versions(registry_async_client, trail, schema_id_1, schema_1_versions)
    await assert_schema_versions(registry_async_client, trail, schema_id_2, [])

    # Deleting the latest version, no schemas left
    res = await registry_async_client.delete("subjects/{}/versions/latest".format(subject))
    assert res.status_code == 200
    assert res.json() == version_1

    await assert_schema_versions(registry_async_client, trail, schema_id_1, [])
    await assert_schema_versions(registry_async_client, trail, schema_id_2, [])


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_types(registry_async_client: Client, trail: str) -> None:
    """
    Tests for /schemas/types endpoint.
    """
    res = await registry_async_client.get(f"/schemas/types{trail}")
    assert res.status_code == 200
    json_res = res.json()
    assert len(json_res) == 3
    assert "AVRO" in json_res
    assert "JSON" in json_res
    assert "PROTOBUF" in json_res


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_repost(registry_async_client: Client, trail: str) -> None:
    """ "
    Repost same schema again to see that a new id is not generated but an old one is given back
    """
    subject = create_subject_name_factory(f"test_schema_repost-{trail}")()
    unique_field_factory = create_field_name_factory(trail)

    unique = unique_field_factory()
    schema_str = json.dumps({"type": "string", "unique": unique})
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": schema_str},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    res = await registry_async_client.get(f"schemas/ids/{schema_id}{trail}")
    assert res.status_code == 200
    assert json.loads(res.json()["schema"]) == json.loads(schema_str)

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": schema_str},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"]


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_missing_body(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_schema_missing_body-{trail}")()

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={},
    )
    assert res.status_code == 422
    assert res.json()["error_code"] == 42201
    assert res.json()["message"] == "Empty schema"


async def test_schema_non_existing_id(registry_async_client: Client) -> None:
    """
    Tests getting a non-existing schema id
    """
    result = await registry_async_client.get(os.path.join("schemas/ids/123456789"))
    assert result.json()["error_code"] == 40403


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_non_invalid_id(registry_async_client: Client, trail: str) -> None:
    """
    Tests getting an invalid schema id
    """
    result = await registry_async_client.get(f"schemas/ids/invalid{trail}")
    assert result.status_code == 404
    assert result.json()["error_code"] == 404
    assert result.json()["message"] == "HTTP 404 Not Found"


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_subject_invalid_id(registry_async_client: Client, trail: str) -> None:
    """
    Creates a subject with a schema and trying to find the invalid versions for the subject.
    """
    subject = create_subject_name_factory(f"test_schema_subject_invalid_id-{trail}")()
    unique_field_factory = create_field_name_factory(trail)

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": '{"type": "string", "foo": "string", "%s": "string"}' % unique_field_factory()},
    )
    assert res.status_code == 200

    # Find an invalid version 0
    res = await registry_async_client.get(f"subjects/{subject}/versions/0")
    assert res.status_code == 422
    assert res.json()["error_code"] == 42202
    assert (
        res.json()["message"]
        == "The specified version '0' is not a valid version id. "
        + 'Allowed values are between [1, 2^31-1] and the string "latest"'
    )

    # Find an invalid version (too large)
    res = await registry_async_client.get(f"subjects/{subject}/versions/15")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version 15 not found."


async def test_schema_subject_post_invalid(registry_async_client: Client) -> None:
    """
    Tests posting to /subjects/{subject} with different invalid values.
    """
    subject_name_factory = create_subject_name_factory("test_schema_subject_post_invalid")

    schema_str = json.dumps({"type": "string"})

    # Create the subject
    subject_1 = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions",
        json={"schema": schema_str},
    )
    assert res.status_code == 200

    res = await registry_async_client.post(
        f"subjects/{subject_1}",
        json={"schema": json.dumps({"type": "invalid_type"})},
    )
    assert res.status_code == 500, "Invalid schema for existing subject should return 500"
    assert res.json()["message"] == f"Error while looking up schema under subject {subject_1}"

    # Subject is not found
    subject_2 = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject_2}",
        json={"schema": schema_str},
    )
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject_2}' not found."

    # Schema not found for subject
    res = await registry_async_client.post(
        f"subjects/{subject_1}",
        json={"schema": '{"type": "int"}'},
    )
    assert res.status_code == 404
    assert res.json()["error_code"] == 40403
    assert res.json()["message"] == "Schema not found"

    # Schema not included in the request body
    res = await registry_async_client.post(f"subjects/{subject_1}", json={})
    assert res.status_code == 500
    assert res.json()["error_code"] == 500
    assert res.json()["message"] == f"Error while looking up schema under subject {subject_1}"

    # Schema not included in the request body for subject that does not exist
    subject_3 = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject_3}",
        json={},
    )
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject_3}' not found."


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_lifecycle(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_schema_lifecycle-{trail}")()
    unique_field_factory = create_field_name_factory(trail)

    unique_1 = unique_field_factory()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps({"type": "string", "foo": "string", unique_1: "string"})},
    )
    assert res.status_code == 200
    schema_id_1 = res.json()["id"]

    unique_2 = unique_field_factory()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps({"type": "string", "foo": "string", unique_2: "string"})},
    )
    schema_id_2 = res.json()["id"]
    assert res.status_code == 200
    assert schema_id_1 != schema_id_2

    await assert_schema_versions(registry_async_client, trail, schema_id_1, [(subject, 1)])
    await assert_schema_versions(registry_async_client, trail, schema_id_2, [(subject, 2)])

    result = await registry_async_client.get(os.path.join(f"schemas/ids/{schema_id_1}"))
    schema_json_1 = json.loads(result.json()["schema"])
    assert schema_json_1["type"] == "string"
    assert schema_json_1["foo"] == "string"
    assert schema_json_1[unique_1] == "string"

    result = await registry_async_client.get(os.path.join(f"schemas/ids/{schema_id_2}"))
    schema_json_2 = json.loads(result.json()["schema"])
    assert schema_json_2["type"] == "string"
    assert schema_json_2["foo"] == "string"
    assert schema_json_2[unique_2] == "string"

    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject in res.json()

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1, 2]

    res = await registry_async_client.get(f"subjects/{subject}/versions/1")
    assert res.status_code == 200
    assert res.json()["subject"] == subject
    assert json.loads(res.json()["schema"]) == schema_json_1

    # Delete an actual version
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1")
    assert res.status_code == 200
    assert res.json() == 1

    # Get the schema by id, still there, wasn't hard-deleted
    res = await registry_async_client.get(f"schemas/ids/{schema_id_1}{trail}")
    assert res.status_code == 200
    assert json.loads(res.json()["schema"]) == schema_json_1

    # Get the schema by id
    res = await registry_async_client.get(f"schemas/ids/{schema_id_2}{trail}")
    assert res.status_code == 200

    # Get the versions, old version not found anymore (even if schema itself is)
    await assert_schema_versions(registry_async_client, trail, schema_id_1, [])
    await assert_schema_versions(registry_async_client, trail, schema_id_2, [(subject, 2)])

    # Delete a whole subject
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 200
    assert res.json() == [2]

    # List all subjects, our subject shouldn't be in the list
    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject not in res.json()

    # After deleting the last version of a subject, it shouldn't be in the list
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": '{"type": "string", "unique": "%s"}' % unique_field_factory()},
    )
    assert res.status_code == 200
    res = await registry_async_client.get("subjects")
    assert subject in res.json()
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.json() == [3]
    res = await registry_async_client.delete(f"subjects/{subject}/versions/3")
    assert res.status_code == 200
    res = await registry_async_client.get("subjects")
    assert subject not in res.json()

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."
    res = await registry_async_client.get(f"subjects/{subject}/versions/latest")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    # Creating a new schema works after deleting the only available version
    unique_3 = unique_field_factory()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps({"type": "string", "foo": "string", unique_3: "string"})},
    )
    assert res.status_code == 200
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.json() == [4]


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_version_numbering(registry_async_client: Client, trail: str) -> None:
    """
    Test updating the schema of a subject increases its version number.
    Deletes the subjects and asserts that when recreated, has a greater version number.
    """
    subject = create_subject_name_factory(f"test_schema_version_numbering-{trail}")()
    unique_field_factory = create_field_name_factory(trail)

    unique = unique_field_factory()
    schema = {
        "type": "record",
        "name": unique,
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            }
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "FORWARD"})
    assert res.status_code == 200

    schema2 = {
        "type": "record",
        "name": unique,
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
            {
                "name": "last_name",
                "type": "string",
            },
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema2)})
    assert res.status_code == 200
    assert "id" in res.json()
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1, 2]

    # Recreate subject
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 200
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == 200
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [3]  # Version number generation should now begin at 3


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_version_numbering_complex(registry_async_client: Client, trail: str) -> None:
    """
    Tests that when fetching a more complex schema, it matches with the created one.
    """
    subject = create_subject_name_factory(f"test_schema_version_numbering_complex-{trail}")()
    unique_field_factory = create_field_name_factory(trail)

    schema = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
        ],
        "unique": unique_field_factory(),
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    schema_id = res.json()["id"]

    res = await registry_async_client.get(f"subjects/{subject}/versions/1")
    assert res.status_code == 200
    assert res.json()["subject"] == subject
    assert sorted(json.loads(res.json()["schema"])) == sorted(schema)

    await assert_schema_versions(registry_async_client, trail, schema_id, [(subject, 1)])


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_three_subjects_sharing_schema(registry_async_client: Client, trail: str) -> None:
    """ "
    Submits two subjects with the same schema.
    Submits a third subject initially with different schema. Updates to share the schema.
    Asserts all three subjects have the same schema.
    """
    subject_name_factory = create_subject_name_factory(f"test_schema_XXX-{trail}")
    unique_field_factory = create_field_name_factory(trail)

    # Submitting the exact same schema for a different subject should return the same schema ID.
    subject_1 = subject_name_factory()
    schema = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value",
                "type": "string",
            },
            {
                "name": unique_field_factory(),
                "type": "string",
            },
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject_1}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == 200
    assert "id" in res.json()
    schema_id_1 = res.json()["id"]

    # New subject with the same schema
    subject_2 = subject_name_factory()
    res = await registry_async_client.post(f"subjects/{subject_2}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == 200
    assert "id" in res.json()
    schema_id_2 = res.json()["id"]
    assert schema_id_1 == schema_id_2

    # It also works for multiple versions in a single subject
    subject_3 = subject_name_factory()
    res = await registry_async_client.put(
        f"config/{subject_3}", json={"compatibility": "NONE"}
    )  # We don't care about the compatibility in this test
    res = await registry_async_client.post(
        f"subjects/{subject_3}/versions",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    res = await registry_async_client.post(
        f"subjects/{subject_3}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert res.json()["id"] == schema_id_1  # Same ID as in the previous test step


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_subject_version_schema(registry_async_client: Client, trail: str) -> None:
    """
    Tests for the /subjects/(string: subject)/versions/(versionId: version)/schema endpoint.
    """
    subject_name_factory = create_subject_name_factory(f"test_schema_subject_version_schema_{trail}")
    schema_name = create_schema_name_factory(f"test_schema_subject_version_schema_{trail}")()

    # The subject version schema endpoint returns the correct results
    subject_1 = subject_name_factory()

    schema = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "name": "just_a_value",
                "type": "string",
            }
        ],
    }
    schema_str = json.dumps(schema)

    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions",
        json={"schema": schema_str},
    )
    assert res.status_code == 200
    res = await registry_async_client.get(f"subjects/{subject_1}/versions/1/schema")
    assert res.status_code == 200
    assert res.json() == json.loads(schema_str)

    subject_2 = subject_name_factory()
    res = await registry_async_client.get(f"subjects/{subject_2}/versions/1/schema")  # Invalid subject
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject_2}' not found."

    res = await registry_async_client.get(f"subjects/{subject_1}/versions/2/schema")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version 2 not found."

    res = await registry_async_client.get(f"subjects/{subject_1}/versions/latest/schema")
    assert res.status_code == 200
    assert res.json() == json.loads(schema_str)


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_same_subject(registry_async_client: Client, trail: str) -> None:
    """
    The same schema JSON should be returned when checking the same schema str against the same subject
    """
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
    schema_id = res.json()["id"]
    res = await registry_async_client.post(
        f"subjects/{subject}",
        json={"schema": schema_str},
    )
    assert res.status_code == 200

    # Switch the str schema to a dict for comparison
    json_res = res.json()
    json_res["schema"] = json.loads(json_res["schema"])
    assert json_res == {"id": schema_id, "subject": subject, "schema": json.loads(schema_str), "version": 1}


async def test_schema_same_subject_unnamed(registry_async_client: Client) -> None:
    """
    The same schema JSON should be returned when checking the same schema str against the same subject
    """
    subject_name_factory = create_subject_name_factory("test_schema_same_subject_unnamed")
    schema_name = create_schema_name_factory("test_schema_same_subject_unnamed")()

    schema_str = json.dumps(
        {
            "type": "int",
            "name": schema_name,
        }
    )
    subject = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": schema_str},
    )
    assert res.status_code == 200
    schema_id = res.json()["id"]

    unnamed_schema_str = json.dumps({"type": "int"})

    res = await registry_async_client.post(
        f"subjects/{subject}",
        json={"schema": unnamed_schema_str},
    )
    assert res.status_code == 200

    # Switch the str schema to a dict for comparison
    json_res = res.json()
    json_res["schema"] = json.loads(json_res["schema"])
    assert json_res == {"id": schema_id, "subject": subject, "schema": json.loads(schema_str), "version": 1}


async def test_schema_json_subject_comparison(registry_async_client: Client) -> None:
    """
    The same schema JSON should be returned when checking the same schema against the same subject
    """
    subject_name_factory = create_subject_name_factory("test_schema_json_subject_comparison")

    schema_1 = {"schemaType": "JSON", "schema": '{"type": "string", "value": "int"}'}
    subject = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json=schema_1,
    )
    assert res.status_code == 200
    schema_id = res.json()["id"]

    schema_2 = {"schemaType": "JSON", "schema": '{"type": "string", "value": "int"}'}

    res = await registry_async_client.post(
        f"subjects/{subject}",
        json=schema_2,
    )
    assert res.status_code == 200

    schema_3 = {"schemaType": "JSON", "schema": '{"value": "int", "type": "string"}'}
    res = await registry_async_client.post(
        f"subjects/{subject}",
        json=schema_3,
    )
    assert res.status_code == 200

    json_res = res.json()
    json_res["schema"] = json.loads(json_res["schema"])
    assert json_res == {
        "id": schema_id,
        "schema": json.loads(schema_1["schema"]),
        "subject": subject,
        "schemaType": "JSON",
        "version": 1,
    }


async def test_schema_listing(registry_async_client: Client) -> None:
    subject_name_factory = create_subject_name_factory("test_schema_listing_subject_")
    schema_name = create_schema_name_factory("test_schema_listing_subject_")()

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
    subject_1 = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions",
        json={"schema": schema_str},
    )
    assert res.status_code == 200

    subject_2 = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject_2}/versions",
        json={"schema": schema_str},
    )
    assert res.status_code == 200
    schema_id_2 = res.json()["id"]

    # Soft delete schema 2
    res = await registry_async_client.delete(f"subjects/{subject_2}/versions/{schema_id_2}")
    assert res.status_code == 200
    assert res.json() == 1

    res = await registry_async_client.get("subjects/")
    assert len(res.json()) == 1
    assert res.json()[0] == subject_1

    res = await registry_async_client.get("subjects/?deleted=true")
    assert len(res.json()) == 2
    assert res.json()[0] == subject_1
    assert res.json()[1] == subject_2


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema_version_number_existing_schema(registry_async_client: Client, trail: str) -> None:
    """
    Tests creating the same schemas for two subjects. Asserts the schema ids are the same for both subjects.
    """
    subject_name_factory = create_subject_name_factory(f"test_schema_version_number_existing_schema-{trail}")
    unique_field_factory = create_field_name_factory(trail)

    subject_1 = subject_name_factory()
    res = await registry_async_client.put(
        f"config/{subject_1}", json={"compatibility": "NONE"}
    )  # We don't care about compatibility
    unique = unique_field_factory()
    schema_1 = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value",
                "type": "string",
            },
            {
                "name": f"{unique}",
                "type": "string",
            },
        ],
    }
    schema_2 = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value2",
                "type": "string",
            },
            {
                "name": f"{unique}",
                "type": "string",
            },
        ],
    }
    schema_3 = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value3",
                "type": "int",
            },
            {
                "name": f"{unique}",
                "type": "string",
            },
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject_1}/versions", json={"schema": json.dumps(schema_1)})
    assert res.status_code == 200
    schema_id_1 = res.json()["id"]

    res = await registry_async_client.post(f"subjects/{subject_1}/versions", json={"schema": json.dumps(schema_2)})
    assert res.status_code == 200
    schema_id_2 = res.json()["id"]
    assert schema_id_2 > schema_id_1

    # Reuse the first schema in another subject
    subject_2 = subject_name_factory()
    res = await registry_async_client.put(
        f"config/{subject_2}", json={"compatibility": "NONE"}
    )  # We don't care about compatibility
    res = await registry_async_client.post(f"subjects/{subject_2}/versions", json={"schema": json.dumps(schema_1)})
    assert res.status_code == 200
    assert res.json()["id"] == schema_id_1

    # Create a new schema
    res = await registry_async_client.post(f"subjects/{subject_2}/versions", json={"schema": json.dumps(schema_3)})
    assert res.status_code == 200
    schema_id_3 = res.json()["id"]
    assert res.json()["id"] == schema_id_3
    assert schema_id_3 > schema_id_2


@pytest.mark.parametrize("trail", ["", "/"])
async def test_config(registry_async_client: Client, trail: str) -> None:
    subject_name_factory = create_subject_name_factory(f"test_config-{trail}")

    # Tests /config endpoint
    res = await registry_async_client.put(f"config{trail}", json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await registry_async_client.get(f"config{trail}")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await registry_async_client.put(f"config{trail}", json={"compatibility": "NONE"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "NONE"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await registry_async_client.put(f"config{trail}", json={"compatibility": "nonexistentmode"})
    assert res.status_code == 422
    assert res.json()["error_code"] == 42203
    assert res.json()["message"] == SchemaErrorMessages.INVALID_COMPATIBILITY_LEVEL.value
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Create a new subject so we can try setting its config
    subject_1 = subject_name_factory()
    res = await registry_async_client.post(
        f"subjects/{subject_1}/versions{trail}",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.get(f"config/{subject_1}{trail}")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40408
    assert res.json()["message"] == SchemaErrorMessages.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_FMT.value.format(
        subject=subject_1
    )

    res = await registry_async_client.put(f"config/{subject_1}{trail}", json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await registry_async_client.get(f"config/{subject_1}{trail}")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"

    # Delete set compatibility on subject 1
    res = await registry_async_client.delete(f"config/{subject_1}{trail}")
    assert res.status_code == 200
    assert res.json()["compatibility"] == "NONE"

    # Verify compatibility not set on subject after delete
    res = await registry_async_client.get(f"config/{subject_1}{trail}")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40408
    assert res.json()["message"] == SchemaErrorMessages.SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_FMT.value.format(
        subject=subject_1
    )

    # It's possible to add a config to a subject that doesn't exist yet
    subject_2 = subject_name_factory()
    res = await registry_async_client.put(f"config/{subject_2}{trail}", json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # The subject doesn't exist from the schema point of view
    res = await registry_async_client.get(f"subjects/{subject_2}/versions")
    assert res.status_code == 404

    res = await registry_async_client.post(
        f"subjects/{subject_2}/versions",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.get(f"config/{subject_2}")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"

    # Test that config is returned for a subject that does not have an existing schema
    subject_3 = subject_name_factory()
    res = await registry_async_client.put(f"config/{subject_3}", json={"compatibility": "NONE"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "NONE"
    res = await registry_async_client.get(f"config/{subject_3}")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "NONE"


async def test_http_headers(registry_async_client: Client) -> None:
    res = await registry_async_client.get("subjects", headers={"Accept": "application/json"})
    assert res.headers["Content-Type"] == "application/json"

    # The default is received when not specifying
    res = await registry_async_client.get("subjects")
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Giving an invalid Accept value
    res = await registry_async_client.get("subjects", headers={"Accept": "application/vnd.schemaregistry.v2+json"})
    assert res.status_code == 406
    assert res.json()["message"] == "HTTP 406 Not Acceptable"

    # PUT with an invalid Content type
    res = await registry_async_client.put("config", json={"compatibility": "NONE"}, headers={"Content-Type": "text/html"})
    assert res.status_code == 415
    assert res.json()["message"] == "HTTP 415 Unsupported Media Type"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Multiple Accept values
    res = await registry_async_client.get(
        "subjects", headers={"Accept": "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"}
    )
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Weight works
    res = await registry_async_client.get(
        "subjects",
        headers={"Accept": "application/vnd.schemaregistry.v2+json; q=0.1, application/vnd.schemaregistry+json; q=0.9"},
    )
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry+json"

    # Accept without any subtype works
    res = await registry_async_client.get("subjects", headers={"Accept": "application/*"})
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    res = await registry_async_client.get("subjects", headers={"Accept": "text/*"})
    assert res.status_code == 406
    assert res.json()["message"] == "HTTP 406 Not Acceptable"

    # Accept without any type works
    res = await registry_async_client.get("subjects", headers={"Accept": "*/does_not_matter"})
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Default return is correct
    res = await registry_async_client.get("subjects", headers={"Accept": "*"})
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    res = await registry_async_client.get("subjects", headers={"Accept": "*/*"})
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Octet-stream is supported as a Content-Type
    res = await registry_async_client.put(
        "config", json={"compatibility": "FULL"}, headers={"Content-Type": "application/octet-stream"}
    )
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    res = await registry_async_client.get("subjects", headers={"Accept": "application/octet-stream"})
    assert res.status_code == 406

    # Parse Content-Type correctly
    res = await registry_async_client.put(
        "config",
        json={"compatibility": "NONE"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json; charset=utf-8"},
    )
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    assert res.json()["compatibility"] == "NONE"

    # Works with other than the default charset
    res = await registry_async_client.put_with_data(
        "config",
        data='{"compatibility": "NONE"}'.encode("utf-16"),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json; charset=utf-16"},
    )
    assert res.status_code == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    assert res.json()["compatibility"] == "NONE"
    if "SERVER_URI" in os.environ:
        for content_header in [
            {},
            {"Content-Type": "application/json"},
            {"content-type": "application/json"},
            {"CONTENT-Type": "application/json"},
            {"coNTEnt-tYPe": "application/json"},
        ]:
            path = os.path.join(os.getenv("SERVER_URI"), "subjects/unknown_subject")
            res = requests.request("POST", path, data=b"{}", headers=content_header)
            assert res.status_code == 404, res.content


async def test_schema_body_validation(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_schema_body_validation")()
    post_endpoints = {f"subjects/{subject}", f"subjects/{subject}/versions"}
    for endpoint in post_endpoints:
        # Wrong field name
        res = await registry_async_client.post(endpoint, json={"invalid_field": "invalid_value"})
        assert res.status_code == 422
        assert res.json()["error_code"] == 422
        assert res.json()["message"] == "Unrecognized field: invalid_field"
        # Additional field
        res = await registry_async_client.post(
            endpoint, json={"schema": '{"type": "string"}', "invalid_field": "invalid_value"}
        )
        assert res.status_code == 422
        assert res.json()["error_code"] == 422
        assert res.json()["message"] == "Unrecognized field: invalid_field"
        # Invalid body type
        res = await registry_async_client.post(endpoint, json="invalid")
        assert res.status_code == 500
        assert res.json()["error_code"] == 500
        assert res.json()["message"] == "Internal Server Error"


async def test_version_number_validation(registry_async_client: Client) -> None:
    """
    Creates a subject and schema. Tests that the endpoints
    subjects/{subject}/versions/{version} and
    subjects/{subject}/versions/{version}/schema
    return correct values both with valid and invalid parameters.
    """
    subject = create_subject_name_factory("test_version_number_validation")()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    schema_version = res.json()[0]
    invalid_schema_version = schema_version - 1

    version_endpoints = {f"subjects/{subject}/versions/$VERSION", f"subjects/{subject}/versions/$VERSION/schema"}
    for endpoint in version_endpoints:
        # Valid schema id
        res = await registry_async_client.get(endpoint.replace("$VERSION", str(schema_version)))
        assert res.status_code == 200

        # Invalid number
        res = await registry_async_client.get(endpoint.replace("$VERSION", str(invalid_schema_version)))
        assert res.status_code == 422
        assert res.json()["error_code"] == 42202
        assert (
            res.json()["message"] == f"The specified version '{invalid_schema_version}' is not a valid version id. "
            'Allowed values are between [1, 2^31-1] and the string "latest"'
        )
        # Valid latest string
        res = await registry_async_client.get(endpoint.replace("$VERSION", "latest"))
        assert res.status_code == 200
        # Invalid string
        res = await registry_async_client.get(endpoint.replace("$VERSION", "invalid"))
        assert res.status_code == 422
        assert res.json()["error_code"] == 42202
        assert (
            res.json()["message"] == "The specified version 'invalid' is not a valid version id. "
            'Allowed values are between [1, 2^31-1] and the string "latest"'
        )


async def test_common_endpoints(registry_async_client: Client) -> None:
    res = await registry_async_client.get("")
    assert res.status_code == 200
    assert res.json() == {}


async def test_invalid_namespace(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_invalid_namespace")()
    schema = {"type": "record", "name": "foo", "namespace": "foo-bar-baz", "fields": []}
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
    assert res.status_code == 422, res.json()
    json_res = res.json()
    assert json_res["error_code"] == 42201, json_res
    expected_message = (
        "Invalid AVRO schema. Error: foo-bar-baz is not a valid Avro name because it does not match the pattern "
        "(?:^|\\.)[A-Za-z_][A-Za-z0-9_]*$"
    )
    assert json_res["message"] == expected_message, json_res


async def test_schema_remains_constant(registry_async_client: Client) -> None:
    """
    Creates a subject with schema. Asserts the schema is the same when fetching it using schemas/ids/{schema_id}
    """
    subject = create_subject_name_factory("test_schema_remains_constant")()
    schema_name = create_schema_name_factory("test_schema_remains_constant")()
    schema = {
        "type": "record",
        "name": schema_name,
        "namespace": "foo_bar_baz",
        "fields": [{"type": "string", "name": "bla"}],
    }
    schema_str = json.dumps(schema)
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": schema_str})
    assert res.ok, res.json()
    schema_id = res.json()["id"]
    res = await registry_async_client.get(f"schemas/ids/{schema_id}")
    assert json.loads(res.json()["schema"]) == json.loads(schema_str)


async def test_malformed_kafka_message(
    kafka_servers: KafkaServers,
    registry_cluster: RegistryDescription,
    registry_async_client: Client,
) -> None:
    producer = KafkaProducer(bootstrap_servers=kafka_servers.bootstrap_servers)
    message_key = {"subject": "foo", "version": 1, "magic": 1, "keytype": "SCHEMA"}
    import random

    schema_id = random.randint(20000, 30000)
    payload = {"schema": json.dumps({"foo": "bar"})}
    message_value = {"deleted": False, "id": schema_id, "subject": "foo", "version": 1}
    message_value.update(payload)
    producer.send(
        registry_cluster.schemas_topic, key=json.dumps(message_key).encode(), value=json.dumps(message_value).encode()
    ).get()

    path = f"schemas/ids/{schema_id}"
    res = await repeat_until_successful_request(
        registry_async_client.get,
        path,
        json_data=None,
        headers=None,
        error_msg=f"Schema id {schema_id} not found",
        timeout=20,
        sleep=1,
    )
    res_data = res.json()
    assert res_data == payload, res_data


async def test_inner_type_compat_failure(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_inner_type_compat_failure")()

    sc = {
        "type": "record",
        "name": "record_line_movement_multiple_deleted",
        "namespace": "sya",
        "fields": [
            {
                "name": "meta",
                "type": {"type": "record", "name": "meta", "fields": [{"name": "date", "type": "long"}]},
            }
        ],
    }
    ev = {
        "type": "record",
        "name": "record_line_movement_multiple_deleted",
        "namespace": "sya",
        "fields": [
            {
                "name": "meta",
                "type": {
                    "type": "record",
                    "name": "meta",
                    "fields": [{"name": "date", "type": {"type": "long", "logicalType": "timestamp-millis"}}],
                },
            }
        ],
    }
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(sc)})
    assert res.ok
    sc_id = res.json()["id"]
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(ev)})
    assert res.ok
    assert sc_id != res.json()["id"]


async def test_anon_type_union_failure(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_anon_type_union_failure")()
    schema = {
        "type": "record",
        "name": "record_line_movement_updated",
        "fields": [
            {
                "name": "dependencies",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "record_line_movement_updated_dependencies",
                        "fields": [
                            {
                                "name": "coefficient",
                                "type": ["null", "double"],
                            }
                        ],
                    },
                ],
            },
        ],
    }
    evolved = {
        "type": "record",
        "name": "record_line_movement_updated",
        "fields": [
            {
                "name": "dependencies",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "record_line_movement_updated_dependencies",
                        "fields": [
                            {
                                "name": "coefficient",
                                "type": ["null", "double"],
                                # This is literally the only diff...
                                "doc": "Coeff of unit product",
                            }
                        ],
                    },
                ],
            },
        ],
    }

    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(schema)})
    assert res.ok
    sc_id = res.json()["id"]
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(evolved)})
    assert res.ok
    assert sc_id != res.json()["id"]


@pytest.mark.parametrize("compatibility", ["FULL", "FULL_TRANSITIVE"])
async def test_full_transitive_failure(registry_async_client: Client, compatibility: str) -> None:
    subject = create_subject_name_factory(f"test_full_transitive_failure-{compatibility}")()

    init = {
        "type": "record",
        "name": "order",
        "namespace": "example",
        "fields": [
            {
                "name": "someField",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "someEmbeddedRecord",
                        "namespace": "example",
                        "fields": [{"name": "name", "type": "string"}],
                    },
                ],
                "default": "null",
            }
        ],
    }
    evolved = {
        "type": "record",
        "name": "order",
        "namespace": "example",
        "fields": [
            {
                "name": "someField",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "someEmbeddedRecord",
                        "namespace": "example",
                        "fields": [{"name": "name", "type": "string"}, {"name": "price", "type": "int"}],
                    },
                ],
                "default": "null",
            }
        ],
    }
    await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(init)})
    assert res.ok
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": json.dumps(evolved)})
    assert not res.ok
    assert res.status_code == 409


async def test_invalid_schemas(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_invalid_schemas")()

    repated_field = {
        "type": "record",
        "name": "myrecord",
        "fields": [{"type": "string", "name": "name"}, {"type": "string", "name": "name", "default": "test"}],
    }

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(repated_field)},
    )
    assert res.status_code != 500, "an invalid schema should not cause a server crash"
    assert not is_success(HTTPStatus(res.status_code)), "an invalid schema must not be a success"


async def test_schema_hard_delete_version(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_schema_hard_delete_version")()
    res = await registry_async_client.put("config", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    schemav1 = {
        "type": "record",
        "name": "myenumtest",
        "fields": [
            {
                "type": {
                    "type": "enum",
                    "name": "enumtest",
                    "symbols": ["first", "second"],
                },
                "name": "faa",
            }
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schemav1)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schemav1_id = res.json()["id"]

    schemav2 = {
        "type": "record",
        "name": "myenumtest",
        "fields": [
            {
                "type": {
                    "type": "enum",
                    "name": "enumtest",
                    "symbols": ["first", "second", "third"],
                },
                "name": "faa",
            }
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schemav2)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schemav2_id = res.json()["id"]
    assert schemav1_id != schemav2_id

    # Cannot directly hard delete schema v1
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1?permanent=true")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40407
    assert res.json()["message"] == f"Subject '{subject}' Version 1 was not deleted first before being permanently deleted"

    # Soft delete schema v1
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1")
    assert res.status_code == 200
    assert res.json() == 1

    # Cannot soft delete twice
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40406
    assert (
        res.json()["message"] == f"Subject '{subject}' Version 1 was soft deleted.Set permanent=true to delete permanently"
    )

    res = await registry_async_client.get(f"subjects/{subject}/versions/1")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version 1 not found."

    # Check that soft deleted is found when asking also for deleted schemas
    res = await registry_async_client.get(f"subjects/{subject}/versions/1?deleted=true")
    assert res.status_code == 200
    assert res.json()["version"] == 1
    assert res.json()["subject"] == subject

    # Hard delete schema v1
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1?permanent=true")
    assert res.status_code == 200

    # Cannot hard delete twice
    res = await registry_async_client.delete(f"subjects/{subject}/versions/1?permanent=true")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version 1 not found."

    # Check hard deleted is not found at all
    res = await registry_async_client.get(f"subjects/{subject}/versions/1?deleted=true")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version 1 not found."


async def test_schema_hard_delete_whole_schema(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_schema_hard_delete_whole_schema")()
    res = await registry_async_client.put("config", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    schemav1 = {
        "type": "record",
        "name": "myenumtest",
        "fields": [
            {
                "type": {
                    "type": "enum",
                    "name": "enumtest",
                    "symbols": ["first", "second"],
                },
                "name": "faa",
            }
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schemav1)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schemav1_id = res.json()["id"]

    schemav2 = {
        "type": "record",
        "name": "myenumtest",
        "fields": [
            {
                "type": {
                    "type": "enum",
                    "name": "enumtest",
                    "symbols": ["first", "second", "third"],
                },
                "name": "faa",
            }
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schemav2)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schemav2_id = res.json()["id"]
    assert schemav1_id != schemav2_id

    # Hard delete whole schema cannot be done before soft delete
    res = await registry_async_client.delete(f"subjects/{subject}?permanent=true")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40405
    assert res.json()["message"] == f"Subject '{subject}' was not deleted first before being permanently deleted"

    # Soft delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 200
    assert res.json() == [1, 2]

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    # Soft delete cannot be done twice
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40404
    assert res.json()["message"] == f"Subject '{subject}' was soft deleted.Set permanent=true to delete permanently"

    # Hard delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}?permanent=true")
    assert res.status_code == 200
    assert res.json() == [1, 2]

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."


async def test_schema_hard_delete_and_recreate(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_schema_hard_delete_and_recreate")()
    schema_name = create_schema_name_factory("test_schema_hard_delete_and_recreate")()

    # Deleting non-existing gives valid error message
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    res = await registry_async_client.delete(f"subjects/{subject}/versions/1")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    res = await registry_async_client.put("config", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200
    schema = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "type": {
                    "type": "enum",
                    "name": "enumtest",
                    "symbols": ["first", "second"],
                },
                "name": "faa",
            }
        ],
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    # Soft delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 200

    # Recreate with same subject after soft delete
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"], "after soft delete the same schema registered, the same identifier"

    # Soft delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 200
    # Hard delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}?permanent=true")
    assert res.status_code == 200

    # Deleting non-existing gives valid error message
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    res = await registry_async_client.delete(f"subjects/{subject}/versions/1")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    # Recreate with same subject after hard delete
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"], "after permanent deleted the same schema registered, the same identifier"

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200

    # After recreated, subject again registered
    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject in res.json()


# This test starts with a state that a buggy old Karapace created, but is no longer producible.
# However Karapace should tolerate and fix the situation
async def test_schema_recreate_after_odd_hard_delete(
    kafka_servers: KafkaServers,
    registry_cluster: RegistryDescription,
    registry_async_client: Client,
) -> None:
    subject = create_subject_name_factory("test_schema_recreate_after_odd_hard_delete")()
    schema_name = create_schema_name_factory("test_schema_recreate_after_odd_hard_delete")()

    schema = {
        "type": "record",
        "name": schema_name,
        "fields": [
            {
                "type": {
                    "type": "enum",
                    "name": "enumtest",
                    "symbols": ["first", "second"],
                },
                "name": "faa",
            }
        ],
    }

    producer = KafkaProducer(bootstrap_servers=kafka_servers.bootstrap_servers)
    message_key = json_encode(
        {"subject": subject, "version": 1, "magic": 1, "keytype": "SCHEMA"}, sort_keys=False, compact=True, binary=True
    )
    import random

    schema_id = random.randint(20000, 30000)
    message_value = {"deleted": False, "id": schema_id, "subject": subject, "version": 1, "schema": json.dumps(schema)}
    producer.send(
        registry_cluster.schemas_topic,
        key=message_key,
        value=json_encode(message_value, sort_keys=False, compact=True, binary=True),
    ).get()
    # Produce manual hard delete without soft delete first
    producer.send(registry_cluster.schemas_topic, key=message_key, value=None).get()

    res = await registry_async_client.put("config", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    # Initially no subject registered
    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject not in res.json()

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    # Now subject is listed
    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject in res.json()

    # Also newly registed schema can be fetched
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200

    # Soft delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 200

    # Recreate with same subject after soft delete
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"], "after soft delete the same schema registered, the same identifier"

    # Soft delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}")
    assert res.status_code == 200
    # Hard delete whole schema
    res = await registry_async_client.delete(f"subjects/{subject}?permanent=true")
    assert res.status_code == 200

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == f"Subject '{subject}' not found."

    # Recreate with same subject after hard delete
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": json.dumps(schema)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"], "after permanent deleted the same schema registered, the same identifier"

    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200

    # After recreated, subject again registered
    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject in res.json()


async def test_invalid_schema_should_provide_good_error_messages(registry_async_client: Client) -> None:
    """The user should receive an informative error message when the format is invalid"""
    subject_name_factory = create_subject_name_factory("test_schema_subject_post_invalid_data")
    test_subject = subject_name_factory()

    schema_str = json.dumps({"type": "string"})
    res = await registry_async_client.post(
        f"subjects/{test_subject}/versions",
        json={"schema": schema_str[:-1]},
    )
    assert res.json()["message"] == "Invalid AVRO schema. Error: Expecting ',' delimiter: line 1 column 18 (char 17)"

    # Unfortunately the AVRO library doesn't provide a good error message, it just raises an TypeError
    schema_str = json.dumps({"type": "enum", "name": "error"})
    res = await registry_async_client.post(
        f"subjects/{test_subject}/versions",
        json={"schema": schema_str},
    )
    assert (
        res.json()["message"]
        == "Invalid AVRO schema. Error: Enum symbols must be a sequence of strings, but it is <class 'NoneType'>"
    )

    # This is an upstream bug in the python AVRO library, until the bug is fixed we should at least have a nice error message
    schema_str = json.dumps({"type": "enum", "name": "error", "symbols": {}})
    res = await registry_async_client.post(
        f"subjects/{test_subject}/versions",
        json={"schema": schema_str},
    )
    assert (
        res.json()["message"]
        == "Invalid AVRO schema. Error: Enum symbols must be a sequence of strings, but it is <class 'dict'>"
    )

    # This is an upstream bug in the python AVRO library, until the bug is fixed we should at least have a nice error message
    schema_str = json.dumps({"type": "enum", "name": "error", "symbols": ["A", "B"]})
    res = await registry_async_client.post(
        f"subjects/{test_subject}/versions",
        json={"schema": schema_str},
    )
    assert res.json()["message"] == "Invalid AVRO schema. Error: error is a reserved type name."
