"""
karapace - schema tests

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from http import HTTPStatus
from kafka import KafkaProducer
from karapace.rapu import is_success
from tests.utils import repeat_until_successful_request

import json as jsonlib
import os
import pytest
import requests

baseurl = "http://localhost:8081"


@pytest.mark.parametrize("trail", ["", "/"])
@pytest.mark.parametrize("compatibility", ["FORWARD", "BACKWARD", "FULL"])
async def test_enum_schema_compatibility(registry_async_client, compatibility, trail):
    subject = os.urandom(16).hex()

    res = await registry_async_client.put(f"config{trail}", json={"compatibility": compatibility})
    assert res.status == 200
    schema = {
        "type": "record",
        "name": "myenumtest",
        "fields": [{
            "type": {
                "type": "enum",
                "name": "enumtest",
                "symbols": ["first", "second"],
            },
            "name": "faa",
        }]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]
    schema = {
        "type": "record",
        "name": "myenumtest",
        "fields": [{
            "type": {
                "type": "enum",
                "name": "enumtest",
                "symbols": ["first", "second", "third"],
            },
            "name": "faa",
        }]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id2 = res.json()["id"]
    assert schema_id != schema_id2

    schema = {
        "type": "record",
        "name": "myenumtest",
        "fields": [{
            "type": {
                "type": "enum",
                "name": "enumtest",
                "symbols": ["second"],
            },
            "name": "faa",
        }]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id3 = res.json()["id"]
    assert schema_id3 != schema_id2

    res = await registry_async_client.get(f"schemas/ids/{schema_id3}{trail}")
    assert res.status_code == 200
    res = jsonlib.loads(res.json()["schema"])
    assert res["type"] == "record"
    assert res["name"] == "myenumtest"
    assert res["fields"][0]["name"] == "faa"
    assert res["fields"][0]["type"]["type"] == "enum"
    assert res["fields"][0]["type"]["name"] == "enumtest"
    assert res["fields"][0]["type"]["symbols"] == ["second"]


@pytest.mark.parametrize("trail", ["", "/"])
async def test_union_to_union(registry_async_client, trail):
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status == 200
    init_schema = {"name": "init", "type": "record", "fields": [{"name": "inner", "type": ["string", "int"]}]}
    evolved = {"name": "init", "type": "record", "fields": [{"name": "inner", "type": ["null", "string"]}]}
    evolved_compatible = {
        "name": "init",
        "type": "record",
        "fields": [{
            "name": "inner",
            "type": [
                "int", "string", {
                    "type": "record",
                    "name": "foobar_fields",
                    "fields": [{
                        "name": "foo",
                        "type": "string"
                    }]
                }
            ]
        }]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(init_schema)}
    )
    assert res.status == 200
    assert "id" in res.json()
    res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(evolved)})
    assert res.status == 409
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(evolved_compatible)}
    )
    assert res.status == 200
    # fw compat check
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "FORWARD"})
    assert res.status == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(evolved_compatible)}
    )
    assert res.status == 200
    assert "id" in res.json()
    res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(evolved)})
    assert res.status == 409
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(init_schema)}
    )
    assert res.status == 200


@pytest.mark.parametrize("trail", ["", "/"])
async def test_missing_subject_compatibility(registry_async_client, trail):
    subject = os.urandom(16).hex()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps({"type": "string"})}
    )
    assert res.status_code == 200, f"{res} {subject}"
    res = await registry_async_client.get(f"config/{subject}{trail}")
    assert res.status == 404, f"{res} {subject}"
    res = await registry_async_client.get(f"config/{subject}{trail}?defaultToGlobal=false")
    assert res.status == 404, f"subject should have no compatibility when not defaulting to global: {res.json()}"
    res = await registry_async_client.get(f"config/{subject}{trail}?defaultToGlobal=true")
    assert res.status == 200, f"subject should have a compatibility when not defaulting to global: {res.json()}"

    assert "compatibilityLevel" in res.json(), res.json()


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_union_schema_compatibility(registry_async_client, trail):
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status == 200
    original_schema = {
        "name": "bar",
        "namespace": "foo",
        "type": "record",
        "fields": [{
            "name": "foobar",
            "type": [{
                "type": "array",
                "name": "foobar_items",
                "items": {
                    "type": "record",
                    "name": "foobar_fields",
                    "fields": [{
                        "name": "foo",
                        "type": "string"
                    }]
                }
            }]
        }]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(original_schema)}
    )
    assert res.status == 200
    assert "id" in res.json()

    evolved_schema = {
        "name": "bar",
        "namespace": "foo",
        "type": "record",
        "fields": [{
            "name": "foobar",
            "type": [{
                "type": "array",
                "name": "foobar_items",
                "items": {
                    "type": "record",
                    "name": "foobar_fields",
                    "fields": [{
                        "name": "foo",
                        "type": "string"
                    }, {
                        "name": "bar",
                        "type": ["null", "string"],
                        "default": None
                    }]
                }
            }]
        }]
    }
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": jsonlib.dumps(evolved_schema)},
    )
    assert res.status == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(evolved_schema)}
    )
    assert res.status == 200
    assert "id" in res.json()

    # Check that we can delete the field as well
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": jsonlib.dumps(original_schema)},
    )
    assert res.status == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(original_schema)}
    )
    assert res.status == 200
    assert "id" in res.json()


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_nested_schema_compatibility(registry_async_client, trail):
    subject = os.urandom(16).hex()

    res = await registry_async_client.put("config", json={"compatibility": "BACKWARD"})
    assert res.status == 200
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
                }
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert "id" in res.json()

    # change string to integer in the nested record, should fail
    schema["fields"][1]["type"]["fields"][0]["type"] = "int"
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 409


@pytest.mark.parametrize("trail", ["", "/"])
async def test_compatibility_endpoint(registry_async_client, trail):
    res = await registry_async_client.put(f"config{trail}", json={"compatibility": "BACKWARD"})
    assert res.status == 200

    subject = os.urandom(16).hex()
    schema = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "age",
                "type": "int",
            },
        ]
    }

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200

    res = await registry_async_client.get("schemas/ids/{}{}".format(res.json()["id"], trail))
    schema_gotten_back = jsonlib.loads(res.json()["schema"])
    assert schema_gotten_back == schema

    # replace int with long
    schema["fields"] = [{"type": "long", "name": "age"}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert res.json() == {"is_compatible": True}

    schema["fields"] = [{"type": "string", "name": "age"}]
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert res.json() == {"is_compatible": False}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_type_compatibility(registry_async_client, trail):
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

    for compatibility, source_type, target_type, expected in _test_cases():
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        schema = {
            "type": "record",
            "name": "Objct",
            "fields": [
                {
                    "name": "field",
                    "type": source_type,
                },
            ]
        }
        res = await registry_async_client.post(
            f"subjects/{subject}/versions{trail}",
            json={"schema": jsonlib.dumps(schema)},
        )
        assert res.status == 200

        schema["fields"][0]["type"] = target_type
        res = await registry_async_client.post(
            f"compatibility/subjects/{subject}/versions/latest{trail}",
            json={"schema": jsonlib.dumps(schema)},
        )
        assert res.status == 200
        assert res.json() == {"is_compatible": expected}


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_schema_compatibility(registry_async_client, trail):
    subject = os.urandom(16).hex()

    res = await registry_async_client.put("config", json={"compatibility": "FORWARD"})
    assert res.status == 200
    schema = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
        ]
    }

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    schema2 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "first_name",
                "type": "string"
            },
            {
                "name": "last_name",
                "type": "string"
            },
            {
                "name": "age",
                "type": "int"
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema2)},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id2 = res.json()["id"]
    assert schema_id != schema_id2

    schema3a = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "last_name",
                "type": "string"
            },
            {
                "name": "third_name",
                "type": "string",
                "default": "foodefaultvalue"
            },
            {
                "name": "age",
                "type": "int"
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema3a)},
    )
    # Fails because field removed
    assert res.status == 409
    res_json = res.json()
    assert res_json["error_code"] == 409

    schema3b = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "first_name",
                "type": "string"
            },
            {
                "name": "last_name",
                "type": "string"
            },
            {
                "name": "age",
                "type": "long"
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema3b)},
    )
    # Fails because incompatible type change
    assert res.status == 409
    res_json = res.json()
    assert res_json["error_code"] == 409

    schema4 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "first_name",
                "type": "string"
            },
            {
                "name": "last_name",
                "type": "string"
            },
            {
                "name": "third_name",
                "type": "string",
                "default": "foodefaultvalue"
            },
            {
                "name": "age",
                "type": "int"
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema4)},
    )
    assert res.status == 200

    res = await registry_async_client.put("config", json={"compatibility": "BACKWARD"})
    schema5 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "first_name",
                "type": "string"
            },
            {
                "name": "last_name",
                "type": "string"
            },
            {
                "name": "third_name",
                "type": "string",
                "default": "foodefaultvalue"
            },
            {
                "name": "fourth_name",
                "type": "string"
            },
            {
                "name": "age",
                "type": "int"
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema5)},
    )
    assert res.status == 409

    # Add a default value for the field
    schema5["fields"][3] = {"name": "fourth_name", "type": "string", "default": "foof"}
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema5)},
    )
    assert res.status == 200
    assert "id" in res.json()

    # Try to submit schema with a different definition
    schema5["fields"][3] = {"name": "fourth_name", "type": "int", "default": 2}
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": jsonlib.dumps(schema5)},
    )
    assert res.status == 409

    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    schema = {"type": "record", "name": "Object", "fields": [{"name": "first_name", "type": "string"}]}
    res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    schema["fields"].append({"name": "last_name", "type": "string"})
    res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
    assert res.status == 409


@pytest.mark.parametrize("trail", ["", "/"])
async def test_enum_schema_field_add_compatibility(registry_async_client, trail):
    expected_results = [("BACKWARD", 200), ("FORWARD", 200), ("FULL", 200)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status == 200
        schema = {"type": "enum", "name": "Suit", "symbols": ["SPADES", "HEARTS", "DIAMONDS"]}
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Add a field
        schema["symbols"].append("CLUBS")
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == status_code


@pytest.mark.parametrize("trail", ["", "/"])
async def test_array_schema_field_add_compatibility(registry_async_client, trail):
    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status == 200
        schema = {"type": "array", "items": "int"}
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Modify the items type
        schema["items"] = "long"
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == status_code


@pytest.mark.parametrize("trail", ["", "/"])
async def test_array_nested_record_compatibility(registry_async_client, trail):
    expected_results = [("BACKWARD", 409), ("FORWARD", 200), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status == 200
        schema = {
            "type": "array",
            "items": {
                "type": "record",
                "name": "object",
                "fields": [{
                    "name": "first_name",
                    "type": "string"
                }]
            }
        }
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Add a second field to the record
        schema["items"]["fields"].append({"name": "last_name", "type": "string"})
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == status_code


@pytest.mark.parametrize("trail", ["", "/"])
async def test_record_nested_array_compatibility(registry_async_client, trail):
    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": compatibility})
        assert res.status == 200
        schema = {
            "type": "record",
            "name": "object",
            "fields": [{
                "name": "simplearray",
                "type": {
                    "type": "array",
                    "items": "int"
                }
            }]
        }
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Modify the array items type
        schema["fields"][0]["type"]["items"] = "long"
        res = await registry_async_client.post(f"subjects/{subject}/versions{trail}", json={"schema": jsonlib.dumps(schema)})
        assert res.status == status_code


async def test_map_schema_field_add_compatibility(
    registry_async_client
):  # TODO: Rename to pålain check map schema and add additional steps
    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status == 200
        schema = {"type": "map", "values": "int"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Modify the items type
        schema["values"] = "long"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == status_code


async def test_enum_schema(registry_async_client):
    for compatibility in {"BACKWARD", "FORWARD", "FULL"}:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status == 200
        schema = {"type": "enum", "name": "testenum", "symbols": ["first"]}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})

        # Add a symbol.
        schema["symbols"].append("second")
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Remove a symbol
        schema["symbols"].pop(1)
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Change the name
        schema["name"] = "another"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 409

        # Inside record
        subject = os.urandom(16).hex()
        schema = {
            "type": "record",
            "name": "object",
            "fields": [{
                "name": "enumkey",
                "type": {
                    "type": "enum",
                    "name": "testenum",
                    "symbols": ["first"]
                }
            }]
        }
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})

        # Add a symbol.
        schema["fields"][0]["type"]["symbols"].append("second")
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Remove a symbol
        schema["fields"][0]["type"]["symbols"].pop(1)
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200

        # Change the name
        schema["fields"][0]["type"]["name"] = "another"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 409


@pytest.mark.parametrize("compatibility", ["BACKWARD", "FORWARD", "FULL"])
async def test_fixed_schema(registry_async_client, compatibility):
    status_code_allowed = 200
    status_code_denied = 409
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
    assert res.status == 200
    schema = {"type": "fixed", "size": 16, "name": "md5", "aliases": ["testalias"]}
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})

    # Add new alias
    schema["aliases"].append("anotheralias")
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.status == status_code_allowed

    # Try to change size
    schema["size"] = 32
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.status == status_code_denied

    # Try to change name
    schema["size"] = 16
    schema["name"] = "denied"
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.status == status_code_denied

    # In a record
    subject = os.urandom(16).hex()
    schema = {
        "type": "record",
        "name": "object",
        "fields": [{
            "name": "fixedkey",
            "type": {
                "type": "fixed",
                "size": 16,
                "name": "md5",
                "aliases": ["testalias"]
            }
        }]
    }
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})

    # Add new alias
    schema["fields"][0]["type"]["aliases"].append("anotheralias")
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.status == status_code_allowed

    # Try to change size
    schema["fields"][0]["type"]["size"] = 32
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.status == status_code_denied

    # Try to change name
    schema["fields"][0]["type"]["size"] = 16
    schema["fields"][0]["type"]["name"] = "denied"
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.status == status_code_denied


async def test_primitive_schema(registry_async_client):
    expected_results = [("BACKWARD", 200), ("FORWARD", 200), ("FULL", 200)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status == 200

        # Transition from string to bytes
        schema = {"type": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200
        schema["type"] = "bytes"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == status_code

    expected_results = [("BACKWARD", 409), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status == 200

        # Transition from string to int
        schema = {"type": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200
        schema["type"] = "int"
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})


async def test_union_comparing_to_other_types(registry_async_client):
    expected_results = [("BACKWARD", 409), ("FORWARD", 200), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status == 200

        # Union vs non-union with the same schema
        schema = [{"type": "array", "name": "listofstrings", "items": "string"}, "string"]
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200
        plain_schema = {"type": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(plain_schema)})
        assert res.status == status_code

    expected_results = [("BACKWARD", 200), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status == 200

        # Non-union first
        schema = {"type": "array", "name": "listofstrings", "items": "string"}
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200
        union_schema = [{"type": "array", "name": "listofstrings", "items": "string"}, "string"]
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(union_schema)})
        assert res.status == status_code

    expected_results = [("BACKWARD", 409), ("FORWARD", 409), ("FULL", 409)]
    for compatibility, status_code in expected_results:
        subject = os.urandom(16).hex()
        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
        assert res.status == 200

        # Union to a completely different schema
        schema = [{"type": "array", "name": "listofstrings", "items": "string"}, "string"]
        res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
        assert res.status == 200
        plain_wrong_schema = {"type": "int"}
        res = await registry_async_client.post(
            f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(plain_wrong_schema)}
        )
        assert res.status == status_code


async def test_transitive_compatibility(registry_async_client):
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "BACKWARD_TRANSITIVE"})
    assert res.status == 200

    schema0 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "age",
                "type": "int"
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": jsonlib.dumps(schema0)},
    )
    assert res.status == 200

    schema1 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "age",
                "type": "int"
            },
            {
                "name": "first_name",
                "type": "string",
                "default": "John",
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": jsonlib.dumps(schema1)},
    )
    assert res.status == 200

    schema2 = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "age",
                "type": "int"
            },
            {
                "name": "first_name",
                "type": "string",
            },
            {
                "name": "last_name",
                "type": "string",
                "default": "Doe",
            },
        ]
    }
    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": jsonlib.dumps(schema2)},
    )
    assert res.status == 409
    res_json = res.json()
    assert res_json["error_code"] == 409


@pytest.mark.parametrize("trail", ["", "/"])
async def test_schema(registry_async_client, trail):
    subject = os.urandom(16).hex()
    schema_str = '{"type": "string"}'
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": schema_str},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]
    res = await registry_async_client.get(f"schemas/ids/{schema_id}{trail}")
    assert res.status_code == 200
    assert res.json()["schema"] == schema_str

    # repost same schema again to see that a new id is not generated but an old one is given back
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"]

    # Schema missing in the json body
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={},
    )
    assert res.status == 500
    assert res.json()["error_code"] == 500
    assert res.json()["message"] == "Internal Server Error"

    # nonexistent schema id
    result = await registry_async_client.get(os.path.join("schemas/ids/123456789"))
    assert result.json()["error_code"] == 40403

    # invalid schema_id
    result = await registry_async_client.get(f"schemas/ids/invalid{trail}")
    assert result.status == 404
    assert result.json()["error_code"] == 404
    assert result.json()["message"] == "HTTP 404 Not Found"

    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject), json={"schema": "{\"type\": \"string\", \"foo\": \"string\"}"}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id != res.json()["id"]

    # Fetch the schema back to see how it was mangled
    result = await registry_async_client.get(os.path.join("schemas/ids/{}".format(res.json()["id"])))
    schema = jsonlib.loads(result.json()["schema"])
    assert schema["type"] == "string"
    assert schema["foo"] == "string"

    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject in res.json()

    res = await registry_async_client.get("subjects/{}/versions".format(subject))
    assert res.status_code == 200
    assert res.json() == [1, 2]

    res = await registry_async_client.get("subjects/{}/versions/1".format(subject))
    assert res.status_code == 200
    assert res.json()["subject"] == subject
    assert res.json()["schema"] == schema_str

    # Find an invalid version 0
    res = await registry_async_client.get("subjects/{}/versions/0".format(subject))
    assert res.status_code == 422
    assert res.json()["error_code"] == 42202
    assert res.json()["message"] == \
        'The specified version is not a valid version id. Allowed values are between [1, 2^31-1] and the string "latest"'

    # Find an invalid version (too large)
    res = await registry_async_client.get("subjects/{}/versions/15".format(subject))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version not found."

    # Delete an actual version
    res = await registry_async_client.delete("subjects/{}/versions/1".format(subject))
    assert res.status_code == 200
    assert res.json() == 1

    # Delete a whole subject
    res = await registry_async_client.delete("subjects/{}".format(subject))
    assert res.status_code == 200
    assert res.json() == [2]

    # List all subjects, our subject shouldn't be in the list
    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    assert subject not in res.json()

    # After deleting the last version of a subject, it shouldn't be in the list
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    res = await registry_async_client.get("subjects")
    assert subject in res.json()
    res = await registry_async_client.get("subjects/{}/versions".format(subject))
    assert res.json() == [3]
    res = await registry_async_client.delete("subjects/{}/versions/3".format(subject))
    assert res.status_code == 200
    res = await registry_async_client.get("subjects")
    assert subject not in res.json()

    res = await registry_async_client.get("subjects/{}/versions".format(subject))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."
    res = await registry_async_client.get("subjects/{}/versions/latest".format(subject))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."

    # Creating a new schema works after deleting the only available version
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    res = await registry_async_client.get("subjects/{}/versions".format(subject))
    assert res.json() == [4]

    # Check version number generation when deleting an entire subjcect
    subject = os.urandom(16).hex()
    res = await registry_async_client.put("config/{}".format(subject), json={"compatibility": "NONE"})
    assert res.status == 200
    schema = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
        ]
    }
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    assert "id" in res.json()
    schema2 = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
            {
                "name": "last_name",
                "type": "string",
            },
        ]
    }
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema2)})
    assert res.status == 200
    assert "id" in res.json()
    res = await registry_async_client.get("subjects/{}/versions".format(subject))
    assert res.status == 200
    assert res.json() == [1, 2]
    res = await registry_async_client.delete("subjects/{}".format(subject))
    assert res.status == 200
    # Recreate subject
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    res = await registry_async_client.get("subjects/{}/versions".format(subject))
    assert res.json() == [3]  # Version number generation should now begin at 3

    # Check the return format on a more complex schema for version get
    subject = os.urandom(16).hex()
    schema = {
        "type": "record",
        "name": "Objct",
        "fields": [
            {
                "name": "first_name",
                "type": "string",
            },
        ]
    }
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    res = await registry_async_client.get("subjects/{}/versions/1".format(subject))
    assert res.status == 200
    assert res.json()["subject"] == subject
    assert sorted(jsonlib.loads(res.json()["schema"])) == sorted(schema)

    # Submitting the exact same schema for a different subject should return the same schema ID.
    subject = os.urandom(16).hex()
    schema = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value",
                "type": "string",
            },
        ]
    }
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    assert "id" in res.json()
    original_schema_id = res.json()["id"]
    # New subject with the same schema
    subject = os.urandom(16).hex()
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    assert "id" in res.json()
    new_schema_id = res.json()["id"]
    assert original_schema_id == new_schema_id

    # It also works for multiple versions in a single subject
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(
        "config/{}".format(subject), json={"compatibility": "NONE"}
    )  # We don't care about the compatibility in this test
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert res.json()["id"] == new_schema_id  # Same ID as in the previous test step

    # The subject version schema endpoint returns the correct results
    subject = os.urandom(16).hex()
    schema_str = '{"type": "string"}'
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": schema_str},
    )
    assert res.status == 200
    res = await registry_async_client.get(f"subjects/{subject}/versions/1/schema")
    assert res.status == 200
    assert res.json() == jsonlib.loads(schema_str)
    res = await registry_async_client.get(f"subjects/{os.urandom(16).hex()}/versions/1/schema")  # Invalid subject
    assert res.status == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."
    res = await registry_async_client.get(f"subjects/{subject}/versions/2/schema")
    assert res.status == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version not found."
    res = await registry_async_client.get(f"subjects/{subject}/versions/latest/schema")
    assert res.status == 200
    assert res.json() == jsonlib.loads(schema_str)

    # The schema check for subject endpoint returns correct results
    subject = os.urandom(16).hex()
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": schema_str},
    )
    assert res.status == 200
    schema_id = res.json()["id"]
    # The same ID should be returned when checking the same schema against the same subject
    res = await registry_async_client.post(
        f"subjects/{subject}",
        json={"schema": schema_str},
    )
    assert res.status == 200
    assert res.json() == {"id": schema_id, "subject": subject, "schema": schema_str, "version": 1}
    # Invalid schema should return 500
    res = await registry_async_client.post(
        f"subjects/{subject}",
        json={"schema": '{"type": "invalid_type"}'},
    )
    assert res.status == 500
    assert res.json()["message"] == f"Error while looking up schema under subject {subject}"
    # Subject is not found
    res = await registry_async_client.post(
        f"subjects/{os.urandom(16).hex()}",
        json={"schema": schema_str},
    )
    assert res.status == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."
    # Schema not found for subject
    res = await registry_async_client.post(
        f"subjects/{subject}",
        json={"schema": '{"type": "int"}'},
    )
    assert res.status == 404
    assert res.json()["error_code"] == 40403
    assert res.json()["message"] == "Schema not found"
    # Schema not included in the request body
    res = await registry_async_client.post(f"subjects/{subject}", json={})
    assert res.status == 500
    assert res.json()["error_code"] == 500
    assert res.json()["message"] == "Internal Server Error"
    # Schema not included in the request body for subject that does not exist
    res = await registry_async_client.post(
        f"subjects/{os.urandom(16).hex()}",
        json={},
    )
    assert res.status == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."

    # Test that global ID values stay consistent after using pre-existing schema ids
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(
        "config/{}".format(subject), json={"compatibility": "NONE"}
    )  # We don't care about compatibility
    schema = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value",
                "type": "string",
            },
        ]
    }
    schema2 = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value2",
                "type": "string",
            },
        ]
    }
    schema3 = {
        "type": "record",
        "name": "Object",
        "fields": [
            {
                "name": "just_a_value3",
                "type": "int",
            },
        ]
    }
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    first_schema_id = res.json()["id"]
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema2)})
    assert res.status == 200
    assert res.json()["id"] == first_schema_id + 1
    # Reuse the first schema in another subject
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(
        "config/{}".format(subject), json={"compatibility": "NONE"}
    )  # We don't care about compatibility
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    assert res.json()["id"] == first_schema_id
    # Create a new schema
    res = await registry_async_client.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema3)})
    assert res.status == 200
    assert res.json()["id"] == first_schema_id + 2


@pytest.mark.parametrize("trail", ["", "/"])
async def test_config(registry_async_client, trail):
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
    assert res.json()["message"] == "Invalid compatibility level. Valid values are none, backward, forward and full"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Create a new subject so we can try setting its config
    subject = os.urandom(16).hex()
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.get(f"config/{subject}{trail}")
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await registry_async_client.get(f"config/{subject}{trail}")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"

    # It's possible to add a config to a subject that doesn't exist yet
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # The subject doesn't exist from the schema point of view
    res = await registry_async_client.get("subjects/{}/versions".format(subject))
    assert res.status_code == 404

    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.get("config/{}".format(subject))
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"

    # Test that config is returned for a subject that does not have an existing schema
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "NONE"})
    assert res.status == 200
    assert res.json()["compatibility"] == "NONE"
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status == 200
    assert res.json()["compatibilityLevel"] == "NONE"


async def test_http_headers(registry_async_client):
    res = await registry_async_client.get("subjects", headers={"Accept": "application/json"})
    assert res.headers["Content-Type"] == "application/json"

    # The default is received when not specifying
    res = await registry_async_client.get("subjects")
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Giving an invalid Accept value
    res = await registry_async_client.get("subjects", headers={"Accept": "application/vnd.schemaregistry.v2+json"})
    assert res.status == 406
    assert res.json()["message"] == HTTPStatus.NOT_ACCEPTABLE.description

    # PUT with an invalid Content type
    res = await registry_async_client.put("config", json={"compatibility": "NONE"}, headers={"Content-Type": "text/html"})
    assert res.status == 415
    assert res.json()["message"] == HTTPStatus.UNSUPPORTED_MEDIA_TYPE.description
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Multiple Accept values
    res = await registry_async_client.get(
        "subjects", headers={"Accept": "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"}
    )
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Weight works
    res = await registry_async_client.get(
        "subjects",
        headers={"Accept": "application/vnd.schemaregistry.v2+json; q=0.1, application/vnd.schemaregistry+json; q=0.9"}
    )
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry+json"

    # Accept without any subtype works
    res = await registry_async_client.get("subjects", headers={"Accept": "application/*"})
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    res = await registry_async_client.get("subjects", headers={"Accept": "text/*"})
    assert res.status == 406
    assert res.json()["message"] == HTTPStatus.NOT_ACCEPTABLE.description

    # Accept without any type works
    res = await registry_async_client.get("subjects", headers={"Accept": "*/does_not_matter"})
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Default return is correct
    res = await registry_async_client.get("subjects", headers={"Accept": "*"})
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    res = await registry_async_client.get("subjects", headers={"Accept": "*/*"})
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Octet-stream is supported as a Content-Type
    res = await registry_async_client.put(
        "config", json={"compatibility": "FULL"}, headers={"Content-Type": "application/octet-stream"}
    )
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    res = await registry_async_client.get("subjects", headers={"Accept": "application/octet-stream"})
    assert res.status == 406
    assert res.json()["message"] == HTTPStatus.NOT_ACCEPTABLE.description

    # Parse Content-Type correctly
    res = await registry_async_client.put(
        "config",
        json={"compatibility": "NONE"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json; charset=utf-8"}
    )
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    assert res.json()["compatibility"] == "NONE"

    # Works with other than the default charset
    res = await registry_async_client.put_with_data(
        "config",
        data="{\"compatibility\": \"NONE\"}".encode("utf-16"),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json; charset=utf-16"}
    )
    assert res.status == 200
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    assert res.json()["compatibility"] == "NONE"
    if "SERVER_URI" in os.environ:
        for content_header in [
            {},
            {
                "Content-Type": "application/json"
            },
            {
                "content-type": "application/json"
            },
            {
                "CONTENT-Type": "application/json"
            },
            {
                "coNTEnt-tYPe": "application/json"
            },
        ]:
            path = os.path.join(os.getenv("SERVER_URI"), "subjects/unknown_subject")
            res = requests.request("POST", path, data=b"{}", headers=content_header)
            assert res.status_code == 404, res.content


async def test_schema_body_validation(registry_async_client):
    subject = os.urandom(16).hex()
    post_endpoints = {f"subjects/{subject}", f"subjects/{subject}/versions"}
    for endpoint in post_endpoints:
        # Wrong field name
        res = await registry_async_client.post(endpoint, json={"invalid_field": "invalid_value"})
        assert res.status == 422
        assert res.json()["error_code"] == 422
        assert res.json()["message"] == "Unrecognized field: invalid_field"
        # Additional field
        res = await registry_async_client.post(
            endpoint, json={
                "schema": '{"type": "string"}',
                "invalid_field": "invalid_value"
            }
        )
        assert res.status == 422
        assert res.json()["error_code"] == 422
        assert res.json()["message"] == "Unrecognized field: invalid_field"
        # Invalid body type
        res = await registry_async_client.post(endpoint, json="invalid")
        assert res.status == 500
        assert res.json()["error_code"] == 500
        assert res.json()["message"] == "Internal Server Error"


async def test_version_number_validation(registry_async_client):
    # Create a schema
    subject = os.urandom(16).hex()
    res = await registry_async_client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    version_endpoints = {f"subjects/{subject}/versions/$VERSION", f"subjects/{subject}/versions/$VERSION/schema"}
    for endpoint in version_endpoints:
        # Valid schema id
        res = await registry_async_client.get(endpoint.replace("$VERSION", "1"))
        assert res.status == 200
        # Invalid number
        res = await registry_async_client.get(endpoint.replace("$VERSION", "0"))
        assert res.status == 422
        assert res.json()["error_code"] == 42202
        assert res.json()[
            "message"
        ] == "The specified version is not a valid version id. " \
            "Allowed values are between [1, 2^31-1] and the string \"latest\""
        # Valid latest string
        res = await registry_async_client.get(endpoint.replace("$VERSION", "latest"))
        assert res.status == 200
        # Invalid string
        res = await registry_async_client.get(endpoint.replace("$VERSION", "invalid"))
        assert res.status == 422
        assert res.json()["error_code"] == 42202
        assert res.json()[
            "message"
        ] == "The specified version is not a valid version id. " \
            "Allowed values are between [1, 2^31-1] and the string \"latest\""


async def test_common_endpoints(registry_async_client):
    res = await registry_async_client.get("")
    assert res.status == 200
    assert res.json() == {}


async def test_invalid_namespace(registry_async_client):
    schema = {"type": "record", "name": "foo", "namespace": "foo-bar-baz", "fields": []}
    subject = os.urandom(16).hex()
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.ok, res.json()


async def test_schema_remains_constant(registry_async_client):
    schema = {"type": "record", "name": "foo", "namespace": "foo-bar-baz", "fields": [{"type": "string", "name": "bla"}]}
    subject = os.urandom(16).hex()
    schema_str = jsonlib.dumps(schema)
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": schema_str})
    assert res.ok, res.json()
    scid = res.json()["id"]
    res = await registry_async_client.get(f"schemas/ids/{scid}")
    assert res.json()["schema"] == schema_str


async def test_malformed_kafka_message(registry_async, registry_async_client):
    topic = registry_async.config["topic_name"]

    prod = KafkaProducer(bootstrap_servers=registry_async.config["bootstrap_uri"])
    message_key = {"subject": "foo", "version": 1, "magic": 1, "keytype": "SCHEMA"}
    import random
    schema_id = random.randint(20000, 30000)
    payload = {"schema": jsonlib.dumps({"foo": "bar"}, indent=None, separators=(",", ":"))}
    message_value = {"deleted": False, "id": schema_id, "subject": "foo", "version": 1}
    message_value.update(payload)
    prod.send(topic, key=jsonlib.dumps(message_key).encode(), value=jsonlib.dumps(message_value).encode()).get()

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


async def test_inner_type_compat_failure(registry_async_client):
    sc = {
        "type": "record",
        "name": "record_line_movement_multiple_deleted",
        "namespace": "sya",
        "fields": [{
            "name": "meta",
            "type": {
                "type": "record",
                "name": "meta",
                "fields": [{
                    "name": "date",
                    "type": "long"
                }]
            },
        }]
    }
    ev = {
        "type": "record",
        "name": "record_line_movement_multiple_deleted",
        "namespace": "sya",
        "fields": [{
            "name": "meta",
            "type": {
                "type": "record",
                "name": "meta",
                "fields": [{
                    "name": "date",
                    "type": {
                        "type": "long",
                        "logicalType": "timestamp-millis"
                    }
                }]
            },
        }]
    }
    subject = os.urandom(16).hex()
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(sc)})
    assert res.ok
    sc_id = res.json()["id"]
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(ev)})
    assert res.ok
    assert sc_id != res.json()["id"]


async def test_anon_type_union_failure(registry_async_client):
    schema = {
        "type": "record",
        "name": "record_line_movement_updated",
        "fields": [
            {
                "name": "dependencies",
                "type": [
                    "null", {
                        "type": "record",
                        "name": "record_line_movement_updated_dependencies",
                        "fields": [{
                            "name": "coefficient",
                            "type": ["null", "double"],
                        }]
                    }
                ],
            },
        ]
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
                        "fields": [{
                            "name": "coefficient",
                            "type": ["null", "double"],
                            # This is literally the only diff...
                            "doc": "Coeff of unit product",
                        }]
                    }
                ],
            },
        ]
    }

    subject = os.urandom(16).hex()
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(schema)})
    assert res.ok
    sc_id = res.json()["id"]
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(evolved)})
    assert res.ok
    assert sc_id != res.json()["id"]


@pytest.mark.parametrize("compatibility", ["FULL", "FULL_TRANSITIVE"])
async def test_full_transitive_failure(registry_async_client, compatibility):
    init = {
        "type": "record",
        "name": "order",
        "namespace": "example",
        "fields": [{
            "name": "someField",
            "type": [
                "null", {
                    "type": "record",
                    "name": "someEmbeddedRecord",
                    "namespace": "example",
                    "fields": [{
                        "name": "name",
                        "type": "string"
                    }]
                }
            ],
            "default": "null"
        }]
    }
    evolved = {
        "type": "record",
        "name": "order",
        "namespace": "example",
        "fields": [{
            "name": "someField",
            "type": [
                "null", {
                    "type": "record",
                    "name": "someEmbeddedRecord",
                    "namespace": "example",
                    "fields": [{
                        "name": "name",
                        "type": "string"
                    }, {
                        "name": "price",
                        "type": "int"
                    }]
                }
            ],
            "default": "null"
        }]
    }
    subject = os.urandom(16).hex()
    await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(init)})
    assert res.ok
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": jsonlib.dumps(evolved)})
    assert not res.ok
    assert res.status == 409


async def test_invalid_schemas(registry_async_client):
    subject = os.urandom(16).hex()

    repated_field = {
        "type": "record",
        "name": "myrecord",
        "fields": [{
            "type": "string",
            "name": "name"
        }, {
            "type": "string",
            "name": "name",
            "default": "test"
        }]
    }

    res = await registry_async_client.post(
        f"subjects/{subject}/versions",
        json={"schema": jsonlib.dumps(repated_field)},
    )
    assert res.status != 500, "an invalid schema should not cause a server crash"
    assert not is_success(HTTPStatus(res.status)), "an invalid schema must not be a success"
