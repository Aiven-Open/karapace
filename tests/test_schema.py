"""
karapace - schema tests

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from .utils import Client
from karapace.karapace import Karapace

import json as jsonlib
import os
import pytest

pytest_plugins = "aiohttp.pytest_plugin"
baseurl = "http://localhost:8081"


def create_service(datadir, kafka_server):
    config_path = os.path.join(str(datadir), "karapace_config.json")
    with open(config_path, "w") as fp:
        karapace_config = {"log_level": "INFO", "bootstrap_uri": "127.0.0.1:{}".format(kafka_server["kafka_port"])}
        fp.write(jsonlib.dumps(karapace_config))
    kc = Karapace(config_path)
    return kc


async def enum_schema_compatibility_checks(c, compatibility):
    subject = os.urandom(16).hex()

    res = await c.put("config", json={"compatibility": compatibility})
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id3 = res.json()["id"]
    assert schema_id3 != schema_id2

    res = await c.get("schemas/ids/{}".format(schema_id3))
    assert res.status_code == 200
    res = jsonlib.loads(res.json()["schema"])
    assert res["type"] == "record"
    assert res["name"] == "myenumtest"
    assert res["fields"][0]["name"] == "faa"
    assert res["fields"][0]["type"]["type"] == "enum"
    assert res["fields"][0]["type"]["name"] == "enumtest"
    assert res["fields"][0]["type"]["symbols"] == ["second"]


async def record_nested_schema_compatibility_checks(c):
    subject = os.urandom(16).hex()

    res = await c.put("config", json={"compatibility": "BACKWARD"})
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert "id" in res.json()

    # change string to integer in the nested record, should fail
    schema["fields"][1]["type"]["fields"][0]["type"] = "int"
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 409


async def compatibility_endpoint_checks(c):
    res = await c.put("config", json={"compatibility": "BACKWARD"})
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

    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200

    res = await c.get("schemas/ids/{}".format(res.json()["id"]))
    schema_gotten_back = jsonlib.loads(res.json()["schema"])
    assert schema_gotten_back == schema

    # replace int with long
    schema["fields"] = [{"type": "long", "name": "age"}]
    res = await c.post(
        "compatibility/subjects/{}/versions/latest".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert res.json() == {"is_compatible": True}

    schema["fields"] = [{"type": "string", "name": "age"}]
    res = await c.post(
        "compatibility/subjects/{}/versions/latest".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert res.json() == {"is_compatible": False}


async def check_type_compatibility(c):
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
        res = await c.put(f"config/{subject}", json={"compatibility": compatibility})
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
        res = await c.post(
            f"subjects/{subject}/versions",
            json={"schema": jsonlib.dumps(schema)},
        )
        assert res.status == 200

        schema["fields"][0]["type"] = target_type
        res = await c.post(
            f"compatibility/subjects/{subject}/versions/latest",
            json={"schema": jsonlib.dumps(schema)},
        )
        assert res.status == 200
        assert res.json() == {"is_compatible": expected}


async def record_schema_compatibility_checks(c):
    subject = os.urandom(16).hex()

    res = await c.put("config", json={"compatibility": "FORWARD"})
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

    res = await c.post(
        "subjects/{}/versions".format(subject),
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema3a)},
    )
    # Fails because field removed
    assert res.status == 409
    assert res.json() == {"error_code": 409, "message": "Schema being registered is incompatible with an earlier schema"}

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
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema3b)},
    )
    # Fails because incompatible type change
    assert res.status == 409
    assert res.json() == {"error_code": 409, "message": "Schema being registered is incompatible with an earlier schema"}

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
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema4)},
    )
    assert res.status == 200

    res = await c.put("config", json={"compatibility": "BACKWARD"})
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema5)},
    )
    assert res.status == 409

    # Add a default value for the field
    schema5["fields"][3] = {"name": "fourth_name", "type": "string", "default": "foof"}
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema5)},
    )
    assert res.status == 200
    assert "id" in res.json()

    # Try to submit schema with a different definition
    schema5["fields"][3] = {"name": "fourth_name", "type": "int", "default": 2}
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema5)},
    )
    assert res.status == 409


async def check_transitive_compatibility(c):
    subject = os.urandom(16).hex()
    res = await c.put(f"config/{subject}", json={"compatibility": "BACKWARD_TRANSITIVE"})
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
    res = await c.post(
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
    res = await c.post(
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
    res = await c.post(
        f"subjects/{subject}/versions",
        json={"schema": jsonlib.dumps(schema2)},
    )
    assert res.status == 409
    assert res.json() == {"error_code": 409, "message": "Schema being registered is incompatible with an earlier schema"}


async def schema_checks(c):
    subject = os.urandom(16).hex()
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    res = await c.get("schemas/ids/{}".format(res.json()["id"]))
    assert res.status_code == 200
    assert res.json()["schema"] == '"string"'

    # repost same schema again to see that a new id is not generated but an old one is given back
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"]

    # nonexistent schema id
    result = await c.get(os.path.join("schemas/ids/{}".format(123456789)))
    assert result.json()["error_code"] == 40403

    res = await c.post(
        "subjects/{}/versions".format(subject), json={"schema": "{\"type\": \"string\", \"foo\": \"string\"}"}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id != res.json()["id"]

    # Fetch the schema back to see how it was mangled
    result = await c.get(os.path.join("schemas/ids/{}".format(res.json()["id"])))
    schema = jsonlib.loads(result.json()["schema"])
    assert schema["type"] == "string"
    assert schema["foo"] == "string"

    res = await c.get("subjects")
    assert res.status_code == 200
    assert subject in res.json()

    res = await c.get("subjects/{}/versions".format(subject))
    assert res.status_code == 200
    assert res.json() == [1, 2]

    res = await c.get("subjects/{}/versions/1".format(subject))
    assert res.status_code == 200
    assert res.json()["subject"] == subject
    assert res.json()["schema"] == '"string"'

    # Find an invalid version 0
    res = await c.get("subjects/{}/versions/0".format(subject))
    assert res.status_code == 422
    assert res.json()["error_code"] == 42202
    assert res.json()["message"] == \
        'The specified version is not a valid version id. Allowed values are between [1, 2^31-1] and the string "latest"'

    # Find an invalid version (too large)
    res = await c.get("subjects/{}/versions/15".format(subject))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402
    assert res.json()["message"] == "Version not found."

    # Delete an actual version
    res = await c.delete("subjects/{}/versions/1".format(subject))
    assert res.status_code == 200
    assert res.json() == 1

    # Delete a whole subject
    res = await c.delete("subjects/{}".format(subject))
    assert res.status_code == 200
    assert res.json() == [2]

    # List all subjects, our subject shouldn't be in the list
    res = await c.get("subjects")
    assert res.status_code == 200
    assert subject not in res.json()

    # After deleting the last version of a subject, it shouldn't be in the list
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    res = await c.get("subjects")
    assert subject in res.json()
    res = await c.get("subjects/{}/versions".format(subject))
    assert res.json() == [3]
    res = await c.delete("subjects/{}/versions/3".format(subject))
    assert res.status_code == 200
    res = await c.get("subjects")
    assert subject not in res.json()

    res = await c.get("subjects/{}/versions".format(subject))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."
    res = await c.get("subjects/{}/versions/latest".format(subject))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."

    # Creating a new schema works after deleting the only available version
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    res = await c.get("subjects/{}/versions".format(subject))
    assert res.json() == [4]

    # Check version number generation when deleting an entire subjcect
    subject = os.urandom(16).hex()
    res = await c.put("config/{}".format(subject), json={"compatibility": "NONE"})
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
    res = await c.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
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
    res = await c.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema2)})
    assert res.status == 200
    assert "id" in res.json()
    res = await c.get("subjects/{}/versions".format(subject))
    assert res.status == 200
    assert res.json() == [1, 2]
    res = await c.delete("subjects/{}".format(subject))
    assert res.status == 200
    # Recreate subject
    res = await c.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    res = await c.get("subjects/{}/versions".format(subject))
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
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    res = await c.get("subjects/{}/versions/1".format(subject))
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
    res = await c.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    assert "id" in res.json()
    original_schema_id = res.json()["id"]
    # New subject with the same schema
    subject = os.urandom(16).hex()
    res = await c.post("subjects/{}/versions".format(subject), json={"schema": jsonlib.dumps(schema)})
    assert res.status == 200
    assert "id" in res.json()
    new_schema_id = res.json()["id"]
    assert original_schema_id == new_schema_id

    # It also works for multiple versions in a single subject
    subject = os.urandom(16).hex()
    res = await c.put(
        "config/{}".format(subject), json={"compatibility": "NONE"}
    )  # We don't care about the compatibility in this test
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema)},
    )
    assert res.status == 200
    assert res.json()["id"] == new_schema_id  # Same ID as in the previous test step


async def config_checks(c):
    # Tests /config endpoint
    res = await c.put("config", json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await c.get("config")
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await c.put("config", json={"compatibility": "NONE"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "NONE"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await c.put("config", json={"compatibility": "nonexistentmode"})
    assert res.status_code == 422
    assert res.json()["error_code"] == 42203
    assert res.json()["message"] == "Invalid compatibility level. Valid values are none, backward, forward and full"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # Create a new subject so we can try setting its config
    subject = os.urandom(16).hex()
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await c.get("config/{}".format(subject))
    assert res.status_code == 404
    assert res.json()["error_code"] == 40401
    assert res.json()["message"] == "Subject not found."

    res = await c.put("config/{}".format(subject), json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    res = await c.get("config/{}".format(subject))
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"

    # It's possible to add a config to a subject that doesn't exist yet
    subject = os.urandom(16).hex()
    res = await c.put("config/{}".format(subject), json={"compatibility": "FULL"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "FULL"
    assert res.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"

    # The subject doesn't exist from the schema point of view
    res = await c.get("subjects/{}/versions".format(subject))
    assert res.status_code == 404

    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await c.get("config/{}".format(subject))
    assert res.status_code == 200
    assert res.json()["compatibilityLevel"] == "FULL"


async def run_schema_tests(c):
    await schema_checks(c)
    await check_type_compatibility(c)
    await compatibility_endpoint_checks(c)
    await record_schema_compatibility_checks(c)
    await record_nested_schema_compatibility_checks(c)
    for compatibility in {"FORWARD", "BACKWARD", "FULL"}:
        await enum_schema_compatibility_checks(c, compatibility)
    await config_checks(c)
    await check_transitive_compatibility(c)


async def test_local(session_tmpdir, kafka_server, aiohttp_client):
    datadir = session_tmpdir()
    kc = create_service(datadir, kafka_server)
    client = await aiohttp_client(kc.app)
    c = Client(client=client)
    await run_schema_tests(c)
    kc.close()


async def test_remote():
    server_uri = os.environ.get("SERVER_URI")
    if not server_uri:
        pytest.skip("SERVER_URI env variable not set")
    c = Client(server_uri=server_uri)
    await run_schema_tests(c)
