"""
karapace - schema tests

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import json as jsonlib
import os

import pytest

from karapace.karapace import Karapace
from .utils import Client

pytest_plugins = "aiohttp.pytest_plugin"
baseurl = "http://localhost:8081"


def create_service(datadir, kafka_server):
    config_path = os.path.join(datadir, "karapace_config.json")
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

    schema3 = {
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
        ]
    }
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": jsonlib.dumps(schema3)},
    )
    # Fails because field removed
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


async def test_local(session_tmpdir, kafka_server, aiohttp_client):
    datadir = session_tmpdir()

    kc = create_service(datadir, kafka_server)
    client = await aiohttp_client(kc.app)
    c = Client(client=client)

    await schema_checks(c)
    await compatibility_endpoint_checks(c)
    await record_schema_compatibility_checks(c)
    await record_nested_schema_compatibility_checks(c)
    for compatibility in {"FORWARD", "BACKWARD", "FULL"}:
        await enum_schema_compatibility_checks(c, compatibility)
    await config_checks(c)
    kc.close()


async def test_remote():
    server_uri = os.environ.get("SERVER_URI")
    if not server_uri:
        pytest.skip("SERVER_URI env variable not set")
    c = Client(server_uri=os.environ.get("SERVER_URI"))
    await schema_checks(c)
    await compatibility_endpoint_checks(c)
    await record_schema_compatibility_checks(c)
    await record_nested_schema_compatibility_checks(c)
    for compatibility in {"FORWARD", "BACKWARD", "FULL"}:
        await enum_schema_compatibility_checks(c, compatibility)
    await config_checks(c)
