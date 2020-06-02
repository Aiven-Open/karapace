from jsonschema import Draft7Validator
from karapace.compatibility import IncompatibleSchema, JsonSchemaCompatibility

import copy
import json as jsonlib
import os
import pytest


async def check_jsonschema_succeeds(new: dict, old: dict, compatibility: str, client=None):
    if not client:
        for schema in [new, old]:
            Draft7Validator.check_schema(schema)
        JsonSchemaCompatibility(new, old, compatibility).check()
    else:
        subject = os.urandom(16).hex()
        res = await client.put("config", json={"compatibility": compatibility})
        assert res.status == 200
        res = await client.post(
            "subjects/{}/versions".format(subject),
            json={
                "schema": jsonlib.dumps(old),
                "schemaType": "JSON"
            },
        )
        assert res.status == 200
        assert "id" in res.json()
        schema_id = res.json()["id"]
        res = await client.post(
            "subjects/{}/versions".format(subject),
            json={
                "schema": jsonlib.dumps(new),
                "schemaType": "JSON"
            },
        )
        assert res.status == 200
        assert "id" in res.json()
        new_id = res.json()["id"]
        assert new_id != schema_id
        await client.delete("subjects/{}".format(subject))


async def check_jsonschema_fails(new: dict, old: dict, compatibility: str, client=None):
    if not client:
        for schema in [new, old]:
            Draft7Validator.check_schema(schema)
        with pytest.raises(IncompatibleSchema):
            JsonSchemaCompatibility(new, old, compatibility).check()
    else:
        subject = os.urandom(16).hex()
        res = await client.put("config", json={"compatibility": compatibility})
        assert res.status == 200
        res = await client.post(
            "subjects/{}/versions".format(subject),
            json={
                "schema": jsonlib.dumps(old),
                "schemaType": "JSON"
            },
        )
        assert res.status == 200
        assert "id" in res.json()
        res = await client.post(
            "subjects/{}/versions".format(subject),
            json={
                "schema": jsonlib.dumps(new),
                "schemaType": "JSON"
            },
        )
        await client.delete("subjects/{}".format(subject))
        assert res.status == 409


async def test_jsonschema_compatibility():
    # uncomment and delete the parameter / fixture to do compatibility updates
    registry_async_client = None
    import logging
    logging.basicConfig(level=logging.DEBUG)
    source = {"type": "object", "properties": {"foo": {"type": "number"}}}
    target = {
        "type": "object",
        "properties": {
            "foo": {
                "type": "integer"
            },
        },
    }
    # some basic checks for required fields
    await check_jsonschema_succeeds(source, target, "BACKWARD", registry_async_client)
    # base data types in top level schema
    await check_jsonschema_succeeds({"type": "string"}, {"type": "string"}, "NONE")
    # Base
    # Number types
    # int to number promotion
    await check_jsonschema_succeeds({"type": "number"}, {"type": "integer"}, "BACKWARD", registry_async_client)
    # invalid type promotion
    await check_jsonschema_fails({"type": "number"}, {"type": "integer"}, "FORWARD", registry_async_client)
    # multiple promotion
    await check_jsonschema_succeeds({
        "type": "number",
        "multipleOf": 2
    }, {
        "type": "number",
        "multipleOf": 4
    }, "BACKWARD", registry_async_client)
    # invalid multiple promotion
    await check_jsonschema_fails({
        "type": "number",
        "multipleOf": 2
    }, {
        "type": "number",
        "multipleOf": 4
    }, "FORWARD", registry_async_client)
    # inclusive domain
    await check_jsonschema_succeeds({
        "type": "number",
        "minimum": -10,
        "maximum": 100
    }, {
        "type": "number",
        "minimum": -9,
        "maximum": 9
    }, "BACKWARD", registry_async_client)
    # overlapping but not inclusive
    await check_jsonschema_fails({
        "type": "number",
        "minimum": -10,
        "maximum": 100
    }, {
        "type": "number",
        "minimum": -19,
        "maximum": 9
    }, "BACKWARD", registry_async_client)
    # String
    # min len
    await check_jsonschema_succeeds({
        "type": "string",
        "minLength": 8
    }, {
        "type": "string",
        "minLength": 9
    }, "BACKWARD", registry_async_client)
    await check_jsonschema_fails({
        "type": "string",
        "minLength": 10
    }, {
        "type": "string",
        "minLength": 9
    }, "BACKWARD", registry_async_client)
    # max len
    await check_jsonschema_succeeds({
        "type": "string",
        "maxLength": 18
    }, {
        "type": "string",
        "maxLength": 10
    }, "BACKWARD", registry_async_client)
    await check_jsonschema_fails({
        "type": "string",
        "maxLength": 10
    }, {
        "type": "string",
        "maxLength": 19
    }, "BACKWARD", registry_async_client)
    # format or pattern
    # TODO -> check if format is supported by confluent
    for k in ["pattern"]:
        await check_jsonschema_fails({
            "type": "string",
            k: "foo"
        }, {
            "type": "string",
            k: "bar"
        }, "BACKWARD", registry_async_client)
    # Complex
    # Array
    src = {"type": "array", "items": {"type": "number"}}
    tgt = {"type": "array", "items": {"type": "integer"}}
    await check_jsonschema_succeeds(src, tgt, "BACKWARD", registry_async_client)
    await check_jsonschema_fails(src, tgt, "FORWARD", registry_async_client)
    for minI, maxI, arr in [(10, 20, src), (11, 19, tgt)]:
        arr["minItems"] = minI
        arr["maxItems"] = maxI
    await check_jsonschema_succeeds(src, tgt, "BACKWARD", registry_async_client)
    for minI, maxI, arr in [(12, 18, src), (11, 19, tgt)]:
        arr["minItems"] = minI
        arr["maxItems"] = maxI
    await check_jsonschema_fails(src, tgt, "BACKWARD", registry_async_client)
    await check_jsonschema_succeeds({
        "type": "array",
        "items": {
            "type": "integer"
        }
    }, {
        "type": "array",
        "items": {
            "type": "integer"
        },
        "uniqueItems": True
    }, "BACKWARD", registry_async_client)
    # Object
    src = {"type": "object", "properties": {"some_num": {"type": "number"}}}
    tgt = {
        "type": "object",
        "properties": {
            "some_num": {
                "type": "integer"
            }
        },
    }
    base_src, base_tgt = copy.deepcopy(src), copy.deepcopy(tgt)
    await check_jsonschema_succeeds(src, tgt, "BACKWARD", registry_async_client)
    src["properties"]["some_str"] = {"type": "string"}
    src["properties"]["some_num"]["type"] = "integer"
    await check_jsonschema_fails(tgt, src, "FORWARD", registry_async_client)
    tgt["additionalProperties"] = {"type": "integer"}
    src["additionalProperties"] = {"type": "number"}
    for minP, maxP, obj in [(7, 17, base_src), (8, 16, base_tgt)]:
        obj["minProperties"] = minP
        obj["maxProperties"] = maxP
    await check_jsonschema_succeeds(base_src, base_tgt, "BACKWARD", registry_async_client)
    for minP, maxP, obj in [(7, 17, base_src), (6, 16, base_tgt)]:
        obj["minProperties"] = minP
        obj["maxProperties"] = maxP
    await check_jsonschema_fails(base_src, base_tgt, "BACKWARD", registry_async_client)
    await check_jsonschema_fails(base_src, base_tgt, "FORWARD", registry_async_client)
    src = {
        "type": "object",
        "properties": {
            "some_obj_num": {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "number"
                    }
                },
                "required": ["foo"]
            },
            "some_str": {
                "type": "string"
            }
        },
        "required": ["some_obj_num", "some_str"]
    }
    tgt = {
        "type": "object",
        "properties": {
            "some_obj_num": {
                "type": "object",
                "properties": {
                    "foo": {
                        "type": "integer"
                    }
                },
                "required": ["foo"]
            },
            "some_str": {
                "type": "string"
            }
        },
        "required": ["some_obj_num", "some_str"]
    }
    await check_jsonschema_succeeds(src, tgt, "BACKWARD", registry_async_client)
    await check_jsonschema_fails(src, tgt, "FORWARD", registry_async_client)
    # Union (apparently you cannot have complex types in unions, so ... yay?)
    # mangled order does not work as expected :(
    src = {"type": ["string", "integer"]}
    tgt = {"type": ["string", "integer", "boolean"]}
    await check_jsonschema_succeeds(src, tgt, "FORWARD", registry_async_client)
    src = {"type": ["string", "integer"]}
    tgt = {"type": ["string", "number"]}
    await check_jsonschema_fails({"type": ["string", "integer"]}, {"type": ["integer", "string"]}, "BACKWARD",
                                 registry_async_client)
    await check_jsonschema_succeeds(src, tgt, "FORWARD", registry_async_client)
    src = {
        "type": "object",
        "properties": {
            "num": {
                "type": "number"
            },
            "uni": {
                "type": ["string", "number"]
            }
        },
        "required": ["num", "uni"]
    }
    tgt = {
        "type": "object",
        "properties": {
            "num": {
                "type": "integer"
            },
            "uni": {
                "type": ["string", "number"]
            }
        },
        "required": ["num", "uni"]
    }
    await check_jsonschema_succeeds(src, tgt, "BACKWARD", registry_async_client)
