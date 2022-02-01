"""
karapace - schema tests

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.utils import Client
from tests.utils import create_subject_name_factory

import json
import logging
import pytest
import requests

baseurl = "http://localhost:8081"

compatibility_test_url = (
    "https://raw.githubusercontent.com/confluentinc/schema-registry/"
    + "0530b0107749512b997f49cc79fe423f21b43b87/"
    + "protobuf-provider/src/test/resources/diff-schema-examples.json"
)


def add_slashes(text: str) -> str:
    escape_dict = {
        "\a": "\\a",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
        "\v": "\\v",
        "'": "\\'",
        '"': '\\"',
        "\\": "\\\\",
    }
    trans_table = str.maketrans(escape_dict)
    return text.translate(trans_table)


log = logging.getLogger(__name__)


@pytest.mark.parametrize("trail", ["", "/"])
async def test_protobuf_schema_compatibility(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_protobuf_schema_compatibility-{trail}")()

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status == 200

    original_schema = """
            |syntax = "proto3";
            |package a1;
            |message TestMessage {
            |    message Value {
            |        string str2 = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""

    original_schema = trim_margin(original_schema)

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status == 200
    assert "id" in res.json()

    evolved_schema = """
            |syntax = "proto3";
            |package a1;
            |message TestMessage {
            |    message Value {
            |        string str2 = 1;
            |        Enu x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |    enum Enu {
            |        A = 0;
            |        B = 1;
            |    }
            |}
            |"""
    evolved_schema = trim_margin(evolved_schema)

    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema},
    )
    assert res.status == 200
    assert res.json() == {"is_compatible": True}

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": evolved_schema}
    )
    assert res.status == 200
    assert "id" in res.json()

    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": original_schema},
    )
    assert res.json() == {"is_compatible": True}
    assert res.status == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status == 200
    assert "id" in res.json()


class Schemas:
    url = requests.get(compatibility_test_url)
    sch = json.loads(url.text)
    schemas = {}
    descriptions = []
    max_count = 120
    count = 0
    for a in sch:
        # if a["description"] == "Detect compatible add field to oneof":
        descriptions.append(a["description"])
        schemas[a["description"]] = dict(a)
        count += 1
        if a["description"] == "Detect incompatible message index change":
            break
        if count == max_count:
            break


@pytest.mark.parametrize("trail", ["", "/"])
@pytest.mark.parametrize("desc", Schemas.descriptions)
async def test_schema_registry_examples(registry_async_client: Client, trail: str, desc) -> None:
    subject = create_subject_name_factory(f"test_protobuf_schema_compatibility-{trail}")()

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status == 200

    description = desc

    schema = Schemas.schemas[description]
    original_schema = schema["original_schema"]
    evolved_schema = schema["update_schema"]
    compatible = schema["compatible"]

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status == 200
    assert "id" in res.json()

    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema},
    )
    assert res.status == 200
    assert res.json() == {"is_compatible": compatible}

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": evolved_schema}
    )

    if compatible:
        assert res.status == 200
        assert "id" in res.json()
    else:
        assert res.status == 409
