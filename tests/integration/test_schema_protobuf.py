"""
karapace - schema tests

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.protobuf.kotlin_wrapper import trim_margin
from tests.utils import create_subject_name_factory

import logging
import pytest

baseurl = "http://localhost:8081"


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


# This test ProtoBuf schemas in subject registeration, compatibility of evolved version and querying the schema
# w.r.t. normalization of whitespace and other minor differences to verify equality and inequality comparison of such schemas
@pytest.mark.parametrize("trail", ["", "/"])
async def test_protobuf_schema_normalization(registry_async_client: Client, trail: str) -> None:
    subject = create_subject_name_factory(f"test_protobuf_schema_compatibility-{trail}")()

    res = await registry_async_client.put(f"config/{subject}{trail}", json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

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

    # Same schema with different whitespaces to see API equality comparison works
    original_schema_with_whitespace = trim_margin(
        """
            |syntax = "proto3";
            |
            |package a1;
            |
            |
            |message TestMessage {
            |    message Value {
            |        string str2 = 1;
            |      int32 x = 2;
            |    }
            |  string test = 1;
            |      .a1.TestMessage.Value val = 2;
            |}
            |"""
    )

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    original_id = res.json()["id"]

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id == res.json()["id"], "No duplication"

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema_with_whitespace}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id == res.json()["id"], "No duplication with whitespace differences"

    res = await registry_async_client.post(
        f"subjects/{subject}{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert "schema" in res.json()
    assert original_id == res.json()["id"], "Check returns original id"

    res = await registry_async_client.post(
        f"subjects/{subject}{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema_with_whitespace}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert "schema" in res.json()
    assert original_id == res.json()["id"], "Check returns original id"

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
    assert res.status_code == 200
    assert res.json() == {"is_compatible": True}

    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": evolved_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id != res.json()["id"], "Evolved is not equal"
    evolved_id = res.json()["id"]

    res = await registry_async_client.post(
        f"compatibility/subjects/{subject}/versions/latest{trail}",
        json={"schemaType": "PROTOBUF", "schema": original_schema},
    )
    assert res.json() == {"is_compatible": True}
    assert res.status_code == 200
    res = await registry_async_client.post(
        f"subjects/{subject}/versions{trail}", json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id == res.json()["id"], "Original id again"

    res = await registry_async_client.post(
        f"subjects/{subject}{trail}", json={"schemaType": "PROTOBUF", "schema": evolved_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert "schema" in res.json()
    assert evolved_id == res.json()["id"], "Check returns evolved id"


async def test_protobuf_schema_references(registry_async_client: Client) -> None:
    customer_schema = """
                |syntax = "proto3";
                |package a1;
                |import "Place.proto";
                |import "google/protobuf/duration.proto";
                |import "google/type/color.proto";
                |message Customer {
                |        string name = 1;
                |        int32 code = 2;
                |        Place place = 3;
                |        google.protobuf.Duration dur = 4;
                |        google.type.Color color = 5;
                |}
                |"""

    customer_schema = trim_margin(customer_schema)

    place_schema = """
            |syntax = "proto3";
            |package a1;
            |message Place {
            |        string city = 1;
            |        int32 zone = 2;
            |}
            |"""

    place_schema = trim_margin(place_schema)
    res = await registry_async_client.post(
        "subjects/place/versions", json={"schemaType": "PROTOBUF", "schema": place_schema}
    )
    assert res.status_code == 200

    assert "id" in res.json()

    customer_references = [{"name": "Place.proto", "subject": "place", "version": 1}]
    res = await registry_async_client.post(
        "subjects/customer/versions",
        json={"schemaType": "PROTOBUF", "schema": customer_schema, "references": customer_references},
    )
    assert res.status_code == 200

    assert "id" in res.json()

    original_schema = """
            |syntax = "proto3";
            |package a1;
            |import "Customer.proto";
            |message TestMessage {
            |    enum Enum {
            |       HIGH = 0;
            |       MIDDLE = 1;
            |       LOW = 2;
            |    }
            |    message Value {
            |        message Label{
            |              int32 Id = 1;
            |              string name = 2;
            |        }
            |        Customer customer = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |    oneof page_info {
            |      option (my_option) = true;
            |      int32 page_number = 3;
            |      int32 result_per_page = 4;
            |    }
            |}
            |"""

    original_schema = trim_margin(original_schema)
    references = [{"name": "Customer.proto", "subject": "customer", "version": 1}]
    res = await registry_async_client.post(
        "subjects/test_schema/versions",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": references},
    )
    assert res.status_code == 200

    assert "id" in res.json()

    res = await registry_async_client.get("subjects/customer/versions/latest/referencedby", json={})
    assert res.status_code == 200

    myjson = res.json()
    referents = [3]
    assert not any(x != y for x, y in zip(myjson, referents))

    res = await registry_async_client.get("subjects/place/versions/latest/referencedby", json={})
    assert res.status_code == 200

    res = await registry_async_client.delete("subjects/customer/versions/1")
    assert res.status_code == 404

    match_msg = "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=customer,version=1}"
    myjson = res.json()
    assert myjson["error_code"] == 42206 and myjson["message"] == match_msg

    res = await registry_async_client.delete("subjects/test_schema/versions/1")
    assert res.status_code == 200

    res = await registry_async_client.delete("subjects/test_schema/versions/1")
    myjson = res.json()
    match_msg = "Subject 'test_schema' Version 1 was soft deleted.Set permanent=true to delete permanently"
    assert res.status_code == 404
    assert myjson["error_code"] == 40406 and myjson["message"] == match_msg

    res = await registry_async_client.delete("subjects/customer/versions/1")
    myjson = res.json()
    match_msg = "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=customer,version=1}"
    assert res.status_code == 404
    assert myjson["error_code"] == 42206 and myjson["message"] == match_msg

    res = await registry_async_client.delete("subjects/test_schema/versions/1?permanent=true")
    assert res.status_code == 200

    res = await registry_async_client.delete("subjects/customer/versions/1")
    assert res.status_code == 200


async def test_protobuf_schema_jjaakola_one(registry_async_client: Client) -> None:
    no_ref = """
             |syntax = "proto3";
             |
             |message NoReference {
             |    string name = 1;
             |}
             |"""

    no_ref = trim_margin(no_ref)
    res = await registry_async_client.post("subjects/sub1/versions", json={"schemaType": "PROTOBUF", "schema": no_ref})
    assert res.status_code == 200
    assert "id" in res.json()

    with_first_ref = """
                |syntax = "proto3";
                |
                |import "NoReference.proto";
                |
                |message WithReference {
                |    string name = 1;
                |    NoReference ref = 2;
                |}"""

    with_first_ref = trim_margin(with_first_ref)
    references = [{"name": "NoReference.proto", "subject": "sub1", "version": 1}]

    res = await registry_async_client.post(
        "subjects/sub2/versions",
        json={"schemaType": "PROTOBUF", "schema": with_first_ref, "references": references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    no_ref_second = """
                    |syntax = "proto3";
                    |
                    |message NoReferenceTwo {
                    |    string name = 1;
                    |}
                    |"""

    no_ref_second = trim_margin(no_ref_second)
    res = await registry_async_client.post(
        "subjects/sub3/versions", json={"schemaType": "PROTOBUF", "schema": no_ref_second}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    add_new_ref_in_sub2 = """
                             |syntax = "proto3";
                             |import "NoReference.proto";
                             |import "NoReferenceTwo.proto";
                             |message WithReference {
                             |    string name = 1;
                             |    NoReference ref = 2;
                             |    NoReferenceTwo refTwo = 3;
                             |}
                             |"""

    add_new_ref_in_sub2 = trim_margin(add_new_ref_in_sub2)

    references = [
        {"name": "NoReference.proto", "subject": "sub1", "version": 1},
        {"name": "NoReferenceTwo.proto", "subject": "sub3", "version": 1},
    ]

    res = await registry_async_client.post(
        "subjects/sub2/versions",
        json={"schemaType": "PROTOBUF", "schema": add_new_ref_in_sub2, "references": references},
    )
    assert res.status_code == 200
    assert "id" in res.json()


async def test_protobuf_schema_jjaakola_two(registry_async_client: Client) -> None:
    no_ref = """
             |syntax = "proto3";
             |
             |message NoReference {
             |    string name = 1;
             |}
             |"""

    no_ref = trim_margin(no_ref)
    res = await registry_async_client.post("subjects/sub1/versions", json={"schemaType": "PROTOBUF", "schema": no_ref})
    assert res.status_code == 200
    assert "id" in res.json()

    with_first_ref = """
                |syntax = "proto3";
                |
                |import "NoReference.proto";
                |
                |message WithReference {
                |    string name = 1;
                |    NoReference ref = 2;
                |}"""

    with_first_ref = trim_margin(with_first_ref)
    references = [{"name": "NoReference.proto", "subject": "sub1", "version": 1}]
    res = await registry_async_client.post(
        "subjects/sub2/versions",
        json={"schemaType": "PROTOBUF", "schema": with_first_ref, "references": references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.delete("subjects/sub2/versions/1")
    assert res.status_code == 200
    res = await registry_async_client.get("subjects/sub2/versions/1")
    assert res.status_code == 404
    res = await registry_async_client.delete("subjects/sub2/versions/1")
    assert res.status_code == 404


async def test_protobuf_schema_verifier(registry_async_client: Client) -> None:
    customer_schema = """
            |syntax = "proto3";
            |package a1;
            |message Customer {
            |        string name = 1;
            |        int32 code = 2;
            |}
            |"""

    customer_schema = trim_margin(customer_schema)
    res = await registry_async_client.post(
        "subjects/customer/versions", json={"schemaType": "PROTOBUF", "schema": customer_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    original_schema = """
            |syntax = "proto3";
            |package a1;
            |import "Customer.proto";
            |message TestMessage {
            |    enum Enum {
            |       HIGH = 0;
            |       MIDDLE = 1;
            |       LOW = 2;
            |    }
            |    message Value {
            |        message Label{
            |              int32 Id = 1;
            |              string name = 2;
            |        }
            |        Customer customer = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |    TestMessage.Value valx = 3;
            |
            |    oneof page_info {
            |      option (my_option) = true;
            |      int32 page_number = 5;
            |      int32 result_per_page = 6;
            |    }
            |}
            |"""

    original_schema = trim_margin(original_schema)
    references = [{"name": "Customer.proto", "subject": "customer", "version": 1}]
    res = await registry_async_client.post(
        "subjects/test_schema/versions",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": references},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    res = await registry_async_client.get("subjects/customer/versions/latest/referencedby", json={})
    assert res.status_code == 200
    myjson = res.json()
    referents = [2]
    assert not any(x != y for x, y in zip(myjson, referents))

    res = await registry_async_client.delete("subjects/customer/versions/1")
    assert res.status_code == 404
    match_msg = "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=customer,version=1}"
    myjson = res.json()
    assert myjson["error_code"] == 42206 and myjson["message"] == match_msg

    res = await registry_async_client.delete("subjects/test_schema/versions/1")
    assert res.status_code == 200

    res = await registry_async_client.delete("subjects/customer/versions/1")
    assert res.status_code == 404

    res = await registry_async_client.delete("subjects/test_schema/versions/1?permanent=true")
    assert res.status_code == 200

    res = await registry_async_client.delete("subjects/customer/versions/1")
    assert res.status_code == 200
