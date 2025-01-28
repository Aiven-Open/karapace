"""
karapace - json schema (with references) tests

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.client import Client, Result
from tests.utils import create_subject_name_factory

import json

baseurl = "http://localhost:8081"

# country.schema.json
SCHEMA_COUNTRY = {
    "$id": "https://example.com/country.schema.json",
    "title": "Country",
    "type": "object",
    "description": "A country of registration",
    "properties": {"name": {"type": "string"}, "code": {"type": "string"}},
    "required": ["name", "code"],
}

# address.schema.json
SCHEMA_ADDRESS = {
    "$id": "https://example.com/address.schema.json",
    "title": "Address",
    "type": "object",
    "properties": {
        "street": {"type": "string"},
        "city": {"type": "string"},
        "postalCode": {"type": "string"},
        "country": {"$ref": "https://example.com/country.schema.json"},
    },
    "required": ["street", "city", "postalCode", "country"],
}

# job.schema.json
SCHEMA_JOB = {
    "$id": "https://example.com/job.schema.json",
    "title": "Job",
    "type": "object",
    "properties": {"title": {"type": "string"}, "salary": {"type": "number"}},
    "required": ["title", "salary"],
}

# person.schema.json
SCHEMA_PERSON = {
    "$id": "https://example.com/person.schema.json",
    "title": "Person",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer"},
        "address": {"$ref": "https://example.com/address.schema.json"},
        "job": {"$ref": "https://example.com/job.schema.json"},
    },
    "required": ["name", "age", "address", "job"],
}

SCHEMA_PERSON_AGE_INT_LONG = {
    "$id": "https://example.com/person.schema.json",
    "title": "Person",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer"},
        "address": {"$ref": "https://example.com/address.schema.json"},
        "job": {"$ref": "https://example.com/job.schema.json"},
    },
    "required": ["name", "age", "address", "job"],
}

SCHEMA_PERSON_AGE_LONG_STRING = {
    "$id": "https://example.com/person.schema.json",
    "title": "Person",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "string"},
        "address": {"$ref": "https://example.com/address.schema.json"},
        "job": {"$ref": "https://example.com/job.schema.json"},
    },
    "required": ["name", "age", "address", "job"],
}

SCHEMA_ADDRESS_INCOMPATIBLE = {
    "$id": "https://example.com/address2.schema.json",
    "title": "ChangedAddress",
    "type": "object",
    "properties": {
        "street2": {"type": "string"},
        "city": {"type": "string"},
        "postalCode": {"type": "string"},
        "country": {"$ref": "https://example.com/country.schema.json"},
    },
    "required": ["street", "city", "postalCode", "country"],
}


def address_references(subject_prefix: str) -> list:
    return [{"name": "country.schema.json", "subject": f"{subject_prefix}country", "version": 1}]


def person_references(subject_prefix: str) -> list:
    return [
        {"name": "address.schema.json", "subject": f"{subject_prefix}address", "version": 1},
        {"name": "job.schema.json", "subject": f"{subject_prefix}job", "version": 1},
    ]


def stored_person_subject(subject_prefix: str, subject_id: int) -> dict:
    return {
        "id": subject_id,
        "references": [
            {"name": "address.schema.json", "subject": f"{subject_prefix}address", "version": 1},
            {"name": "job.schema.json", "subject": f"{subject_prefix}job", "version": 1},
        ],
        "schema": SCHEMA_PERSON,
        "schemaType": "JSON",
        "subject": f"{subject_prefix}person",
        "version": 1,
    }


async def basic_json_references_fill_test(registry_async_client: Client, subject_prefix: str) -> Result:
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}country/versions", json={"schemaType": "JSON", "schema": json.dumps(SCHEMA_COUNTRY)}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.post(
        f"subjects/{subject_prefix}address/versions",
        json={"schemaType": "JSON", "schema": json.dumps(SCHEMA_ADDRESS), "references": address_references(subject_prefix)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    address_id = res.json()["id"]

    # Check if the schema has now been registered under the subject

    res = await registry_async_client.post(
        f"subjects/{subject_prefix}address",
        json={"schemaType": "JSON", "schema": json.dumps(SCHEMA_ADDRESS), "references": address_references(subject_prefix)},
    )
    assert res.status_code == 200
    assert "subject" in res.json()
    assert "id" in res.json()
    assert address_id == res.json()["id"]
    assert "version" in res.json()
    assert "schema" in res.json()

    res = await registry_async_client.post(
        f"subjects/{subject_prefix}job/versions", json={"schemaType": "JSON", "schema": json.dumps(SCHEMA_JOB)}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}person/versions",
        json={"schemaType": "JSON", "schema": json.dumps(SCHEMA_PERSON), "references": person_references(subject_prefix)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    return res


async def test_basic_json_references(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("basic-json-references-")()
    res = await basic_json_references_fill_test(registry_async_client, subject_prefix)
    person_id = res.json()["id"]
    res = await registry_async_client.get(f"subjects/{subject_prefix}country/versions/latest")
    assert res.status_code == 200
    res = await registry_async_client.get(f"subjects/{subject_prefix}person/versions/latest")
    assert res.status_code == 200
    r = res.json()
    r["schema"] = json.loads(r["schema"])
    assert r == stored_person_subject(subject_prefix, person_id)


async def test_json_references_compatibility(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("json-references-compatibility-")()
    await basic_json_references_fill_test(registry_async_client, subject_prefix)

    res = await registry_async_client.post(
        f"compatibility/subjects/{subject_prefix}person/versions/latest",
        json={
            "schemaType": "JSON",
            "schema": json.dumps(SCHEMA_PERSON_AGE_INT_LONG),
            "references": person_references(subject_prefix),
        },
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": True}
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject_prefix}person/versions/latest",
        json={
            "schemaType": "JSON",
            "schema": json.dumps(SCHEMA_PERSON_AGE_LONG_STRING),
            "references": person_references(subject_prefix),
        },
    )
    assert res.status_code == 200
    assert res.json() == {
        "is_compatible": False,
        "messages": ["type Instance.STRING is not compatible with type Instance.INTEGER"],
    }


async def test_json_incompatible_name_references(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("json-references-incompatible-name-")()
    await basic_json_references_fill_test(registry_async_client, subject_prefix)
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}address/versions",
        json={
            "schemaType": "JSON",
            "schema": json.dumps(SCHEMA_ADDRESS_INCOMPATIBLE),
            "references": address_references(subject_prefix),
        },
    )
    assert res.status_code == 409
    msg = (
        "Incompatible schema, compatibility_mode=BACKWARD. Incompatibilities: Restricting acceptable values of "
        "properties is an incompatible change. The following properties street2 accepted any value because of the "
        "lack of validation (the object schema had neither patternProperties nor additionalProperties), "
        "now these values are restricted."
    )
    assert res.json()["message"] == msg
