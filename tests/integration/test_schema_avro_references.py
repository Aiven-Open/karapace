"""
karapace - schema tests

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.client import Client, Result
from tests.utils import create_subject_name_factory

import json

baseurl = "http://localhost:8081"

# country.avsc
SCHEMA_COUNTRY = {
    "type": "record",
    "name": "Country",
    "namespace": "com.netapp",
    "fields": [{"name": "name", "type": "string"}, {"name": "code", "type": "string"}],
}

# address.avsc
SCHEMA_ADDRESS = {
    "type": "record",
    "name": "Address",
    "namespace": "com.netapp",
    "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "postalCode", "type": "string"},
        {"name": "country", "type": "Country"},
    ],
}

# job.avsc
SCHEMA_JOB = {
    "type": "record",
    "name": "Job",
    "namespace": "com.netapp",
    "fields": [{"name": "title", "type": "string"}, {"name": "salary", "type": "double"}],
}

# person.avsc
SCHEMA_PERSON = {
    "type": "record",
    "name": "Person",
    "namespace": "com.netapp",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        # {"name": "address", "type": "Address"},
        {"name": "job", "type": "Job"},
    ],
}

SCHEMA_PERSON_RECURSIVE = {
    "type": "record",
    "name": "PersonRecursive",
    "namespace": "com.netapp",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "job", "type": "Job"},
        {"name": "father", "type": "PersonRecursive"},
    ],
}

SCHEMA_JOB_INDIRECT_RECURSIVE = {
    "type": "record",
    "name": "JobIndirectRecursive",
    "namespace": "com.netapp",
    "fields": [
        {"name": "title", "type": "string"},
        {"name": "salary", "type": "double"},
        {"name": "consultant", "type": "Person"},
    ],
}


SCHEMA_PERSON_AGE_INT_LONG = {
    "type": "record",
    "name": "Person",
    "namespace": "com.netapp",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "long"},
        # {"name": "address", "type": "Address"},
        {"name": "job", "type": "Job"},
    ],
}

SCHEMA_PERSON_AGE_LONG_STRING = {
    "type": "record",
    "name": "Person",
    "namespace": "com.netapp",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "string"},
        # {"name": "address", "type": "Address"},
        {"name": "job", "type": "Job"},
    ],
}

SCHEMA_UNION_REFERENCES = {
    "type": "record",
    "namespace": "com.netapp",
    "name": "Person2",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        # {"name": "address", "type": "Address"},
        {"name": "job", "type": "Job"},
        {
            "name": "children",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "child",
                    "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
                },
            ],
        },
    ],
}

SCHEMA_UNION_REFERENCES2 = [
    {
        "type": "record",
        "name": "Person",
        "namespace": "com.netapp",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            # {"name": "address", "type": "Address"},
            {"name": "job", "type": "Job"},
        ],
    },
    {
        "type": "record",
        "name": "UnemployedPerson",
        "namespace": "com.netapp",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            # {"name": "address", "type": "Address"},
        ],
    },
]

SCHEMA_ADDRESS_INCOMPATIBLE = {
    "type": "record",
    "name": "ChangedAddress",
    "namespace": "com.netapp",
    "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "postalCode", "type": "string"},
        {"name": "country", "type": "Country"},
    ],
}


def address_references(subject_prefix: str) -> list:
    return [{"name": "country.avsc", "subject": f"{subject_prefix}country", "version": 1}]


def person_references(subject_prefix: str) -> list:
    return [
        # {"name": "address.avsc", "subject": f"{subject_prefix}address", "version": 1},
        {"name": "job.avsc", "subject": f"{subject_prefix}job", "version": 1},
    ]


def job_indirect_recursive_references(subject_prefix: str) -> list:
    return [
        {"name": "person.avsc", "subject": f"{subject_prefix}person", "version": 1},
    ]


def stored_person_subject(subject_prefix: str, subject_id: int) -> dict:
    return {
        "id": subject_id,
        "references": [
            # {"name": "address.avsc", "subject": f"{subject_prefix}address", "version": 1},
            {"name": "job.avsc", "subject": f"{subject_prefix}job", "version": 1},
        ],
        "schema": json.dumps(
            {
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    # {"name": "address", "type": "Address"},
                    {"name": "job", "type": "Job"},
                ],
                "name": "Person",
                "namespace": "com.netapp",
                "type": "record",
            },
            separators=(",", ":"),
        ),
        "subject": f"{subject_prefix}person",
        "version": 1,
    }


async def basic_avro_references_fill_test(registry_async_client: Client, subject_prefix: str) -> Result:
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}country/versions", json={"schema": json.dumps(SCHEMA_COUNTRY)}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.post(
        f"subjects/{subject_prefix}address/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(SCHEMA_ADDRESS), "references": address_references(subject_prefix)},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    address_id = res.json()["id"]

    # Check if the schema has now been registered under the subject

    res = await registry_async_client.post(
        f"subjects/{subject_prefix}address",
        json={"schemaType": "AVRO", "schema": json.dumps(SCHEMA_ADDRESS), "references": address_references(subject_prefix)},
    )
    assert res.status_code == 200
    assert "subject" in res.json()
    assert "id" in res.json()
    assert address_id == res.json()["id"]
    assert "version" in res.json()
    assert "schema" in res.json()

    res = await registry_async_client.post(f"subjects/{subject_prefix}job/versions", json={"schema": json.dumps(SCHEMA_JOB)})
    assert res.status_code == 200
    assert "id" in res.json()

    res = await registry_async_client.post(
        f"subjects/{subject_prefix}person/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(SCHEMA_PERSON), "references": person_references(subject_prefix)},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    return res


async def test_basic_avro_references(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("basic-avro-references-")()
    res = await basic_avro_references_fill_test(registry_async_client, subject_prefix)
    person_id = res.json()["id"]
    res = await registry_async_client.get(f"subjects/{subject_prefix}person/versions/latest")
    assert res.status_code == 200
    assert res.json() == stored_person_subject(subject_prefix, person_id)


async def test_avro_references_compatibility(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("avro-references-compatibility-")()
    await basic_avro_references_fill_test(registry_async_client, subject_prefix)

    res = await registry_async_client.post(
        f"compatibility/subjects/{subject_prefix}person/versions/latest",
        json={
            "schemaType": "AVRO",
            "schema": json.dumps(SCHEMA_PERSON_AGE_INT_LONG),
            "references": person_references(subject_prefix),
        },
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": True}
    res = await registry_async_client.post(
        f"compatibility/subjects/{subject_prefix}person/versions/latest",
        json={
            "schemaType": "AVRO",
            "schema": json.dumps(SCHEMA_PERSON_AGE_LONG_STRING),
            "references": person_references(subject_prefix),
        },
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": False, "messages": ["reader type: string not compatible with writer type: int"]}


async def test_avro_union_references(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("avro-references-union-one-")()
    await basic_avro_references_fill_test(registry_async_client, subject_prefix)
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}person2/versions",
        json={
            "schemaType": "AVRO",
            "schema": json.dumps(SCHEMA_UNION_REFERENCES),
            "references": person_references(subject_prefix),
        },
    )
    assert res.status_code == 200
    assert "id" in res.json()


async def test_avro_union_references2(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("avro-references-union-two-")()
    await basic_avro_references_fill_test(registry_async_client, subject_prefix)
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}person2/versions",
        json={
            "schemaType": "AVRO",
            "schema": json.dumps(SCHEMA_UNION_REFERENCES2),
            "references": person_references(subject_prefix),
        },
    )
    assert res.status_code == 200 and "id" in res.json()


async def test_avro_incompatible_name_references(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("avro-references-incompatible-name-")()
    await basic_avro_references_fill_test(registry_async_client, subject_prefix)
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}address/versions",
        json={
            "schemaType": "AVRO",
            "schema": json.dumps(SCHEMA_ADDRESS_INCOMPATIBLE),
            "references": address_references(subject_prefix),
        },
    )
    assert res.status_code == 409
    msg = "Incompatible schema, compatibility_mode=BACKWARD. Incompatibilities: expected: com.netapp.Address"
    assert res.json()["message"] == msg


async def test_recursive_reference(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("avro-recursive-reference")()
    await basic_avro_references_fill_test(registry_async_client, subject_prefix)
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}person-recursive/versions",
        json={
            "schemaType": "AVRO",
            "schema": json.dumps(SCHEMA_PERSON_RECURSIVE),
            "references": person_references(subject_prefix),
        },
    )
    assert res.status_code == 200
    assert "id" in res.json()


# This test fails because indirect references are not implemented
async def test_indirect_recursive_reference(registry_async_client: Client) -> None:
    subject_prefix = create_subject_name_factory("avro-indirect-recursive-reference")()
    await basic_avro_references_fill_test(registry_async_client, subject_prefix)
    res = await registry_async_client.post(
        f"subjects/{subject_prefix}person-indirect-recursive/versions",
        json={
            "schemaType": "AVRO",
            "schema": json.dumps(SCHEMA_JOB_INDIRECT_RECURSIVE),
            "references": job_indirect_recursive_references(subject_prefix),
        },
    )
    assert res.status_code == 200
    assert "id" in res.json()
