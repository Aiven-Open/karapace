"""
karapace - schema tests

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
import json

from karapace.client import Client

baseurl = "http://localhost:8081"


async def test_avro_references(registry_async_client: Client) -> None:
    schema_country = {
        "type": "record",
        "name": "Country",
        "namespace": "com.netapp",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "code", "type": "string"}
        ]
    }

    schema_address = {
        "type": "record",
        "name": "Address",
        "namespace": "com.netapp",
        "fields": [
            {"name": "street", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "postalCode", "type": "string"},
            {"name": "country", "type": "Country"}
        ]

    }

    res = await registry_async_client.post(
        f"subjects/country/versions", json={"schema": json.dumps(schema_country)}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    country_references = [{"name": "country.proto", "subject": "country", "version": 1}]

    res = await registry_async_client.post(
        "subjects/address/versions",
        json={"schemaType": "AVRO", "schema": json.dumps(schema_address), "references": country_references},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    address_id = res.json()["id"]

    # Check if the schema has now been registered under the subject

    res = await registry_async_client.post(
        "subjects/address",
        json={"schemaType": "AVRO", "schema": json.dumps(schema_address), "references": country_references},
    )
    assert res.status_code == 200
    assert "subject" in res.json()
    assert "id" in res.json()
    assert address_id == res.json()["id"]
    assert "version" in res.json()
    assert "schema" in res.json()
