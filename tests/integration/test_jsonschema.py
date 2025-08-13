"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import json

import pytest

from karapace.core.client import Client
from karapace.core.compatibility import CompatibilityModes
from karapace.core.schema_reader import SchemaType
from karapace.core.typing import SchemaMetadata, SchemaRuleSet
from tests.schemas.json_schemas import ALL_SCHEMAS
from tests.utils import new_random_name


@pytest.mark.parametrize("compatibility", [CompatibilityModes.FORWARD, CompatibilityModes.BACKWARD, CompatibilityModes.FULL])
@pytest.mark.parametrize("metadata", [None, {}])
@pytest.mark.parametrize("rule_set", [None, {}])
async def test_same_jsonschema_must_have_same_id(
    registry_async_client: Client,
    compatibility: CompatibilityModes,
    metadata: SchemaMetadata,
    rule_set: SchemaRuleSet,
) -> None:
    for schema in ALL_SCHEMAS:
        subject = new_random_name("subject")

        res = await registry_async_client.put(f"config/{subject}", json={"compatibility": compatibility.value})
        assert res.status_code == 200

        first_res = await registry_async_client.post(
            f"subjects/{subject}/versions",
            json={
                "schema": json.dumps(schema.schema),
                "schemaType": SchemaType.JSONSCHEMA.value,
                "metadata": metadata,
                "ruleSet": rule_set,
            },
        )
        assert first_res.status_code == 200
        first_id = first_res.json().get("id")
        assert first_id

        second_res = await registry_async_client.post(
            f"subjects/{subject}/versions",
            json={
                "schema": json.dumps(schema.schema),
                "schemaType": SchemaType.JSONSCHEMA.value,
                "metadata": metadata,
                "ruleSet": rule_set,
            },
        )
        assert second_res.status_code == 200
        assert first_id == second_res.json()["id"]
