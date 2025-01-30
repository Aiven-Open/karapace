"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import json

import pytest
from jsonschema import Draft7Validator

from karapace.core.client import Client
from karapace.core.compatibility import CompatibilityModes
from karapace.core.schema_reader import SchemaType
from karapace.core.typing import SchemaMetadata, SchemaRuleSet
from tests.schemas.json_schemas import (
    A_DINT_B_DINT_OBJECT_SCHEMA,
    A_DINT_B_INT_OBJECT_SCHEMA,
    A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
    A_DINT_B_NUM_OBJECT_SCHEMA,
    A_DINT_OBJECT_SCHEMA,
    A_INT_B_DINT_OBJECT_SCHEMA,
    A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA,
    A_INT_B_INT_OBJECT_SCHEMA,
    A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
    A_INT_OBJECT_SCHEMA,
    A_INT_OPEN_OBJECT_SCHEMA,
    A_OBJECT_SCHEMA,
    ALL_SCHEMAS,
    ARRAY_OF_INT_SCHEMA,
    ARRAY_OF_NUMBER_SCHEMA,
    ARRAY_OF_POSITIVE_INTEGER,
    ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
    ARRAY_OF_STRING_SCHEMA,
    ARRAY_SCHEMA,
    B_DINT_OPEN_OBJECT_SCHEMA,
    B_INT_OBJECT_SCHEMA,
    B_INT_OPEN_OBJECT_SCHEMA,
    B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
    B_NUM_C_INT_OBJECT_SCHEMA,
    B_NUM_C_INT_OPEN_OBJECT_SCHEMA,
    BOOLEAN_SCHEMA,
    BOOLEAN_SCHEMAS,
    EMPTY_OBJECT_SCHEMA,
    EMPTY_SCHEMA,
    ENUM_AB_SCHEMA,
    ENUM_ABC_SCHEMA,
    ENUM_BC_SCHEMA,
    EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA,
    EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
    EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA,
    EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
    EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA,
    EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
    EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
    EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
    FALSE_SCHEMA,
    INT_SCHEMA,
    MAX_ITEMS_DECREASED_SCHEMA,
    MAX_ITEMS_SCHEMA,
    MAX_LENGTH_DECREASED_SCHEMA,
    MAX_LENGTH_SCHEMA,
    MAX_PROPERTIES_DECREASED_SCHEMA,
    MAX_PROPERTIES_SCHEMA,
    MAXIMUM_DECREASED_INTEGER_SCHEMA,
    MAXIMUM_DECREASED_NUMBER_SCHEMA,
    MAXIMUM_INTEGER_SCHEMA,
    MAXIMUM_NUMBER_SCHEMA,
    MIN_ITEMS_INCREASED_SCHEMA,
    MIN_ITEMS_SCHEMA,
    MIN_LENGTH_INCREASED_SCHEMA,
    MIN_LENGTH_SCHEMA,
    MIN_PATTERN_SCHEMA,
    MIN_PATTERN_STRICT_SCHEMA,
    MIN_PROPERTIES_INCREASED_SCHEMA,
    MIN_PROPERTIES_SCHEMA,
    MINIMUM_INCREASED_INTEGER_SCHEMA,
    MINIMUM_INCREASED_NUMBER_SCHEMA,
    MINIMUM_INTEGER_SCHEMA,
    MINIMUM_NUMBER_SCHEMA,
    NON_OBJECT_SCHEMAS,
    NOT_OF_EMPTY_SCHEMA,
    NOT_OF_TRUE_SCHEMA,
    NUMBER_SCHEMA,
    OBJECT_SCHEMA,
    OBJECT_SCHEMAS,
    ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA,
    ONEOF_ARRAY_B_NUM_C_DINT_OPEN_SCHEMA,
    ONEOF_ARRAY_B_NUM_C_INT_SCHEMA,
    ONEOF_INT_SCHEMA,
    ONEOF_NUMBER_SCHEMA,
    ONEOF_STRING_INT_SCHEMA,
    ONEOF_STRING_SCHEMA,
    PATTERN_PROPERTY_ASTAR_OBJECT_SCHEMA,
    PROPERTY_NAMES_ASTAR_OBJECT_SCHEMA,
    STRING_SCHEMA,
    TRUE_SCHEMA,
    TUPLE_OF_INT_INT_OPEN_SCHEMA,
    TUPLE_OF_INT_INT_SCHEMA,
    TUPLE_OF_INT_OPEN_SCHEMA,
    TUPLE_OF_INT_SCHEMA,
    TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA,
    TYPES_STRING_INT_SCHEMA,
    TYPES_STRING_SCHEMA,
)
from tests.utils import new_random_name


async def debugging_details(
    newer: Draft7Validator,
    older: Draft7Validator,
    client: Client,
    subject: str,
) -> str:
    newer_schema = json.dumps(newer.schema)
    older_schema = json.dumps(older.schema)
    config_res = await client.get(f"config/{subject}?defaultToGlobal=true")
    config = config_res.json()
    return f"subject={subject} newer={newer_schema} older={older_schema} compatibility={config}"


async def not_schemas_are_compatible(
    newer: Draft7Validator,
    older: Draft7Validator,
    client: Client,
    compatibility_mode: CompatibilityModes,
) -> None:
    subject = new_random_name("subject")

    # sanity check
    subject_res = await client.get(f"subjects/{subject}/versions")
    assert subject_res.status_code == 404, "random subject should no exist {subject}"

    older_res = await client.post(
        f"subjects/{subject}/versions",
        json={
            "schema": json.dumps(older.schema),
            "schemaType": SchemaType.JSONSCHEMA.value,
        },
    )
    assert older_res.status_code == 200, await debugging_details(newer, older, client, subject)
    assert "id" in older_res.json(), await debugging_details(newer, older, client, subject)

    # enforce the target compatibility mode. not using the global setting
    # because that interfere with parallel runs.
    subject_config_res = await client.put(f"config/{subject}", json={"compatibility": compatibility_mode.value})
    assert subject_config_res.status_code == 200

    newer_res = await client.post(
        f"subjects/{subject}/versions",
        json={
            "schema": json.dumps(newer.schema),
            "schemaType": SchemaType.JSONSCHEMA.value,
        },
    )
    assert newer_res.status_code != 200, await debugging_details(newer, older, client, subject)

    # Sanity check. The compatibility must be explicitly set because any
    # difference can result in unexpected errors.
    subject_config_res = await client.get(f"config/{subject}?defaultToGlobal=true")
    subject_config = subject_config_res.json()
    assert subject_config["compatibilityLevel"] == compatibility_mode.value


async def schemas_are_compatible(
    client: Client,
    newer: Draft7Validator,
    older: Draft7Validator,
    compatibility_mode: CompatibilityModes,
) -> None:
    subject = new_random_name("subject")

    # sanity check
    subject_res = await client.get(f"subjects/{subject}/versions")
    assert subject_res.status_code == 404, "random subject should no exist {subject}"

    older_res = await client.post(
        f"subjects/{subject}/versions",
        json={
            "schema": json.dumps(older.schema),
            "schemaType": SchemaType.JSONSCHEMA.value,
        },
    )
    assert older_res.status_code == 200, await debugging_details(newer, older, client, subject)
    assert "id" in older_res.json(), await debugging_details(newer, older, client, subject)

    # enforce the target compatibility mode. not using the global setting
    # because that interfere with parallel runs.
    subject_config_res = await client.put(f"config/{subject}", json={"compatibility": compatibility_mode.value})
    assert subject_config_res.status_code == 200

    newer_res = await client.post(
        f"subjects/{subject}/versions",
        json={
            "schema": json.dumps(newer.schema),
            "schemaType": SchemaType.JSONSCHEMA.value,
        },
    )
    assert newer_res.status_code == 200, await debugging_details(newer, older, client, subject)
    # Because the IDs are global, and the same schema is used in multiple
    # tests, their order is unknown.
    assert older_res.json()["id"] != newer_res.json()["id"], await debugging_details(newer, older, client, subject)

    # Sanity check. The compatibility must be explicitly set because any
    # difference can result in unexpected errors.
    subject_config_res = await client.get(f"config/{subject}?defaultToGlobal=true")
    subject_config = subject_config_res.json()
    assert subject_config["compatibilityLevel"] == compatibility_mode.value


async def schemas_are_backward_compatible(
    reader: Draft7Validator,
    writer: Draft7Validator,
    client: Client,
) -> None:
    await schemas_are_compatible(
        # For backwards compatibility the newer schema is the reader
        newer=reader,
        older=writer,
        client=client,
        compatibility_mode=CompatibilityModes.BACKWARD,
    )


async def not_schemas_are_backward_compatible(
    reader: Draft7Validator,
    writer: Draft7Validator,
    client: Client,
) -> None:
    await not_schemas_are_compatible(
        # For backwards compatibility the newer schema is the reader
        newer=reader,
        older=writer,
        client=client,
        compatibility_mode=CompatibilityModes.BACKWARD,
    )


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


async def test_schemaregistry_schemaregistry_extra_optional_field_with_open_model_is_compatible(
    registry_async_client: Client,
) -> None:
    # - the newer is an open model, the extra field produced by the older is
    # automatically accepted
    await schemas_are_backward_compatible(
        reader=OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=TRUE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=EMPTY_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # - the older is a closed model, so the field `b` was never produced, which
    # means that the older never produced an invalid value.
    # - the newer's `b` field is optional, so the absenced of the field is not
    # a problem, and `a` is ignored because of the open model
    await schemas_are_backward_compatible(
        reader=B_INT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # - if the model is closed, then `a` must also be accepted
    await schemas_are_backward_compatible(
        reader=A_INT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # Examples a bit more complex
    await schemas_are_backward_compatible(
        reader=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=B_NUM_C_INT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_schemaregistry_extra_field_with_closed_model_is_incompatible(
    registry_async_client: Client,
) -> None:
    # await not_schemas_are_backward_compatible(
    #     reader=FALSE_SCHEMA,
    #     writer=A_INT_OBJECT_SCHEMA,
    #     client=registry_async_client,
    # )
    await not_schemas_are_backward_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=FALSE_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=NOT_OF_TRUE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=NOT_OF_EMPTY_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_schemaregistry_missing_required_field_is_incompatible(registry_async_client: Client) -> None:
    await not_schemas_are_backward_compatible(
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    # await not_schemas_are_backward_compatible(
    #     reader=A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA,
    #     writer=A_INT_OBJECT_SCHEMA,
    #     client=registry_async_client,
    # )
    await not_schemas_are_backward_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_giving_a_default_value_for_a_non_required_field_is_compatible(
    registry_async_client: Client,
) -> None:
    await schemas_are_backward_compatible(
        reader=OBJECT_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=TRUE_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=EMPTY_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=B_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=A_INT_B_DINT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=A_DINT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=A_DINT_B_DINT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=A_DINT_B_DINT_OBJECT_SCHEMA,
        writer=EMPTY_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_boolean_schemas_are_backward_compatible(registry_async_client: Client) -> None:
    # await not_schemas_are_backward_compatible(
    #     reader=FALSE_SCHEMA,
    #     writer=TRUE_SCHEMA,
    #     client=registry_async_client,
    # )
    await schemas_are_backward_compatible(
        reader=TRUE_SCHEMA,
        writer=FALSE_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=FALSE_SCHEMA,
        writer=TRUE_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_from_closed_to_open_is_incompatible(registry_async_client: Client) -> None:
    await not_schemas_are_backward_compatible(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_union_with_incompatible_elements(registry_async_client: Client) -> None:
    await not_schemas_are_backward_compatible(
        reader=ONEOF_ARRAY_B_NUM_C_INT_SCHEMA,
        writer=ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_union_with_compatible_elements(registry_async_client: Client) -> None:
    await schemas_are_backward_compatible(
        reader=ONEOF_ARRAY_B_NUM_C_DINT_OPEN_SCHEMA,
        writer=ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_array_and_tuples_are_incompatible(registry_async_client: Client) -> None:
    await not_schemas_are_backward_compatible(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=ARRAY_OF_INT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=TUPLE_OF_INT_OPEN_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_true_schema_is_compatible_with_object(registry_async_client: Client) -> None:
    for schema in OBJECT_SCHEMAS + BOOLEAN_SCHEMAS:
        if schema != TRUE_SCHEMA:
            await schemas_are_backward_compatible(
                reader=TRUE_SCHEMA,
                writer=schema,
                client=registry_async_client,
            )

    for schema in NON_OBJECT_SCHEMAS:
        await not_schemas_are_backward_compatible(
            reader=TRUE_SCHEMA,
            writer=schema,
            client=registry_async_client,
        )


async def test_schemaregistry_schema_compatibility_successes(registry_async_client: Client) -> None:
    # allowing a broader set of values is compatible
    await schemas_are_backward_compatible(
        reader=NUMBER_SCHEMA,
        writer=INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=ARRAY_OF_NUMBER_SCHEMA,
        writer=ARRAY_OF_INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA,
        writer=TUPLE_OF_INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=ENUM_ABC_SCHEMA,
        writer=ENUM_AB_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=ONEOF_STRING_INT_SCHEMA,
        writer=ONEOF_STRING_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=ONEOF_STRING_INT_SCHEMA,
        writer=STRING_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=A_INT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_B_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # requiring less values is compatible
    await schemas_are_backward_compatible(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_INT_OPEN_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_INT_SCHEMA,
        client=registry_async_client,
    )

    # equivalences
    await schemas_are_backward_compatible(
        reader=ONEOF_STRING_SCHEMA,
        writer=STRING_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=STRING_SCHEMA,
        writer=ONEOF_STRING_SCHEMA,
        client=registry_async_client,
    )

    # new non-required fields is compatible
    await schemas_are_backward_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=EMPTY_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=A_INT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_type_narrowing_incompabilities(registry_async_client: Client) -> None:
    await not_schemas_are_backward_compatible(
        reader=INT_SCHEMA,
        writer=NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=ARRAY_OF_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ENUM_AB_SCHEMA,
        writer=ENUM_ABC_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ENUM_BC_SCHEMA,
        writer=ENUM_ABC_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ONEOF_INT_SCHEMA,
        writer=ONEOF_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ONEOF_STRING_SCHEMA,
        writer=ONEOF_STRING_INT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=INT_SCHEMA,
        writer=ONEOF_STRING_INT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_type_mismatch_incompabilities(registry_async_client: Client) -> None:
    await not_schemas_are_backward_compatible(
        reader=BOOLEAN_SCHEMA,
        writer=INT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=INT_SCHEMA,
        writer=BOOLEAN_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=STRING_SCHEMA,
        writer=BOOLEAN_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=STRING_SCHEMA,
        writer=INT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=ARRAY_OF_STRING_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=TUPLE_OF_INT_INT_SCHEMA,
        writer=TUPLE_OF_INT_OPEN_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=TUPLE_OF_INT_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_OPEN_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=INT_SCHEMA,
        writer=ENUM_AB_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=ENUM_AB_SCHEMA,
        writer=INT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_true_and_false_schemas(registry_async_client: Client) -> None:
    await schemas_are_backward_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
        client=registry_async_client,
    )
    # await schemas_are_backward_compatible(
    #     writer=NOT_OF_TRUE_SCHEMA,
    #     reader=FALSE_SCHEMA,
    #     client=registry_async_client,
    # )
    # await schemas_are_backward_compatible(
    #     writer=NOT_OF_EMPTY_SCHEMA,
    #     reader=FALSE_SCHEMA,
    #     client=registry_async_client,
    # )

    await schemas_are_backward_compatible(
        writer=TRUE_SCHEMA,
        reader=EMPTY_SCHEMA,
        client=registry_async_client,
    )

    # await schemas_are_backward_compatible(
    #     writer=NOT_OF_EMPTY_SCHEMA,
    #     reader=TRUE_SCHEMA,
    #     client=registry_async_client,
    # )
    # await schemas_are_compatible(
    #     writer=NOT_OF_TRUE_SCHEMA,
    #     reader=TRUE_SCHEMA,
    #     client=registry_async_client,
    # )
    # await schemas_are_compatible(
    #     writer=NOT_OF_EMPTY_SCHEMA,
    #     reader=TRUE_SCHEMA,
    #     client=registry_async_client,
    # )

    await not_schemas_are_backward_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=TRUE_SCHEMA,
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=FALSE_SCHEMA,
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=NOT_OF_TRUE_SCHEMA,
        reader=FALSE_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=FALSE_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=FALSE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=FALSE_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=TRUE_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=NOT_OF_TRUE_SCHEMA,
        reader=TRUE_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=TRUE_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_schema_restrict_attributes_is_incompatible(registry_async_client: Client) -> None:
    await not_schemas_are_backward_compatible(
        writer=STRING_SCHEMA,
        reader=MAX_LENGTH_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MAX_LENGTH_SCHEMA,
        reader=MAX_LENGTH_DECREASED_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=STRING_SCHEMA,
        reader=MIN_LENGTH_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MIN_LENGTH_SCHEMA,
        reader=MIN_LENGTH_INCREASED_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=STRING_SCHEMA,
        reader=MIN_PATTERN_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MIN_PATTERN_SCHEMA,
        reader=MIN_PATTERN_STRICT_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=INT_SCHEMA,
        reader=MAXIMUM_INTEGER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=INT_SCHEMA,
        reader=MAXIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=NUMBER_SCHEMA,
        reader=MAXIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MAXIMUM_NUMBER_SCHEMA,
        reader=MAXIMUM_DECREASED_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MAXIMUM_INTEGER_SCHEMA,
        reader=MAXIMUM_DECREASED_INTEGER_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=INT_SCHEMA,
        reader=MINIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=NUMBER_SCHEMA,
        reader=MINIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MINIMUM_NUMBER_SCHEMA,
        reader=MINIMUM_INCREASED_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MINIMUM_INTEGER_SCHEMA,
        reader=MINIMUM_INCREASED_INTEGER_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=OBJECT_SCHEMA,
        reader=MAX_PROPERTIES_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MAX_PROPERTIES_SCHEMA,
        reader=MAX_PROPERTIES_DECREASED_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=OBJECT_SCHEMA,
        reader=MIN_PROPERTIES_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MIN_PROPERTIES_SCHEMA,
        reader=MIN_PROPERTIES_INCREASED_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=ARRAY_SCHEMA,
        reader=MAX_ITEMS_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MAX_ITEMS_SCHEMA,
        reader=MAX_ITEMS_DECREASED_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        writer=ARRAY_SCHEMA,
        reader=MIN_ITEMS_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        writer=MIN_ITEMS_SCHEMA,
        reader=MIN_ITEMS_INCREASED_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_schema_broadenning_attributes_is_compatible(registry_async_client: Client) -> None:
    await schemas_are_backward_compatible(
        writer=MAX_LENGTH_SCHEMA,
        reader=STRING_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MAX_LENGTH_DECREASED_SCHEMA,
        reader=MAX_LENGTH_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MIN_LENGTH_SCHEMA,
        reader=STRING_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MIN_LENGTH_INCREASED_SCHEMA,
        reader=MIN_LENGTH_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MIN_PATTERN_SCHEMA,
        reader=STRING_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MAXIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MAXIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MAXIMUM_DECREASED_NUMBER_SCHEMA,
        reader=MAXIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MINIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MINIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MINIMUM_INCREASED_NUMBER_SCHEMA,
        reader=MINIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MAX_PROPERTIES_SCHEMA,
        reader=OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MAX_PROPERTIES_DECREASED_SCHEMA,
        reader=MAX_PROPERTIES_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MIN_PROPERTIES_SCHEMA,
        reader=OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MIN_PROPERTIES_INCREASED_SCHEMA,
        reader=MIN_PROPERTIES_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MAX_ITEMS_SCHEMA,
        reader=ARRAY_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MAX_ITEMS_DECREASED_SCHEMA,
        reader=MAX_ITEMS_SCHEMA,
        client=registry_async_client,
    )

    await schemas_are_backward_compatible(
        writer=MIN_ITEMS_SCHEMA,
        reader=ARRAY_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        writer=MIN_ITEMS_INCREASED_SCHEMA,
        reader=MIN_ITEMS_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_pattern_properties(registry_async_client: Client):
    await schemas_are_backward_compatible(
        reader=OBJECT_SCHEMA,
        writer=PATTERN_PROPERTY_ASTAR_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    # In backward compatibility mode it is allowed to delete fields
    await schemas_are_backward_compatible(
        reader=A_OBJECT_SCHEMA,
        writer=PATTERN_PROPERTY_ASTAR_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    # In backward compatibility mode it is allowed to add optional fields
    await schemas_are_backward_compatible(
        reader=PATTERN_PROPERTY_ASTAR_OBJECT_SCHEMA,
        writer=A_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # - older accept any value for `a`
    # - newer requires it to be an `int`, therefore the other values became
    # invalid
    await not_schemas_are_backward_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=PATTERN_PROPERTY_ASTAR_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # - older has property `b`
    # - newer only accepts properties with match regex `a*`
    await not_schemas_are_backward_compatible(
        reader=B_INT_OBJECT_SCHEMA,
        writer=PATTERN_PROPERTY_ASTAR_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_object_properties(registry_async_client: Client):
    await not_schemas_are_backward_compatible(
        reader=A_OBJECT_SCHEMA,
        writer=OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=OBJECT_SCHEMA,
        writer=A_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=OBJECT_SCHEMA,
        client=registry_async_client,
    )

    await not_schemas_are_backward_compatible(
        reader=B_INT_OBJECT_SCHEMA,
        writer=OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_property_names(registry_async_client: Client):
    await schemas_are_backward_compatible(
        reader=OBJECT_SCHEMA,
        writer=PROPERTY_NAMES_ASTAR_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await not_schemas_are_backward_compatible(
        reader=A_OBJECT_SCHEMA,
        writer=PROPERTY_NAMES_ASTAR_OBJECT_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=PROPERTY_NAMES_ASTAR_OBJECT_SCHEMA,
        writer=A_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # - older accept any value for `a`
    # - newer requires it to be an `int`, therefore the other values became
    # invalid
    await not_schemas_are_backward_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=PROPERTY_NAMES_ASTAR_OBJECT_SCHEMA,
        client=registry_async_client,
    )

    # - older has property `b`
    # - newer only accepts properties with match regex `a*`
    await schemas_are_backward_compatible(
        reader=PROPERTY_NAMES_ASTAR_OBJECT_SCHEMA,
        writer=B_INT_OBJECT_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_type_with_list(registry_async_client: Client):
    # "type": [] is treated as a shortcut for anyOf
    await schemas_are_backward_compatible(
        reader=STRING_SCHEMA,
        writer=TYPES_STRING_SCHEMA,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=TYPES_STRING_INT_SCHEMA,
        writer=TYPES_STRING_SCHEMA,
        client=registry_async_client,
    )


async def test_schemaregistry_ref(registry_async_client: Client):
    await schemas_are_backward_compatible(
        reader=ARRAY_OF_POSITIVE_INTEGER,
        writer=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
        client=registry_async_client,
    )
    await schemas_are_backward_compatible(
        reader=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
        writer=ARRAY_OF_POSITIVE_INTEGER,
        client=registry_async_client,
    )
