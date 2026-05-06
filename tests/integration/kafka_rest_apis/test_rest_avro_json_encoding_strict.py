"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dataclasses import dataclass
import asyncio
import json

from karapace.core.client import Client
from karapace.core.kafka.admin import KafkaAdminClient
import pytest
from tests.utils import REST_HEADERS, new_topic, wait_for_topics

SIMPLE_UNION_RECORD_SCHEMA = {
    "namespace": "com.example.avro",
    "type": "record",
    "name": "simple",
    "fields": [
        {
            "name": "payload",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Payload",
                    "namespace": "com.example.payload.v1",
                    "fields": [{"name": "amount", "type": "float"}],
                },
            ],
        }
    ],
}

LOGICAL_TYPES_SCHEMA = {
    "type": "record",
    "name": "LogicalTypes",
    "namespace": "com.example.avro",
    "fields": [
        {"name": "eventTime", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "businessDate", "type": {"type": "int", "logicalType": "date"}},
    ],
}

# Tests that two union branches with the same short record name but different
# namespaces must be selected by their fullnames.
AMBIGUOUS_NAMES_UNION_SCHEMA = {
    "type": "record",
    "name": "Container",
    "namespace": "com.example",
    "fields": [
        {
            "name": "data",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Item",
                    "namespace": "com.example.a",
                    "fields": [{"name": "value", "type": "string"}],
                },
                {
                    "type": "record",
                    "name": "Item",
                    "namespace": "com.example.b",
                    "fields": [{"name": "count", "type": "int"}],
                },
                {
                    "type": "record",
                    "name": "Item",
                    "fields": [{"name": "count", "type": "int"}],
                },
            ],
        }
    ],
}

# Tests namespace inheritance for a record declared without namespace inside
# an enclosing record that has a namespace.
INHERITED_NAMESPACE_UNION_SCHEMA = {
    "type": "record",
    "name": "Outer",
    "namespace": "com.example",
    "fields": [
        {
            "name": "inner",
            "type": [
                "null",
                {"type": "record", "name": "Inner", "fields": [{"name": "value", "type": "string"}]},
            ],
        }
    ],
}

# Tests that if no namespace exists anywhere, fullname equals short name.
NO_NAMESPACE_UNION_SCHEMA = {
    "type": "record",
    "name": "Wrapper",
    "fields": [
        {
            "name": "item",
            "type": [
                "null",
                {"type": "record", "name": "Bare", "fields": [{"name": "x", "type": "string"}]},
            ],
        }
    ],
}

# Tests ambiguous logical unions where branch type must be explicitly tagged.
UNION_LOGICAL_TYPES_SCHEMA = {
    "type": "record",
    "name": "MultiLogical",
    "fields": [
        {
            "name": "example1",
            "type": [
                "null",
                {"type": "int", "logicalType": "date"},
                {"type": "long", "logicalType": "timestamp-micros"},
            ],
        }
    ],
}

# Reuses one named record type in two union fields to validate fullname handling
# for referenced branches.
REFERENCE_REUSED_RECORD_SCHEMA = {
    "type": "record",
    "name": "Doc",
    "namespace": "com.example.erd",
    "fields": [
        {
            "name": "payload",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Payload",
                    "namespace": "com.example.payload.v1",
                    "fields": [
                        {"name": "amount", "type": "float"},
                        {"name": "nameOfPerson", "type": "string"},
                    ],
                },
            ],
            "default": None,
        },
        {
            "name": "payloadBefore",
            "type": ["null", "com.example.payload.v1.Payload"],
            "default": None,
        },
    ],
}

NON_RECORD_UNION_SCHEMA = {
    "type": "record",
    "name": "NonRecordUnionContainer",
    "namespace": "com.example.avro",
    "fields": [
        {
            "name": "value",
            "type": [
                "null",
                {"type": "array", "items": "int"},
                {"type": "map", "values": "string"},
                "bytes",
                {
                    "type": "enum",
                    "name": "State",
                    "namespace": "com.example.types",
                    "symbols": ["ON", "OFF"],
                },
                {
                    "type": "fixed",
                    "name": "Token",
                    "namespace": "com.example.types",
                    "size": 4,
                },
            ],
        }
    ],
}

# Unsupported schema shape in Avro: unions may not immediately contain unions.
INVALID_NESTED_UNION_SCHEMA = {
    "type": "record",
    "name": "InvalidNestedUnion",
    "namespace": "com.example.avro",
    "fields": [{"name": "value", "type": ["null", ["string", "int"]]}],
}

# Compact logical type matrix for three payload forms:
# 1) union + logical-type tag + string value (extended parser)
# 2) union + base-type tag + base representation
# 3) direct non-union value with its base representation
ALL_LOGICAL_TYPES_CASES = (
    {
        "id": "date",
        "base_type": "int",
        "logical_type": "date",
        "base_value": 20213,
        "string_value": "2025-05-05",
    },
    {
        "id": "timestamp-millis",
        "base_type": "long",
        "logical_type": "timestamp-millis",
        "base_value": 1746448140123,
        "string_value": "2025-05-05T16:29:00.123+04:00",
    },
    {
        "id": "timestamp-micros",
        "base_type": "long",
        "logical_type": "timestamp-micros",
        "base_value": 1746448140123456,
        "string_value": "2025-05-05T16:29:00.123456+04:00",
    },
    {
        "id": "time-millis",
        "base_type": "int",
        "logical_type": "time-millis",
        "base_value": 59340123,
        "string_value": "16:29:00.123",
    },
    {
        "id": "time-micros",
        "base_type": "long",
        "logical_type": "time-micros",
        "base_value": 59340123456,
        "string_value": "16:29:00.123456",
    },
    {
        "id": "decimal",
        "base_type": "bytes",
        "logical_type": "decimal",
        # Confluent-style base64 two's complement unscaled bytes for decimal 14.36 (scale 2).
        "base_value": "BZw=",
        "string_value": "14.36",
    },
)


def _single_non_union_logical_schema(case: dict) -> dict:
    field_type: dict = {"type": case["base_type"], "logicalType": case["logical_type"]}
    if case["logical_type"] == "decimal":
        field_type["precision"] = 9
        field_type["scale"] = 2

    return {
        "type": "record",
        "name": "OneLogical",
        "fields": [{"name": "example1", "type": field_type}],
    }


def _single_union_logical_schema(case: dict) -> dict:
    field_type: dict = {"type": case["base_type"], "logicalType": case["logical_type"]}
    if case["logical_type"] == "decimal":
        field_type["precision"] = 9
        field_type["scale"] = 2

    return {
        "type": "record",
        "name": "OneLogicalUnion",
        "fields": [{"name": "example1", "type": ["null", field_type]}],
    }


def _single_union_decimal_schema(precision: int, scale: int) -> dict:
    """Small helper to isolate decimal precision/scale validation scenarios."""
    return {
        "type": "record",
        "name": "OneDecimalUnion",
        "fields": [
            {
                "name": "example1",
                "type": [
                    "null",
                    {"type": "bytes", "logicalType": "decimal", "precision": precision, "scale": scale},
                ],
            }
        ],
    }


def _payload(value_schema: dict, value: dict) -> dict:
    return {
        "key_schema": json.dumps({"type": "string"}),
        "value_schema": json.dumps(value_schema),
        "records": [{"key": "abc", "value": value}],
    }


async def _post_and_get_status(rest_async_client: Client, topic_name: str, payload: dict) -> tuple[int, dict]:
    for _ in range(10):
        response = await rest_async_client.post(
            f"/topics/{topic_name}",
            json=payload,
            headers=REST_HEADERS["avro"],
        )
        if response.status_code != 404:
            break
        await asyncio.sleep(0.5)
    return response.status_code, response.json()


NEW_TOPIC_TIMEOUT = 10


async def _topic_name(rest_async_client: Client, admin_client: KafkaAdminClient) -> str:
    topic_name = new_topic(admin_client, prefix="avro_v2_strict")
    await wait_for_topics(rest_async_client, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
    return topic_name


def _assert_rejected(status: int) -> None:
    # For baseline runs against external REST implementations we assert behavior
    # (request rejected) instead of an implementation-specific status code.
    assert 400 <= status < 500


def _assert_status(status: int, expected_ok: bool) -> None:
    if expected_ok:
        assert status == 200
    else:
        _assert_rejected(status)


@dataclass
class UnionResolutionCase:
    name: str
    schema: dict
    value: dict
    expected_ok: bool

    def __str__(self) -> str:
        return self.name


SIMPLE_UNION_CASES = (
    UnionResolutionCase(
        name="fullname tag accepted",
        schema=SIMPLE_UNION_RECORD_SCHEMA,
        value={"payload": {"com.example.payload.v1.Payload": {"amount": 2.3}}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="shortname tag rejected",
        schema=SIMPLE_UNION_RECORD_SCHEMA,
        value={"payload": {"Payload": {"amount": 2.3}}},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="untagged record rejected",
        schema=SIMPLE_UNION_RECORD_SCHEMA,
        value={"payload": {"amount": 2.3}},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="null branch accepted",
        schema=SIMPLE_UNION_RECORD_SCHEMA,
        value={"payload": None},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="wrong fullname rejected",
        schema=SIMPLE_UNION_RECORD_SCHEMA,
        value={"payload": {"com.example.wrong.Payload": {"amount": 2.3}}},
        expected_ok=False,
    ),
)

AMBIGUOUS_NAME_UNION_CASES = (
    UnionResolutionCase(
        name="ambiguous shortname rejected",
        schema=AMBIGUOUS_NAMES_UNION_SCHEMA,
        value={"data": {"Item": {"value": "hello"}}},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="first branch fullname accepted",
        schema=AMBIGUOUS_NAMES_UNION_SCHEMA,
        value={"data": {"com.example.a.Item": {"value": "hello"}}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="second branch fullname accepted",
        schema=AMBIGUOUS_NAMES_UNION_SCHEMA,
        value={"data": {"com.example.b.Item": {"count": 5}}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="third branch fullname accepted",
        schema=AMBIGUOUS_NAMES_UNION_SCHEMA,
        value={"data": {"com.example.Item": {"count": 5}}},
        expected_ok=True,
    ),
)

NAMESPACE_UNION_CASES = (
    UnionResolutionCase(
        name="inherited namespace fullname accepted",
        schema=INHERITED_NAMESPACE_UNION_SCHEMA,
        value={"inner": {"com.example.Inner": {"value": "ok"}}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="inherited namespace shortname rejected",
        schema=INHERITED_NAMESPACE_UNION_SCHEMA,
        value={"inner": {"Inner": {"value": "ok"}}},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="no namespace shortname accepted",
        schema=NO_NAMESPACE_UNION_SCHEMA,
        value={"item": {"Bare": {"x": "ok"}}},
        expected_ok=True,
    ),
)

NON_RECORD_UNION_CASES = (
    UnionResolutionCase(
        name="array tag accepted",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"array": [1, 2, 3]}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="map tag accepted",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"map": {"k": "v"}}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="bytes tag accepted",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"bytes": "ABCD"}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="enum fullname tag accepted",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"com.example.types.State": "ON"}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="enum invalid symbol rejected",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"com.example.types.State": "MISSING"}},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="fixed fullname tag accepted",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"com.example.types.Token": "ABCD"}},
        expected_ok=True,
    ),
    UnionResolutionCase(
        name="fixed invalid length rejected",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"com.example.types.Token": "ABC"}},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="untagged array rejected",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": [1, 2, 3]},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="untagged map rejected",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": {"k": "v"}},
        expected_ok=False,
    ),
    UnionResolutionCase(
        name="untagged bytes rejected",
        schema=NON_RECORD_UNION_SCHEMA,
        value={"value": "ABCD"},
        expected_ok=False,
    ),
)


ADDITIONAL_LOGICAL_TYPES_CASES = (
    {
        "id": "uuid",
        "base_type": "string",
        "logical_type": "uuid",
        "base_value": "123e4567-e89b-12d3-a456-426614174000",
        "string_value": "123e4567-e89b-12d3-a456-426614174000",
    },
    {
        "id": "local-timestamp-millis",
        "base_type": "long",
        "logical_type": "local-timestamp-millis",
        "base_value": 1746448140123,
        "string_value": "2025-05-05T16:29:00.123",
    },
    {
        "id": "local-timestamp-micros",
        "base_type": "long",
        "logical_type": "local-timestamp-micros",
        "base_value": 1746448140123456,
        "string_value": "2025-05-05T16:29:00.123456",
    },
)


# --- Union strictness (fullname / short name / ambiguity) ---
@pytest.mark.parametrize(
    "testcase",
    SIMPLE_UNION_CASES,
    ids=str,
)
async def test_simple_union_strict_resolution(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    testcase: UnionResolutionCase,
) -> None:
    """Simple union must use explicit, correct tags in strict mode."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(testcase.schema, testcase.value),
    )
    _assert_status(status, expected_ok=testcase.expected_ok)


async def test_logical_types_numeric_values_are_accepted(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
) -> None:
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(
            LOGICAL_TYPES_SCHEMA,
            {"eventTime": 1743849600000000, "businessDate": 20213},
        ),
    )
    assert status == 200


async def test_logical_types_iso8601_values_are_rejected(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
) -> None:
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, body = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(
            LOGICAL_TYPES_SCHEMA,
            {"eventTime": "2026-05-05T16:29:00+04:00", "businessDate": "2025-05-05"},
        ),
    )
    _assert_rejected(status)
    assert body.get("message")
    assert body["message"] != "None"


@pytest.mark.parametrize(
    "testcase",
    AMBIGUOUS_NAME_UNION_CASES,
    ids=str,
)
async def test_ambiguous_name_union_strict_resolution(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    testcase: UnionResolutionCase,
) -> None:
    """Union with same short names must resolve only from unambiguous fullnames."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(testcase.schema, testcase.value),
    )
    _assert_status(status, expected_ok=testcase.expected_ok)


@pytest.mark.parametrize(
    "testcase",
    NAMESPACE_UNION_CASES,
    ids=str,
)
async def test_namespace_union_tag_resolution(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    testcase: UnionResolutionCase,
) -> None:
    """Namespace presence controls whether short name or fullname tag is valid."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(testcase.schema, testcase.value),
    )
    _assert_status(status, expected_ok=testcase.expected_ok)


@pytest.mark.parametrize("testcase", NON_RECORD_UNION_CASES, ids=str)
async def test_non_record_union_tag_resolution(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    testcase: UnionResolutionCase,
) -> None:
    """Strict mode requires explicit union tags for array/map/bytes/enum/fixed branches."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(testcase.schema, testcase.value),
    )
    _assert_status(status, expected_ok=testcase.expected_ok)


async def test_union_logical_type_untagged_iso8601_is_rejected(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
) -> None:
    """Reject untagged ISO-8601 string in logical union because branch is ambiguous."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(UNION_LOGICAL_TYPES_SCHEMA, {"example1": "2025-05-05"}),
    )
    _assert_rejected(status)


# --- Logical types matrix ---
@pytest.mark.parametrize("case", ALL_LOGICAL_TYPES_CASES, ids=[case["id"] for case in ALL_LOGICAL_TYPES_CASES])
async def test_logical_type_union_base_type_tag_is_accepted(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    case: dict,
) -> None:
    """For each logical type, strict mode accepts explicit union base-type tag."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    schema = _single_union_logical_schema(case)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(schema, {"example1": {case["base_type"]: case["base_value"]}}),
    )
    _assert_status(status, expected_ok=True)


@pytest.mark.parametrize("case", ALL_LOGICAL_TYPES_CASES, ids=[case["id"] for case in ALL_LOGICAL_TYPES_CASES])
async def test_logical_type_union_string_tag_is_accepted_with_extended_parser(
    rest_async_strict_extended_parser_client: Client,
    admin_client: KafkaAdminClient,
    case: dict,
) -> None:
    """For each logical type, strict+extended mode accepts logical-type string tag."""
    topic_name = await _topic_name(rest_async_strict_extended_parser_client, admin_client)
    schema = _single_union_logical_schema(case)
    status, _ = await _post_and_get_status(
        rest_async_strict_extended_parser_client,
        topic_name,
        _payload(schema, {"example1": {case["logical_type"]: case["string_value"]}}),
    )
    _assert_status(status, expected_ok=True)


@pytest.mark.parametrize("case", ALL_LOGICAL_TYPES_CASES, ids=[case["id"] for case in ALL_LOGICAL_TYPES_CASES])
async def test_logical_type_non_union_base_value_is_accepted(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    case: dict,
) -> None:
    """For each logical type, non-union schema accepts base representation."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    schema = _single_non_union_logical_schema(case)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(schema, {"example1": case["base_value"]}),
    )
    _assert_status(status, expected_ok=True)


@pytest.mark.parametrize("case", ALL_LOGICAL_TYPES_CASES, ids=[case["id"] for case in ALL_LOGICAL_TYPES_CASES])
async def test_logical_type_union_null_branch_is_accepted(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    case: dict,
) -> None:
    """For each logical type union, null branch stays valid in strict mode."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    schema = _single_union_logical_schema(case)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(schema, {"example1": None}),
    )
    _assert_status(status, expected_ok=True)


@pytest.mark.parametrize(
    "value",
    [
        # Ambiguous human-readable form: can look like date or timestamp without explicit tag.
        "2025-05-05",
        "2025-05-05T16:29:00+04:00",
    ],
    ids=["date-like", "datetime-like"],
)
async def test_logical_union_untagged_human_readable_value_is_rejected(
    rest_async_strict_extended_parser_client: Client,
    admin_client: KafkaAdminClient,
    value: str,
) -> None:
    """Reject untagged string when union has multiple logical branches."""
    topic_name = await _topic_name(rest_async_strict_extended_parser_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_extended_parser_client,
        topic_name,
        _payload(UNION_LOGICAL_TYPES_SCHEMA, {"example1": value}),
    )
    _assert_status(status, expected_ok=False)


# Logical-type name tags are only expected to work with extended parser mode.
@pytest.mark.parametrize("case", ALL_LOGICAL_TYPES_CASES, ids=[case["id"] for case in ALL_LOGICAL_TYPES_CASES])
async def test_logical_type_union_string_tag_is_rejected_without_extended_parser(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    case: dict,
) -> None:
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    schema = _single_union_logical_schema(case)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(schema, {"example1": {case["logical_type"]: case["string_value"]}}),
    )
    _assert_status(status, expected_ok=False)


@pytest.mark.parametrize(
    "case",
    ADDITIONAL_LOGICAL_TYPES_CASES,
    ids=[case["id"] for case in ADDITIONAL_LOGICAL_TYPES_CASES],
)
async def test_additional_logical_types_union_base_and_tag_behavior(
    rest_async_strict_client: Client,
    rest_async_strict_extended_parser_client: Client,
    admin_client: KafkaAdminClient,
    case: dict,
) -> None:
    """Lock strict behavior for logical types not covered by the main matrix."""
    schema = _single_union_logical_schema(case)

    topic_name_base = await _topic_name(rest_async_strict_client, admin_client)
    base_status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name_base,
        _payload(schema, {"example1": {case["base_type"]: case["base_value"]}}),
    )
    _assert_status(base_status, expected_ok=True)

    topic_name_tag_no_ext = await _topic_name(rest_async_strict_client, admin_client)
    tag_no_ext_status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name_tag_no_ext,
        _payload(schema, {"example1": {case["logical_type"]: case["string_value"]}}),
    )
    # uuid is string-backed and accepted without extended conversion; local timestamps are not.
    _assert_status(tag_no_ext_status, expected_ok=case["logical_type"] == "uuid")

    topic_name_tag_ext = await _topic_name(rest_async_strict_extended_parser_client, admin_client)
    tag_ext_status, _ = await _post_and_get_status(
        rest_async_strict_extended_parser_client,
        topic_name_tag_ext,
        _payload(schema, {"example1": {case["logical_type"]: case["string_value"]}}),
    )
    # Current implementation still does not parse local-timestamp strings.
    _assert_status(tag_ext_status, expected_ok=case["logical_type"] == "uuid")


def _duration_union_schema() -> dict:
    return {
        "type": "record",
        "name": "DurationUnionContainer",
        "namespace": "com.example.avro",
        "fields": [
            {
                "name": "value",
                "type": [
                    "null",
                    {
                        "type": "fixed",
                        "name": "Duration12",
                        "namespace": "com.example.types",
                        "size": 12,
                        "logicalType": "duration",
                    },
                ],
            }
        ],
    }


@pytest.mark.parametrize(
    ("value", "expected_ok"),
    [
        ({"value": {"com.example.types.Duration12": "abcdefghijkl"}}, True),
        ({"value": {"com.example.types.Duration12": "abc"}}, False),
    ],
    ids=["duration-fixed-valid-length", "duration-fixed-invalid-length"],
)
async def test_duration_fixed_union_size_validation(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    value: dict,
    expected_ok: bool,
) -> None:
    """Duration uses fixed(12), so strict validation follows fixed size rules."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(_duration_union_schema(), value),
    )
    _assert_status(status, expected_ok=expected_ok)


async def test_invalid_nested_union_schema_is_rejected(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
) -> None:
    """Nested unions are invalid in Avro and should be rejected at schema handling time."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(INVALID_NESTED_UNION_SCHEMA, {"value": None}),
    )
    _assert_status(status, expected_ok=False)


@pytest.mark.parametrize(
    "payload_value",
    [
        # Too many significant digits for precision=4, scale=2.
        {"decimal": "1234.56"},
        # Same overflow, but via base-type integer path (unscaled 123456 -> 1234.56).
        {"bytes": 123456},
    ],
    ids=["logical-tag-overflow", "base-type-overflow"],
)
async def test_logical_decimal_precision_overflow_is_rejected(
    rest_async_strict_extended_parser_client: Client,
    admin_client: KafkaAdminClient,
    payload_value: dict,
) -> None:
    """Reject decimal values that exceed declared precision in strict mode."""
    topic_name = await _topic_name(rest_async_strict_extended_parser_client, admin_client)
    schema = _single_union_decimal_schema(precision=4, scale=2)
    status, _ = await _post_and_get_status(
        rest_async_strict_extended_parser_client,
        topic_name,
        _payload(schema, {"example1": payload_value}),
    )
    _assert_status(status, expected_ok=False)


@pytest.mark.parametrize(
    ("value", "expected_ok"),
    [
        (
            {
                "payload": {"com.example.payload.v1.Payload": {"amount": 2.3, "nameOfPerson": "Alice"}},
                "payloadBefore": {"com.example.payload.v1.Payload": {"amount": 1.1, "nameOfPerson": "Bob"}},
            },
            True,
        ),
        (
            {
                "payload": {"com.example.payload.v1.Payload": {"amount": 2.3, "nameOfPerson": "Alice"}},
                "payloadBefore": {"Payload": {"amount": 1.1, "nameOfPerson": "Bob"}},
            },
            False,
        ),
    ],
    ids=["referenced-fullname-accepted", "referenced-shortname-rejected"],
)
async def test_referenced_union_fullname_resolution(
    rest_async_strict_client: Client,
    admin_client: KafkaAdminClient,
    value: dict,
    expected_ok: bool,
) -> None:
    """Referenced union branches resolve by fullname, not short name."""
    topic_name = await _topic_name(rest_async_strict_client, admin_client)
    status, _ = await _post_and_get_status(
        rest_async_strict_client,
        topic_name,
        _payload(REFERENCE_REUSED_RECORD_SCHEMA, value),
    )
    _assert_status(status, expected_ok=expected_ok)
