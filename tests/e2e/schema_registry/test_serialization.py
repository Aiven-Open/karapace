"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

import asyncio
import base64
import json
from typing import Any

from confluent_kafka.admin import NewTopic

from karapace.core.client import Client
from karapace.core.container import KarapaceContainer
from karapace.core.kafka.consumer import KafkaConsumer
from karapace.core.kafka.producer import KafkaProducer
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema
from karapace.core.serialization import SchemaRegistrySerializer
from karapace.core.typing import Subject

COMPLEX_UNION_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.minimal",
            "name": "MinimalUnionTest",
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "attrs",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "Attr",
                            "fields": [
                                {"name": "k", "type": "string"},
                                {"name": "v", "type": ["null", "string", "long", "double", "boolean"]},
                            ],
                        },
                    },
                },
                {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
            ],
        }
    ),
)

COMPLEX_UNION_AVRO_RECORD = {
    "id": "one",
    "attrs": [
        {"k": "text", "v": "value"},
        {"k": "count", "v": 5},
        {"k": "ratio", "v": 1.5},
        {"k": "flag", "v": True},
        {"k": "empty", "v": None},
    ],
    "props": {"present": "yes", "missing": None},
}

MAP_UNION_AVRO_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.minimal",
            "name": "MapUnionTest",
            "type": "record",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "props", "type": {"type": "map", "values": ["null", "string"]}},
            ],
        }
    ),
)

MAP_UNION_AVRO_RECORD = {
    "id": "one",
    "props": {"present": "yes", "missing": None},
}

AVRO_BYTES_SCHEMA = ValidatedTypedSchema.parse(
    SchemaType.AVRO,
    json.dumps(
        {
            "namespace": "io.aiven.bytes",
            "name": "BytesEnvelope",
            "type": "record",
            "fields": [
                {
                    "name": "payload",
                    "type": {
                        "type": "record",
                        "name": "Payload",
                        "fields": [
                            {"name": "raw", "type": "bytes"},
                            {"name": "items", "type": {"type": "array", "items": "bytes"}},
                        ],
                    },
                }
            ],
        }
    ),
)

AVRO_BYTES_RECORD = {
    "payload": {
        "raw": b"\x01\x02",
        "items": [b"\x03\x04", b"\x05\x06"],
    }
}


async def _wait_for_primary(client: Client, timeout: float = 30.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        res = await client.get("master_available")
        if res.status_code == 200 and res.json_result and res.json_result.get("master_available") is True:
            return
        await asyncio.sleep(1.0)
    raise TimeoutError("Schema registry did not elect a primary within timeout")


def _build_oidc_serializer(
    karapace_container: KarapaceContainer, registry_async_client_oidc: Client
) -> tuple[SchemaRegistrySerializer, Client]:
    serializer = SchemaRegistrySerializer(config=karapace_container.config())
    original_client = serializer.registry_client.client
    serializer.registry_client.client = registry_async_client_oidc
    return serializer, original_client


def _poll_for_value(consumer: KafkaConsumer, timeout_s: float = 10.0) -> Any:
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        message = consumer.poll(timeout=1.0)
        if message is not None and message.error() is None:
            return message.value()
    raise TimeoutError("Timed out waiting for Kafka message")


async def test_schema_registry_serializer_round_trip_complex_union_avro(
    karapace_container: KarapaceContainer,
    registry_async_client_oidc: Client,
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    new_topic: NewTopic,
) -> None:
    await _wait_for_primary(registry_async_client_oidc)

    serializer, original_client = _build_oidc_serializer(karapace_container, registry_async_client_oidc)
    try:
        subject = Subject(f"{new_topic.topic}-value")
        await serializer.upsert_id_for_schema(COMPLEX_UNION_AVRO_SCHEMA, str(subject))
        payload = await serializer.serialize(COMPLEX_UNION_AVRO_SCHEMA, COMPLEX_UNION_AVRO_RECORD)

        producer.send(new_topic.topic, value=payload)
        producer.flush()

        consumer.subscribe([new_topic.topic])
        kafka_value = _poll_for_value(consumer)

        assert await serializer.deserialize(kafka_value) == COMPLEX_UNION_AVRO_RECORD
        assert await serializer.deserialize(payload) == COMPLEX_UNION_AVRO_RECORD
    finally:
        serializer.registry_client.client = original_client
        await serializer.close()


async def test_schema_registry_serializer_round_trip_map_union_avro(
    karapace_container: KarapaceContainer,
    registry_async_client_oidc: Client,
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    new_topic: NewTopic,
) -> None:
    await _wait_for_primary(registry_async_client_oidc)

    serializer, original_client = _build_oidc_serializer(karapace_container, registry_async_client_oidc)
    try:
        subject = Subject(f"{new_topic.topic}-value")
        await serializer.upsert_id_for_schema(MAP_UNION_AVRO_SCHEMA, str(subject))
        payload = await serializer.serialize(MAP_UNION_AVRO_SCHEMA, MAP_UNION_AVRO_RECORD)

        producer.send(new_topic.topic, value=payload)
        producer.flush()

        consumer.subscribe([new_topic.topic])
        kafka_value = _poll_for_value(consumer)

        assert await serializer.deserialize(kafka_value) == MAP_UNION_AVRO_RECORD
        assert await serializer.deserialize(payload) == MAP_UNION_AVRO_RECORD
    finally:
        serializer.registry_client.client = original_client
        await serializer.close()


async def test_schema_registry_serializer_round_trip_avro_bytes_as_base64_strings(
    karapace_container: KarapaceContainer,
    registry_async_client_oidc: Client,
    producer: KafkaProducer,
    consumer: KafkaConsumer,
    new_topic: NewTopic,
) -> None:
    await _wait_for_primary(registry_async_client_oidc)

    serializer, original_client = _build_oidc_serializer(karapace_container, registry_async_client_oidc)
    try:
        subject = Subject(f"{new_topic.topic}-value")
        await serializer.upsert_id_for_schema(AVRO_BYTES_SCHEMA, str(subject))
        payload = await serializer.serialize(AVRO_BYTES_SCHEMA, AVRO_BYTES_RECORD)

        producer.send(new_topic.topic, value=payload)
        producer.flush()

        consumer.subscribe([new_topic.topic])
        kafka_value = _poll_for_value(consumer)

        expected = {
            "payload": {
                "raw": base64.b64encode(b"\x01\x02").decode("ascii"),
                "items": [
                    base64.b64encode(b"\x03\x04").decode("ascii"),
                    base64.b64encode(b"\x05\x06").decode("ascii"),
                ],
            }
        }
        assert await serializer.deserialize(kafka_value) == expected
        assert await serializer.deserialize(payload) == expected
    finally:
        serializer.registry_client.client = original_client
        await serializer.close()
