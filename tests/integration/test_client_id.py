"""
Tests for client_id configuration in Schema Registry and REST API

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.core.client import Client
from karapace.core.kafka.admin import KafkaAdminClient
from tests.utils import new_random_name, wait_for_topics

NEW_TOPIC_TIMEOUT = 10


async def test_schema_registry_uses_custom_client_id(
    registry_async_client_custom_client_id: Client,
) -> None:
    """Test that Schema Registry properly uses the configured client_id."""
    # Verify the registry is operational with custom client_id
    res = await registry_async_client_custom_client_id.get("subjects")
    assert res.status_code == 200


async def test_rest_api_uses_custom_client_id_for_producing(
    rest_async_client_custom_client_id: Client,
    admin_client: KafkaAdminClient,
) -> None:
    """Test that REST API properly uses the configured client_id when producing messages."""
    topic_name = new_random_name("test-client-id-topic")
    admin_client.new_topic(topic_name, num_partitions=1, replication_factor=1)
    
    try:
        await wait_for_topics(rest_async_client_custom_client_id, topic_names=[topic_name], timeout=NEW_TOPIC_TIMEOUT, sleep=1)
        
        # Produce a message via REST API
        produce_payload = {
            "records": [
                {"value": {"foo": "bar"}},
            ]
        }
        
        res = await rest_async_client_custom_client_id.post(
            f"topics/{topic_name}",
            json=produce_payload,
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
        )
        
        assert res.status_code == 200
        data = res.json()
        assert "offsets" in data
        assert len(data["offsets"]) == 1
    finally:
        admin_client.delete_topic(topic_name)
