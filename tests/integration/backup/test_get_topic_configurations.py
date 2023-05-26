"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from karapace.backup.topic_configurations import ALL_CONFIG_SOURCES, ConfigSource, DEFAULT_CONFIGS, get_topic_configurations
from karapace.constants import TOPIC_CREATION_TIMEOUT_MS
from typing import Dict

import pytest
import secrets


@pytest.fixture(scope="function", name="new_topic")
def topic_fixture(admin_client: KafkaAdminClient) -> NewTopic:
    new_topic = NewTopic(secrets.token_hex(4), 1, 1)
    admin_client.create_topics([new_topic], timeout_ms=TOPIC_CREATION_TIMEOUT_MS)
    try:
        yield new_topic
    finally:
        admin_client.delete_topics([new_topic.name], timeout_ms=TOPIC_CREATION_TIMEOUT_MS)


class TestTopicConfiguration:
    @pytest.mark.parametrize("custom_topic_configs", [{}, {"max.message.bytes": "1234"}])
    def test_get_custom_topic_configurations(
        self,
        new_topic: NewTopic,
        admin_client: KafkaAdminClient,
        custom_topic_configs: Dict[str, str],
    ) -> None:
        admin_client.alter_configs([ConfigResource(ConfigResourceType.TOPIC, new_topic.name, configs=custom_topic_configs)])

        retrieved_configs = get_topic_configurations(
            admin_client, new_topic.name, config_source_filter={ConfigSource.TOPIC_CONFIG}
        )

        # Verify that default configs are retrieved, and then remove them
        for default_config in DEFAULT_CONFIGS:
            assert default_config in retrieved_configs
            del retrieved_configs[default_config]

        # Verify that all custom topic configs are correctly retrieved and no other config is present
        assert retrieved_configs == custom_topic_configs

    def test_get_only_default_topic_configurations(
        self,
        new_topic: NewTopic,
        admin_client: KafkaAdminClient,
    ) -> None:
        custom_topic_configs = {"segment.bytes": "7890"}
        admin_client.alter_configs([ConfigResource(ConfigResourceType.TOPIC, new_topic.name, configs=custom_topic_configs)])

        retrieved_configs = get_topic_configurations(admin_client, new_topic.name, config_source_filter=())

        # Verify that default configs are retrieved, and then remove them
        for default_config in DEFAULT_CONFIGS:
            assert default_config in retrieved_configs
            del retrieved_configs[default_config]

        # Verify that only default configs are retrieved
        assert retrieved_configs == {}

    def test_get_all_topic_configurations(
        self,
        new_topic: NewTopic,
        admin_client: KafkaAdminClient,
    ) -> None:
        custom_topic_configs = {"flush.ms": "999"}
        admin_client.alter_configs([ConfigResource(ConfigResourceType.TOPIC, new_topic.name, configs=custom_topic_configs)])

        retrieved_configs = get_topic_configurations(admin_client, new_topic.name, config_source_filter=ALL_CONFIG_SOURCES)

        # Verify that default configs are retrieved, and then remove them
        for default_config in DEFAULT_CONFIGS:
            assert default_config in retrieved_configs
            del retrieved_configs[default_config]

        # Verify that all custom topic configs are correctly retrieved, and then remove them
        for custom_config_key, custom_config_value in custom_topic_configs.items():
            assert retrieved_configs[custom_config_key] == custom_config_value
            del retrieved_configs[custom_config_key]

        # Verify that also other configs are retrieved
        assert retrieved_configs
