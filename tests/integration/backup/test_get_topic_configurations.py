"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from karapace.backup.topic_configurations import ALL_CONFIG_SOURCES, ConfigSource, DEFAULT_CONFIGS, get_topic_configurations
from karapace.kafka_admin import KafkaAdminClient, NewTopic

import pytest


class TestTopicConfiguration:
    @pytest.mark.parametrize("custom_topic_configs", [{}, {"max.message.bytes": "1234"}])
    def test_get_custom_topic_configurations(
        self,
        new_topic: NewTopic,
        admin_client: KafkaAdminClient,
        custom_topic_configs: dict[str, str],
    ) -> None:
        admin_client.update_topic_config(new_topic.topic, custom_topic_configs)

        retrieved_configs = get_topic_configurations(
            admin_client, new_topic.topic, config_source_filter={ConfigSource.DYNAMIC_TOPIC_CONFIG}
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
        admin_client.update_topic_config(new_topic.topic, {"segment.bytes": "7890"})

        retrieved_configs = get_topic_configurations(admin_client, new_topic.topic, config_source_filter=())

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
        admin_client.update_topic_config(new_topic.topic, {"flush.ms": "999"})

        retrieved_configs = get_topic_configurations(admin_client, new_topic.topic, config_source_filter=ALL_CONFIG_SOURCES)

        # Verify that default configs are retrieved, and then remove them
        for default_config in DEFAULT_CONFIGS:
            assert default_config in retrieved_configs
            del retrieved_configs[default_config]

        # Verify that all custom topic configs are correctly retrieved, and then remove them
        for custom_config_key, custom_config_value in ({"flush.ms": "999"}).items():
            assert retrieved_configs[custom_config_key] == custom_config_value
            del retrieved_configs[custom_config_key]

        # Verify that also other configs are retrieved
        assert retrieved_configs
