"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Container
from confluent_kafka.admin import ConfigSource
from karapace.kafka.admin import KafkaAdminClient
from typing import Final

ALL_CONFIG_SOURCES: Final = ConfigSource


DEFAULT_CONFIGS: Final = [
    "cleanup.policy",
    "min.insync.replicas",
    "retention.bytes",
    "retention.ms",
]


def get_topic_configurations(
    admin: KafkaAdminClient,
    topic_name: str,
    config_source_filter: Container[ConfigSource] = (),
) -> dict[str, str]:
    """Get configurations of the specified topic. The following configurations will be retrieved by default:
        - `cleanup.policy`
        - `min.insync.replicas`
        - `retention.bytes`
        - `retention.ms`

    :param admin: Kafka admin client
    :param topic_name: get configurations for this topic
    :param config_source_filter: returns all the configurations that match the sources specified,
        plus the default configurations. If empty, returns only the default configurations.
    """
    return admin.get_topic_config(
        topic_name,
        config_name_filter=DEFAULT_CONFIGS,
        config_source_filter=config_source_filter,
    )
