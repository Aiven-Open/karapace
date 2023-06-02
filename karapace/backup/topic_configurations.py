"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from enum import Enum
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import for_code
from kafka.protocol.admin import DescribeConfigsRequest
from typing import Container, Dict, Final


class ConfigSource(int, Enum):
    UNKNOWN = 0
    TOPIC_CONFIG = 1
    DYNAMIC_BROKER_CONFIG = 2
    DYNAMIC_DEFAULT_BROKER_CONFIG = 3
    STATIC_BROKER_CONFIG = 4
    DEFAULT_CONFIG = 5
    DYNAMIC_BROKER_LOGGER_CONFIG = 6


ALL_CONFIG_SOURCES: Final = {item.value for item in ConfigSource.__members__.values()}


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
) -> Dict[str, str]:
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
    if admin._matching_api_version(DescribeConfigsRequest) == 0:  # pylint: disable=protected-access
        raise NotImplementedError("Broker version is not supported")
    req_cfgs = [ConfigResource(ConfigResourceType.TOPIC, topic_name)]
    cfgs = admin.describe_configs(req_cfgs)
    assert len(cfgs) == 1
    assert len(cfgs[0].resources) == 1
    err, _, _, _, config_values = cfgs[0].resources[0]
    if err != 0:
        raise for_code(err)
    topic_config = {}
    for cv in config_values:
        name, val, _, config_source, _, _ = cv
        if name in DEFAULT_CONFIGS or (config_source in config_source_filter):
            topic_config[name] = val
    return topic_config
