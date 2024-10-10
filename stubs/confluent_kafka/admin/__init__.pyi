from ..cimpl import NewTopic
from ._config import ConfigEntry, ConfigResource, ConfigSource
from ._listoffsets import ListOffsetsResultInfo, OffsetSpec
from ._metadata import BrokerMetadata, ClusterMetadata, PartitionMetadata, TopicMetadata
from ._resource import ResourceType
from concurrent.futures import Future
from confluent_kafka import IsolationLevel, TopicCollection, TopicPartition
from typing import Callable

__all__ = (
    "AdminClient",
    "BrokerMetadata",
    "ClusterMetadata",
    "ConfigResource",
    "ConfigSource",
    "NewTopic",
    "OffsetSpec",
    "PartitionMetadata",
    "ResourceType",
    "TopicMetadata",
)

class AdminClient:
    def __init__(self, config: dict[str, str | int | Callable]) -> None: ...
    def poll(self, timeout: float = -1) -> int: ...
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> ClusterMetadata: ...
    def create_topics(
        self,
        new_topics: list[NewTopic],
        operation_timeout: float = 0,
        request_timeout: float = -1,
        validate_only: bool = False,
    ) -> dict[str, Future[None]]: ...
    def alter_configs(
        self,
        resources: list[ConfigResource],
        request_timeout: float = -1,
        validate_only: bool = False,
    ) -> dict[str, Future[ConfigResource]]: ...
    def delete_topics(
        self,
        topics: list[str],
        operation_timeout: float = 0,
        request_timeout: float = -1,
    ) -> dict[str, Future[None]]: ...
    def list_offsets(
        self,
        topic_partition_offsets: dict[TopicPartition, OffsetSpec],
        isolation_level: IsolationLevel | None = None,
        request_timeout: float = -1,
    ) -> dict[TopicPartition, Future[ListOffsetsResultInfo]]: ...
    def describe_configs(
        self, resources: list[ConfigResource], request_timeout: float = -1
    ) -> dict[ConfigResource, Future[dict[str, ConfigEntry]]]: ...
    def describe_topics(self, topics: TopicCollection) -> dict[str, Future]: ...
