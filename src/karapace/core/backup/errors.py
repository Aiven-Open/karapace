"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from confluent_kafka import TopicPartition
from karapace.core.backup.poll_timeout import PollTimeout

__all__ = ["BackupError", "BackupTopicAlreadyExists", "EmptyPartition", "PartitionCountError", "StaleConsumerError"]


class BackupError(Exception):
    """Baseclass for all backup errors."""


class EmptyPartition(BackupError): ...


class PartitionCountError(BackupError):
    pass


class BackupTopicAlreadyExists(BackupError):
    pass


class BackupDataRestorationError(BackupError):
    pass


class StaleConsumerError(BackupError, RuntimeError):
    """Raised when the backup consumer does not make any progress and has not reached the last record in the topic."""

    __slots__ = ("__topic_partition", "__start_offset", "__end_offset", "__last_offset", "__poll_timeout")

    def __init__(
        self,
        topic_partition: TopicPartition,
        start_offset: int,
        end_offset: int,
        current_offset: int,
        poll_timeout: PollTimeout,
    ) -> None:
        super().__init__(
            f"{topic_partition.topic}:{topic_partition.partition}#{current_offset:,} ({start_offset:,},{end_offset:,})"
            f" after {poll_timeout}"
        )
        self.__topic_partition = topic_partition
        self.__start_offset = start_offset
        self.__end_offset = end_offset
        self.__last_offset = current_offset
        self.__poll_timeout = poll_timeout

    @property
    def topic_partition(self) -> TopicPartition:
        """Gets the topic and partition that went stale during consumption."""
        return self.__topic_partition

    @property
    def topic(self) -> str:
        """Gets the topic that went stale during consumption."""
        return self.__topic_partition.topic

    @property
    def partition(self) -> int:
        """Gets the partition that went stale during consumption."""
        return self.__topic_partition.partition

    @property
    def start_offset(self) -> int:
        """Gets the start offset of the topic and partition as determined at the start of the backup creation."""
        return self.__start_offset

    @property
    def end_offset(self) -> int:
        """Gets the end offset of the topic and partition as determined at the start of the backup creation.

        This is the offset of the last written record in the topic and partition, not the high watermark.
        """
        return self.__end_offset

    @property
    def last_offset(self) -> int:
        """Gets the last offset of the topic and partition that was successfully consumed."""
        return self.__last_offset

    @property
    def poll_timeout(self) -> PollTimeout:
        """Gets the poll timeout with which the consumer went stale while waiting for more records."""
        return self.__poll_timeout
