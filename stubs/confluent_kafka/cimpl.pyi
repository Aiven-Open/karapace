from confluent_kafka.admin._metadata import ClusterMetadata
from typing import Any, Callable

class KafkaError:
    _NOENT: int
    _AUTHENTICATION: int
    _UNKNOWN_TOPIC: int
    _UNKNOWN_PARTITION: int

    def code(self) -> int: ...

class KafkaException(Exception):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args: tuple[KafkaError]

class NewTopic:
    def __init__(
        self,
        topic: str,
        num_partitions: int = -1,
        replication_factor: int = -1,
        replica_assignment: list | None = None,
        config: dict[str, str] | None = None,
    ) -> None:
        self.topic: str

class TopicPartition:
    def __init__(
        self,
        topic: str,
        partition: int = -1,
        offset: int = -1001,
        metadata: str | None = None,
        leader_epoc: int | None = None,
    ) -> None: ...

class Message:
    def offset(self) -> int: ...
    def timestamp(self) -> tuple[int, int]: ...
    def key(self) -> str | bytes | None: ...
    def value(self) -> str | bytes | None: ...
    def topic(self) -> str: ...
    def partition(self) -> int: ...

class Producer:
    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int = -1,
        on_delivery: Callable[[KafkaError, Message], Any] | None = None,
        timestamp: int | None = -1,
        headers: dict[str | None, bytes | None] | list[tuple[str | None, bytes | None]] | None = None,
    ) -> None: ...
    def flush(self, timeout: float = -1) -> None: ...
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> ClusterMetadata: ...
    def poll(self, timeout: float = -1) -> int: ...
