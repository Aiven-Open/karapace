from typing import Any

class KafkaError:
    _NOENT: int
    _AUTHENTICATION: int

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
