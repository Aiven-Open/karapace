class ClusterMetadata:
    def __init__(self) -> None:
        self.cluster_id: str | None
        self.controller_id: int
        self.brokers: dict[int, BrokerMetadata]
        self.topics: dict[str, TopicMetadata]
        self.orig_broker_id: int
        self.orig_broker_name: str | None

class BrokerMetadata: ...

class TopicMetadata:
    def __init__(self) -> None:
        self.topic: str
        self.partitions: dict[int, PartitionMetadata]

class PartitionMetadata:
    def __init__(self) -> None:
        self.id: int
        self.leader: int
        self.replicas: list[int]
        self.isrs: list[int]
