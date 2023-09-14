"""
karapace

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses import field
from karapace.avro_dataclasses.models import AvroModel
from karapace.dataclasses import default_dataclass
from typing import Mapping, Optional, Tuple

import datetime
import enum
import uuid


@enum.unique
class ChecksumAlgorithm(enum.Enum):
    # Note: We have to have a default value that is unknown here, as otherwise there
    # would be no purpose at all in representing this as an enum, due to how the Avro
    # semantics work. If we introduce a new algorithm, an old version of Karapace will
    # infer the default value of an enum field, so if we didn't have an unknown value,
    # it would default to xxhash3, and just be broken without making sense.
    unknown = "unknown"
    xxhash3_64_be = "xxhash3_64_be"


@default_dataclass
class DataFile(AvroModel):
    filename: str
    partition: int = field(metadata={"type": "long"})
    checksum: bytes
    record_count: int = field(metadata={"type": "long"})
    start_offset: int
    end_offset: int

    def __post_init__(self) -> None:
        assert self.record_count >= 0
        assert self.partition >= 0
        assert self.checksum
        assert self.filename
        assert self.start_offset <= self.end_offset
        assert self.end_offset - self.start_offset + 1 >= self.record_count


@default_dataclass
class Metadata(AvroModel):
    version: int = field(metadata={"type": "int"})
    tool_name: str
    tool_version: str
    started_at: datetime.datetime
    finished_at: datetime.datetime
    record_count: int = field(metadata={"type": "int"})
    topic_name: str
    topic_id: Optional[uuid.UUID]
    partition_count: int = field(metadata={"type": "int"})
    replication_factor: int = field(metadata={"type": "int"})
    topic_configurations: Mapping[str, str]
    data_files: Tuple[DataFile, ...]
    checksum_algorithm: ChecksumAlgorithm = ChecksumAlgorithm.unknown

    def __post_init__(self) -> None:
        assert len(self.data_files) == self.partition_count
        assert self.topic_name
        assert self.finished_at >= self.started_at
        assert self.version == 3
        assert self.record_count == sum(data_file.record_count for data_file in self.data_files)


@default_dataclass
class Header(AvroModel):
    key: bytes
    value: bytes


@default_dataclass
class Record(AvroModel):
    key: Optional[bytes]
    value: Optional[bytes]
    headers: Tuple[Header, ...]
    offset: int = field(metadata={"type": "long"})
    timestamp: int = field(metadata={"type": "long"})
    # In order to reduce the impact of checksums on total file sizes, especially
    # when key + value + headers is small, we only write checksums to a subset
    # of records. When restoring, we accumulate parsed records until
    # encountering a checkpoint, verify the running checksum against it, and
    # only then produce the verified records to Kafka.
    checksum_checkpoint: Optional[bytes]

    def __post_init__(self) -> None:
        assert self.offset >= 0
        assert self.timestamp >= 0
