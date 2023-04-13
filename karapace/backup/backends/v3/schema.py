"""
karapace

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses_avroschema import AvroModel
from karapace.dataclasses import default_dataclass
from typing import Optional, Tuple
from typing_extensions import Annotated, TypeAlias

import datetime
import enum
import uuid

i32: TypeAlias = Annotated[int, "Int32"]
i64: TypeAlias = Annotated[int, "Int64"]


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
    partition: i64
    checksum: bytes
    record_count: i64

    def __post_init__(self) -> None:
        assert self.record_count >= 0
        assert self.partition >= 0
        assert self.checksum
        assert self.filename


@default_dataclass
class Metadata(AvroModel):
    version: i32
    tool_name: str
    tool_version: str
    started_at: datetime.datetime
    finished_at: datetime.datetime
    topic_name: str
    topic_id: Optional[uuid.UUID]
    partition_count: i32
    data_files: Tuple[DataFile, ...]
    checksum_algorithm: ChecksumAlgorithm = ChecksumAlgorithm.unknown

    def __post_init__(self) -> None:
        assert len(self.data_files) == self.partition_count
        assert self.topic_name
        assert self.finished_at >= self.started_at
        assert self.partition_count == 1
        assert self.version == 3


@default_dataclass
class Header(AvroModel):
    key: bytes
    value: bytes


@default_dataclass
class Record(AvroModel):
    key: Optional[bytes]
    value: Optional[bytes]
    headers: Tuple[Header, ...]
    offset: i64
    timestamp: i64
    # In order to reduce the impact of checksums on total file sizes, especially
    # when key + value + headers is small, we only write checksums to a subset
    # of records. When restoring, we accumulate parsed records until
    # encountering a checkpoint, verify the running checksum against it, and
    # only then produce the verified records to Kafka.
    checksum_checkpoint: Optional[bytes]

    def __post_init__(self) -> None:
        assert self.offset >= 0
        assert self.timestamp >= 0
