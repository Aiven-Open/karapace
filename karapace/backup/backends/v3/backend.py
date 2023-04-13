"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from .checksum import RunningChecksum
from .errors import InvalidChecksum, UnknownChecksumAlgorithm
from .readers import read_metadata, read_records
from .schema import ChecksumAlgorithm, DataFile, Header, Metadata, Record
from .writers import write_metadata, write_record
from dataclasses import dataclass
from kafka.consumer.fetcher import ConsumerRecord
from karapace.backup.backends.reader import BaseBackupReader, Instruction, ProducerSend, RestoreTopic
from karapace.backup.backends.writer import BytesBackupWriter, StdOut
from karapace.backup.safe_writer import bytes_writer, staging_directory
from karapace.utils import assert_never
from karapace.version import __version__
from pathlib import Path
from typing import Callable, ContextManager, Final, IO, Iterator, Sequence, TypeVar

import datetime
import uuid
import xxhash


def _get_checksum_implementation(algorithm: ChecksumAlgorithm) -> Callable[[], RunningChecksum]:
    if algorithm is ChecksumAlgorithm.xxhash3_64_be:
        return xxhash.xxh64
    if algorithm is ChecksumAlgorithm.unknown:
        raise UnknownChecksumAlgorithm(
            "The backup file uses a checksum algorithm that this version of Karapace is not aware of."
        )
    assert_never(algorithm)


class SchemaBackupV3Reader(BaseBackupReader):
    def _read_data_file(
        self,
        path: Path,
        metadata: Metadata,
        data_file: DataFile,
    ) -> Iterator[ProducerSend]:
        running_checksum = _get_checksum_implementation(metadata.checksum_algorithm)()

        with path.open("rb") as data_buffer:
            for record in read_records(
                buffer=data_buffer,
                num_records=data_file.record_count,
                running_checksum=running_checksum,
            ):
                yield ProducerSend(
                    value=record.value,
                    key=record.key,
                    headers=tuple((header.key, header.value) for header in record.headers),
                    timestamp=record.timestamp,
                    topic_name=metadata.topic_name,
                    partition_index=data_file.partition,
                )

        if running_checksum.digest() != data_file.checksum:
            raise InvalidChecksum("Found checksum mismatch after reading full data file.")

    # Not part of common interface, because it exposes Metadata which is
    # specific to V3.
    def read_metadata(self, path: Path) -> Metadata:
        with path.open("rb") as buffer:
            return read_metadata(buffer)

    def read(self, path: Path, topic_name: str) -> Iterator[Instruction]:
        with path.open("rb") as metadata_buffer:
            metadata = read_metadata(metadata_buffer)

        if topic_name != metadata.topic_name:
            raise RuntimeError("Mismatch between topic name found in metadata and given topic name.")

        if metadata.checksum_algorithm is ChecksumAlgorithm.unknown:
            raise UnknownChecksumAlgorithm("Tried restoring from a backup with an unknown checksum algorithm.")

        yield RestoreTopic(
            name=topic_name,
            partition_count=metadata.partition_count,
        )

        for data_file in metadata.data_files:
            yield from self._read_data_file(
                path=(path.parent / data_file.filename),
                metadata=metadata,
                data_file=data_file,
            )


# Note: Because the underlying checksum API is mutable, it doesn't make sense to attempt
# to make this immutable.
@dataclass
class _PartitionStats:
    running_checksum: RunningChecksum
    records_written: int = 0
    bytes_written_since_checkpoint: int = 0
    records_written_since_checkpoint: int = 0

    def get_checkpoint(
        self,
        records_threshold: int,
        bytes_threshold: int,
    ) -> bytes | None:
        if (
            self.records_written_since_checkpoint >= records_threshold
            or self.bytes_written_since_checkpoint >= bytes_threshold
        ):
            self.records_written_since_checkpoint = 0
            self.bytes_written_since_checkpoint = 0
            return self.running_checksum.digest()
        return None

    def increase(self, bytes_offset: int) -> None:
        self.records_written_since_checkpoint += 1
        self.records_written += 1
        self.bytes_written_since_checkpoint += bytes_offset


class SchemaBackupV3Writer(BytesBackupWriter[DataFile]):
    def __init__(
        self,
        checksum_algorithm: ChecksumAlgorithm = ChecksumAlgorithm.xxhash3_64_be,
        max_records_per_checkpoint: int = 100,
        max_bytes_per_checkpoint: int = 16_384,
    ) -> None:
        self._checksum_implementation: Final = _get_checksum_implementation(checksum_algorithm)
        self._max_records_per_checkpoint: Final = max_records_per_checkpoint
        self._max_bytes_per_checkpoint: Final = max_bytes_per_checkpoint
        self._partition_stats: Final[dict[int, _PartitionStats]] = {}

    P = TypeVar("P", bound="StdOut | Path")

    def prepare_location(
        self,
        topic_name: str,
        backup_location: P,
    ) -> ContextManager[P]:
        """
        Return a context manager that's responsible for the lifecycle of a
        temporary staging directory, next to the target path
        """
        if not isinstance(backup_location, Path):
            raise RuntimeError("Cannot use stdout with backup format V3")
        return staging_directory(backup_location / f"topic-{topic_name}")  # type: ignore[return-value]

    @staticmethod
    def _build_data_file_name(
        path: Path,
        topic_name: str,
        partition_index: int,
    ) -> Path:
        # Using colon (:) as delimiter, as this is not valid inside a topic name.
        return path / f"{topic_name}:{partition_index}.data"

    def start_partition(
        self,
        path: Path | StdOut,
        topic_name: str,
        index: int,
    ) -> Path:
        if not isinstance(path, Path):
            raise RuntimeError("Cannot use stdout with backup format V3")
        if index in self._partition_stats:
            raise RuntimeError(f"Already started backing up partition {index}")
        self._partition_stats[index] = _PartitionStats(running_checksum=self._checksum_implementation())
        return self._build_data_file_name(path, topic_name, index)

    def finalize_partition(self, index: int, filename: str) -> DataFile:
        stats = self._partition_stats.pop(index)
        return DataFile(
            filename=filename,
            partition=index,
            checksum=stats.running_checksum.digest(),
            record_count=stats.records_written,
        )

    def store_metadata(
        self,
        path: StdOut | Path,
        topic_name: str,
        topic_id: uuid.UUID | None,
        started_at: datetime.datetime,
        finished_at: datetime.datetime,
        data_files: Sequence[DataFile],
    ) -> None:
        assert isinstance(path, Path)
        metadata_path = path / f"{topic_name}.metadata"

        if len(data_files) != 1:
            raise RuntimeError("Cannot backup multi-partition topics")

        if len(self._partition_stats):
            raise RuntimeError("Cannot write metadata when not all partitions are finalized")

        with bytes_writer(metadata_path, False) as buffer:
            write_metadata(
                buffer,
                metadata=Metadata(
                    version=3,
                    tool_name="karapace",
                    tool_version=__version__,
                    started_at=started_at,
                    finished_at=finished_at,
                    topic_name=topic_name,
                    topic_id=topic_id,
                    partition_count=len(data_files),
                    checksum_algorithm=ChecksumAlgorithm.xxhash3_64_be,
                    data_files=tuple(data_files),
                ),
            )

    def store_record(
        self,
        buffer: IO[bytes],
        record: ConsumerRecord,
    ) -> None:
        stats: Final = self._partition_stats[record.partition]
        checksum_checkpoint: Final = stats.get_checkpoint(
            records_threshold=self._max_records_per_checkpoint,
            bytes_threshold=self._max_bytes_per_checkpoint,
        )
        offset_start: Final = buffer.tell()
        write_record(
            buffer,
            record=Record(
                key=record.key,
                value=record.value,
                headers=tuple(Header(key=key.encode(), value=value) for key, value in record.headers),
                offset=record.offset,
                timestamp=record.timestamp,
                checksum_checkpoint=checksum_checkpoint,
            ),
            running_checksum=stats.running_checksum,
        )
        stats.increase(buffer.tell() - offset_start)
