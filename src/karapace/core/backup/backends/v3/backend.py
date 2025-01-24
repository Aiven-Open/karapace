"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from .checksum import RunningChecksum
from .errors import DecodeError, InconsistentOffset, InvalidChecksum, OffsetMismatch, UnknownChecksumAlgorithm
from .readers import read_metadata, read_records
from .schema import ChecksumAlgorithm, DataFile, Header, Metadata, Record
from .writers import write_metadata, write_record
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from confluent_kafka import Message
from dataclasses import dataclass
from karapace.core.backup.backends.reader import BaseBackupReader, Instruction, ProducerSend, RestoreTopic
from karapace.core.backup.backends.writer import BytesBackupWriter, StdOut
from karapace.core.backup.safe_writer import bytes_writer, staging_directory
from karapace.core.dataclasses import default_dataclass
from karapace.core.utils import assert_never
from karapace.version import __version__
from pathlib import Path
from typing import ContextManager, Final, IO, TypeAlias, TypeVar

import datetime
import io
import itertools
import uuid
import xxhash


@default_dataclass
class VerifySuccess:
    data_file: DataFile


@default_dataclass
class VerifyFailure:
    data_file: DataFile
    exception: DecodeError | None


VerifyResult: TypeAlias = "VerifySuccess | VerifyFailure"


def _get_checksum_implementation(algorithm: ChecksumAlgorithm) -> Callable[[], RunningChecksum]:
    if algorithm is ChecksumAlgorithm.xxhash3_64_be:
        return xxhash.xxh64
    if algorithm is ChecksumAlgorithm.unknown:
        raise UnknownChecksumAlgorithm(
            "The backup file uses a checksum algorithm that this version of Karapace is not aware of."
        )
    assert_never(algorithm)


T = TypeVar("T")


def _peek(iterator: Iterator[T]) -> tuple[T | None, Iterator[T]]:
    try:
        first = next(iterator)
    except StopIteration:
        return None, iterator
    return first, itertools.chain((first,), iterator)


class SchemaBackupV3Reader(BaseBackupReader):
    def _read_data_file(
        self,
        path: Path,
        metadata: Metadata,
        data_file: DataFile,
    ) -> Iterator[ProducerSend]:
        running_checksum = _get_checksum_implementation(metadata.checksum_algorithm)()

        with path.open("rb") as data_buffer:
            record, records = _peek(
                read_records(
                    buffer=data_buffer,
                    num_records=data_file.record_count,
                    running_checksum=running_checksum,
                )
            )

            # Verify first record matches start offset from data file.
            if record is not None and record.offset != data_file.start_offset:
                raise OffsetMismatch(
                    f"First record in data file does not match expected "
                    f"start_offset (expected: {data_file.start_offset}, "
                    f"actual: {record.offset})."
                )

            for record in records:
                yield ProducerSend(
                    value=record.value,
                    key=record.key,
                    headers=tuple((header.key, header.value) for header in record.headers),
                    timestamp=record.timestamp,
                    topic_name=metadata.topic_name,
                    partition_index=data_file.partition,
                )

        # Verify last record matches end offset from data file.
        if record is not None and record.offset != data_file.end_offset:
            raise OffsetMismatch(
                f"Last record in data file does not match expected "
                f"end_offset (expected: {data_file.end_offset}, "
                f"actual: {record.offset})."
            )

        # Verify checksum matches.
        if running_checksum.digest() != data_file.checksum:
            raise InvalidChecksum("Found checksum mismatch after reading full data file.")

    # Not part of common interface, because it exposes Metadata which is
    # specific to V3.
    def read_metadata(self, path: Path) -> Metadata:
        with path.open("rb") as buffer:
            return read_metadata(buffer)

    def read(self, path: Path, topic_name: str) -> Iterator[Instruction]:
        metadata = self.read_metadata(path)

        if topic_name != metadata.topic_name:
            raise RuntimeError("Mismatch between topic name found in metadata and given topic name.")

        if metadata.checksum_algorithm is ChecksumAlgorithm.unknown:
            raise UnknownChecksumAlgorithm("Tried restoring from a backup with an unknown checksum algorithm.")

        yield RestoreTopic(
            topic_name=topic_name,
            partition_count=metadata.partition_count,
            replication_factor=metadata.replication_factor,
            topic_configs=metadata.topic_configurations,
        )

        for data_file in metadata.data_files:
            yield from self._read_data_file(
                path=(path.parent / data_file.filename),
                metadata=metadata,
                data_file=data_file,
            )

    # Not part of common interface, because it exposes DataFile which is
    # specific to V3.
    def verify_files(self, path: Path) -> Generator[VerifyResult, None, None]:
        metadata = self.read_metadata(path)

        for data_file in metadata.data_files:
            checksum = _get_checksum_implementation(metadata.checksum_algorithm)()
            with (path.parent / data_file.filename).open("rb") as buffer:
                while True:
                    chunk = buffer.read(io.DEFAULT_BUFFER_SIZE)
                    if not chunk:
                        break
                    checksum.update(chunk)
            yield (
                VerifySuccess(data_file=data_file)
                if checksum.digest() == data_file.checksum
                else VerifyFailure(
                    data_file=data_file,
                    exception=None,
                )
            )

    # Not part of common interface, because it exposes DataFile which is
    # specific to V3.
    def verify_records(self, path: Path) -> Generator[VerifyResult, None, None]:
        metadata = self.read_metadata(path)

        for data_file in metadata.data_files:
            try:
                for _ in self.read(path, metadata.topic_name):
                    pass
            except DecodeError as exception:
                yield VerifyFailure(
                    data_file=data_file,
                    exception=exception,
                )
            else:
                yield VerifySuccess(data_file=data_file)


# Note: Because the underlying checksum API is mutable, it doesn't make sense to attempt
# to make this immutable.
@dataclass
class _PartitionStats:
    running_checksum: RunningChecksum
    records_written: int = 0
    bytes_written_since_checkpoint: int = 0
    records_written_since_checkpoint: int = 0
    min_offset: int | None = None
    max_offset: int | None = None

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

    def update(
        self,
        bytes_offset: int,
        record_offset: int,
    ) -> None:
        self.records_written_since_checkpoint += 1
        self.records_written += 1
        self.bytes_written_since_checkpoint += bytes_offset

        # Verify that offsets are strictly increasing.
        if self.max_offset is not None and record_offset <= self.max_offset:
            raise InconsistentOffset(
                f"Read record offset that's less than or equal to an already seen "
                f"record. Expected record offset {record_offset} > {self.max_offset}."
            )

        if self.min_offset is None:
            self.min_offset = record_offset
        self.max_offset = record_offset


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
        backup_location: P,
    ) -> ContextManager[P]:
        """
        Return a context manager that's responsible for the lifecycle of a
        temporary staging directory, next to the target path
        """
        if not isinstance(backup_location, Path):
            raise RuntimeError("Cannot use stdout with backup format V3")
        return staging_directory(backup_location)  # type: ignore[return-value]

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
        if stats.min_offset is None or stats.max_offset is None:
            raise RuntimeError(
                "Cannot call .finalize_partition() before storing partition records. "
                "For empty topics, this method should never be called."
            )
        return DataFile(
            filename=filename,
            partition=index,
            checksum=stats.running_checksum.digest(),
            record_count=stats.records_written,
            start_offset=stats.min_offset,
            end_offset=stats.max_offset,
        )

    def store_metadata(
        self,
        path: StdOut | Path,
        topic_name: str,
        topic_id: uuid.UUID | None,
        started_at: datetime.datetime,
        finished_at: datetime.datetime,
        partition_count: int,
        replication_factor: int,
        topic_configurations: Mapping[str, str],
        data_files: Sequence[DataFile],
    ) -> None:
        assert isinstance(path, Path)
        metadata_path = path / f"{topic_name}.metadata"

        if len(data_files) > 1:
            raise RuntimeError("Cannot backup multi-partition topics")

        if self._partition_stats and data_files:
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
                    record_count=sum(data_file.record_count for data_file in data_files),
                    topic_name=topic_name,
                    topic_id=topic_id,
                    partition_count=partition_count,
                    replication_factor=replication_factor,
                    topic_configurations=topic_configurations,
                    checksum_algorithm=ChecksumAlgorithm.xxhash3_64_be,
                    data_files=tuple(data_files),
                ),
            )

    def store_record(
        self,
        buffer: IO[bytes],
        record: Message,
    ) -> None:
        stats: Final = self._partition_stats[record.partition()]
        checksum_checkpoint: Final = stats.get_checkpoint(
            records_threshold=self._max_records_per_checkpoint,
            bytes_threshold=self._max_bytes_per_checkpoint,
        )
        offset_start: Final = buffer.tell()

        record_key = record.key()
        record_value = record.value()

        write_record(
            buffer,
            record=Record(
                key=record_key.encode() if isinstance(record_key, str) else record_key,
                value=record_value.encode() if isinstance(record_value, str) else record_value,
                headers=tuple(Header(key=key.encode(), value=value) for key, value in record.headers() or []),
                offset=record.offset(),
                timestamp=record.timestamp()[1],
                checksum_checkpoint=checksum_checkpoint,
            ),
            running_checksum=stats.running_checksum,
        )
        stats.update(
            bytes_offset=buffer.tell() - offset_start,
            record_offset=record.offset(),
        )
