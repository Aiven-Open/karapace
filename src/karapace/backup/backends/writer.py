"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from confluent_kafka import Message
from karapace.backup.safe_writer import bytes_writer, str_writer
from pathlib import Path
from typing import ContextManager, Generic, IO, Literal, TypeAlias, TypeVar

import abc
import contextlib
import datetime
import uuid

StdOut: TypeAlias = Literal["", "-"]


B = TypeVar("B", str, bytes)
F = TypeVar("F")
T = TypeVar("T")


@contextlib.contextmanager
def _noop_context(path: T) -> Iterator[T]:
    yield path


class BackupWriter(Generic[B, F], abc.ABC):
    """Common interface and base class for all backup writer backends."""

    P = TypeVar("P", bound="StdOut | Path")

    def prepare_location(
        self,
        backup_location: P,
    ) -> ContextManager[P]:
        """
        Hook for setting up a directory to write backup files to.

        Overriding this is optional, and default behavior is to use the given location
        as-is.
        """
        return _noop_context(backup_location)

    def start_partition(
        self,
        path: Path | StdOut,
        topic_name: str,
        index: int,
    ) -> Path | StdOut:
        """
        Hook called before any calls are made to .store_record() for a partition, so
        that initialization for record metrics can be set up.

        Overriding this is optional.
        """
        return path

    def finalize_partition(  # type: ignore[empty-body]
        self,
        index: int,
        filename: str,
    ) -> F:
        """
        Hook called after calls to .store_record() have been made for each record of a
        partition, to enable collection of accumulated metrics.

        Overriding this is optional.
        """

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
        data_files: Sequence[F],
    ) -> None:
        """
        Called after all partitions have been finalized. The values returned by
        .finalize_partition() will be passed as a sequence in the data_files parameter,
        so that metrics accumulated per partition can be used to write separate metadata
        for the topic.

        Overriding this is optional.
        """

    @abc.abstractmethod
    def store_record(
        self,
        buffer: IO[B],
        record: Message,
    ) -> None:
        """
        Called in order for each record read from a topic to be backed up. It's safe to
        assume that .start_partition() has been called before this method is called with
        a record read from a partition, and that .finalize_partition() will be called
        after this method has successfully received calls for every record in the
        partition.

        The buffer argument will be whatever the context manager returned by
        .safe_writer() yields.

        Overriding this is mandatory.
        """

    @classmethod
    @abc.abstractmethod
    def safe_writer(cls, target: Path | StdOut, allow_overwrite: bool) -> ContextManager[IO[B]]:
        """
        Called once for each partition in a topic. The returned context manager will be
        open during all calls to .store_record() for a partition, and entered into after
        the call to .start_partition(), and exited before the call to
        .finalize_partition().

        The reason for this method is so that each backend is responsible for setting up
        a buffer that it can write to. This enables having backends choosing whether to
        write str or bytes.

        Overriding this is mandatory.
        """


class StrBackupWriter(BackupWriter[str, None], abc.ABC):
    @classmethod
    def safe_writer(
        cls,
        target: Path | StdOut,
        allow_overwrite: bool,
    ) -> ContextManager[IO[str]]:
        return str_writer(target, allow_overwrite)


class BytesBackupWriter(BackupWriter[bytes, F], Generic[F], abc.ABC):
    @classmethod
    def safe_writer(
        cls,
        target: Path | StdOut,
        allow_overwrite: bool,
    ) -> ContextManager[IO[bytes]]:
        return bytes_writer(target, allow_overwrite)


class BaseKVBackupWriter(StrBackupWriter, abc.ABC):
    def store_record(
        self,
        buffer: IO[str],
        record: Message,
    ) -> None:
        record_key = record.key()
        record_value = record.value()
        buffer.write(
            self.serialize_record(
                record_key.encode() if isinstance(record_key, str) else record_key,
                record_value.encode() if isinstance(record_value, str) else record_value,
            )
        )

    @staticmethod
    @abc.abstractmethod
    def serialize_record(
        key_bytes: bytes | None,
        value_bytes: bytes | None,
    ) -> str: ...
