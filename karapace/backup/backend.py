"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from kafka.consumer.fetcher import ConsumerRecord
from karapace.typing import JsonData, JsonObject
from typing import Callable, Final, Generator, Generic, IO, Iterator, Optional, Sequence, TypeVar, Union
from typing_extensions import TypeAlias

import abc
import dataclasses
import logging

logger = logging.getLogger(__name__)


# Schema topic has single partition. Use of this in `producer.send` disables the
# partitioner to calculate which partition the data is sent.
PARTITION_ZERO: Final = 0


@dataclasses.dataclass(frozen=True)
class ProducerSend:
    topic_name: str
    value: bytes | None
    key: bytes | None
    headers: Sequence[tuple[bytes | None, bytes | None]] | None = None
    partition: int | None = None
    timestamp_ms: int | None = None


KeyEncoder: TypeAlias = Callable[[Union[JsonObject, str]], Optional[bytes]]
ValueEncoder: TypeAlias = Callable[[JsonData], Optional[bytes]]
B = TypeVar("B", bound="IO[bytes] | IO[str]")


class BaseBackupReader(abc.ABC, Generic[B]):
    @abc.abstractmethod
    def read(
        self,
        topic_name: str,
        buffer: B,
    ) -> Iterator[ProducerSend]:
        ...


class BaseItemsBackupReader(BaseBackupReader[IO[str]]):
    def __init__(
        self,
        key_encoder: KeyEncoder,
        value_encoder: ValueEncoder,
    ) -> None:
        self.key_encoder: Final = key_encoder
        self.value_encoder: Final = value_encoder

    @staticmethod
    @abc.abstractmethod
    def items_from_file(fp: IO[str]) -> Iterator[tuple[str, str]]:
        ...

    def read(
        self,
        topic_name: str,
        buffer: IO[str],
    ) -> Generator[ProducerSend, None, None]:
        for item in self.items_from_file(buffer):
            key, value = item
            yield ProducerSend(
                topic_name=topic_name,
                key=self.key_encoder(key),
                value=self.value_encoder(value),
                partition=PARTITION_ZERO,
            )


class BaseBackupWriter(abc.ABC, Generic[B]):
    @classmethod
    @abc.abstractmethod
    def store_record(
        cls,
        buffer: B,
        record: ConsumerRecord,
    ) -> None:
        ...


class BaseKVBackupWriter(BaseBackupWriter[IO[str]]):
    @classmethod
    def store_record(
        cls,
        buffer: IO[str],
        record: ConsumerRecord,
    ) -> None:
        buffer.write(cls.serialize_record(record.key, record.value))

    @staticmethod
    @abc.abstractmethod
    def serialize_record(
        key_bytes: bytes | None,
        value_bytes: bytes | None,
    ) -> str:
        ...
