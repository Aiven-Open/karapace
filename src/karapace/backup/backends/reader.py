"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from karapace.dataclasses import default_dataclass
from karapace.typing import JsonData, JsonObject
from pathlib import Path
from typing import ClassVar, Final, IO, Optional, TypeAlias, TypeVar, Union

import abc

# Schema topic has single partition. Use of this in `producer.send` disables the
# partitioner to calculate which partition the data is sent.
PARTITION_ZERO: Final = 0


@default_dataclass
class RestoreTopicLegacy:
    topic_name: str
    partition_count: int


@default_dataclass
class RestoreTopic:
    topic_name: str
    partition_count: int
    replication_factor: int
    topic_configs: Mapping[str, str]


@default_dataclass
class ProducerSend:
    topic_name: str
    partition_index: int
    key: bytes | None
    value: bytes | None
    headers: Sequence[tuple[bytes | None, bytes | None]] = ()
    timestamp: int | None = None


Instruction: TypeAlias = "RestoreTopicLegacy | RestoreTopic | ProducerSend"


KeyEncoder: TypeAlias = Callable[[Union[JsonObject, str]], Optional[bytes]]
ValueEncoder: TypeAlias = Callable[[JsonData], Optional[bytes]]
B = TypeVar("B", str, bytes)


class BaseBackupReader(abc.ABC):
    """Common interface and base class for all backup reader backends."""

    @abc.abstractmethod
    def read(
        self,
        path: Path,
        topic_name: str,
    ) -> Iterator[Instruction]:
        """
        Implementations are expected to override this method with a generator that
        produces instances of Instruction. The common API consumes instructions from the
        generator handling them one-by-one to fully restore a topic.
        """


class BaseItemsBackupReader(BaseBackupReader, abc.ABC):
    marker_size: ClassVar = 0

    def __init__(
        self,
        key_encoder: KeyEncoder,
        value_encoder: ValueEncoder,
    ) -> None:
        self.key_encoder: Final = key_encoder
        self.value_encoder: Final = value_encoder

    @staticmethod
    @abc.abstractmethod
    def items_from_file(fp: IO[str]) -> Iterator[Sequence[str]]: ...

    def read(
        self,
        path: Path,
        topic_name: str,
    ) -> Generator[Instruction, None, None]:
        yield RestoreTopicLegacy(
            topic_name=topic_name,
            partition_count=1,
        )
        with path.open("r") as buffer:
            buffer.seek(self.marker_size)
            for item in self.items_from_file(buffer):
                key, value = item
                yield ProducerSend(
                    topic_name=topic_name,
                    partition_index=PARTITION_ZERO,
                    key=self.key_encoder(key),
                    value=self.value_encoder(value),
                )
