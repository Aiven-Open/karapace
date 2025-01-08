"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from .checksum import RunningChecksum
from .constants import V3_MARKER
from .errors import InvalidChecksum, InvalidHeader, TooFewRecords, TooManyRecords, UnexpectedEndOfData
from .schema import Metadata, Record
from collections.abc import Generator
from karapace.avro_dataclasses.models import AvroModel
from typing import IO, TypeVar

import io
import struct


def read_uint32(buffer: IO[bytes]) -> int:
    return struct.unpack(">I", buffer.read(4))[0]


def read_uint64(buffer: IO[bytes]) -> int:
    return struct.unpack(">Q", buffer.read(8))[0]


M = TypeVar("M", bound=AvroModel)


def read_sized(buffer: IO[bytes], type_: type[M]) -> M:
    size = read_uint32(buffer)
    with io.BytesIO(buffer.read(size)) as view:
        return type_.parse(view)


def read_metadata(buffer: IO[bytes]) -> Metadata:
    header = buffer.read(4)
    if header != V3_MARKER:
        raise InvalidHeader(f"Expected to read V3 header in metadata file, found: {header!r}")
    return read_sized(buffer, Metadata)


R = TypeVar("R", bound=RunningChecksum)


def read_record(buffer: IO[bytes], running_checksum: R) -> Record:
    """
    Decode a length-value encoded envelope from `buffer`, verify its checksum is intact
    and return the wrapped record decoded. This function assumes that `running_checksum`
    is mutable, and it will call its `.update()` for all bytes read from the buffer.

    :raises DecodeRecordError:
    :raises MessageNotProcessable: Backup contains a schema-valid message, but the
        message does not contain all data to make it processable. This is a consequence
        of protobuf treating everything as optional
    """
    # Parse record and record start and end offsets.
    offset_start = buffer.tell()
    try:
        record = read_sized(buffer, Record)
    except struct.error as e:
        raise UnexpectedEndOfData from e
    offset_delta = buffer.tell() - offset_start

    # Verify checksum if current record has a checkpoint.
    if record.checksum_checkpoint is not None and record.checksum_checkpoint != running_checksum.digest():
        raise InvalidChecksum

    # Re-read record bytes and update running checksum.
    buffer.seek(offset_start)
    running_checksum.update(buffer.read(offset_delta))

    return record


def read_records(
    buffer: IO[bytes],
    num_records: int,
    running_checksum: R,
) -> Generator[Record, None, None]:
    for record_count in range(num_records):
        try:
            yield read_record(buffer, running_checksum)
        except InvalidChecksum as e:
            raise InvalidChecksum(
                f"Found invalid checksum at record number {record_count} (counting "
                f"from 0), file byte offset {buffer.tell()}."
            ) from e
        except UnexpectedEndOfData as e:
            raise TooFewRecords("Data file contains fewer records than expected.") from e

    if buffer.read(1) != b"":
        raise TooManyRecords("Data file contains data beyond last expected record.")
