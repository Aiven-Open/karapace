"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from .checksum import RunningChecksum
from .constants import V3_MARKER
from .errors import IntegerAboveBound, IntegerBelowBound
from .schema import Metadata, Record
from karapace.avro_dataclasses.models import AvroModel
from typing import Final, IO, NoReturn, TypeVar

import io
import struct

UINT32_RANGE: Final = range(0, 2**32)
UINT64_RANGE: Final = range(0, 2**64)


def _reraise_for_bound(valid: range, value: int, exception: struct.error) -> NoReturn:
    if value < valid.start:
        raise IntegerBelowBound(f"Value is too small for valid {valid}") from exception
    if value >= valid.stop:
        raise IntegerAboveBound(f"Value is too large for valid {valid}") from exception
    raise exception


def write_uint32(buffer: IO[bytes], value: int) -> None:
    # Because try-blocks are cheap, zero-cost after Python 3.11 even, and
    # because we expect valid values to be much more common than invalid values,
    # we do not validate `value` up-front. Instead, we let struct.pack() raise
    # and figure out a reasonable error message after-wards.
    try:
        buffer.write(struct.pack(">I", value))
    except struct.error as exception:
        _reraise_for_bound(UINT32_RANGE, value, exception)


def write_uint64(buffer: IO[bytes], value: int) -> None:
    try:
        buffer.write(struct.pack(">Q", value))
    except struct.error as exception:
        _reraise_for_bound(UINT64_RANGE, value, exception)


T = TypeVar("T", bound=AvroModel)


def write_sized(buffer: IO[bytes], model: AvroModel) -> None:
    with io.BytesIO() as partial_buffer:
        model.serialize(partial_buffer)
        write_uint32(buffer, partial_buffer.tell())
        buffer.write(partial_buffer.getvalue())


def write_metadata(buffer: IO[bytes], metadata: Metadata) -> None:
    buffer.write(V3_MARKER)
    write_sized(buffer, metadata)


def write_record(
    buffer: IO[bytes],
    record: Record,
    running_checksum: RunningChecksum,
) -> None:
    """
    Encode Record, compute its checksum, wrap it in an Envelope which is written to
    `buffer`, preceded by its byte length.
    """

    with io.BytesIO() as record_buffer:
        record.serialize(record_buffer)
        encoded_record = record_buffer.getvalue()

    # Encode size as uint32 and prepend to record.
    with io.BytesIO() as size_buffer:
        write_uint32(size_buffer, len(encoded_record))
        encoded_size = size_buffer.getvalue()

    # Write length-encoded record to buffer. Note: keeping the size and record separate
    # here and in the checksum update calls, instead of concatenating should minimize
    # the amount of copies of the encoded record.
    buffer.write(encoded_size)
    buffer.write(encoded_record)

    # Update running checksum.
    running_checksum.update(encoded_size)
    running_checksum.update(encoded_record)
