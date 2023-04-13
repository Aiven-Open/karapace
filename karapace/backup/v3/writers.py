"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .schema_pb2 import Envelope, Metadata, Record
from collections.abc import Iterable
from typing import IO
from xxhash import xxh64

import struct


def write_uint32(buffer: IO[bytes], value: int) -> None:
    buffer.write(struct.pack(">I", value))


def write_uint64(buffer: IO[bytes], value: int) -> None:
    buffer.write(struct.pack(">Q", value))


def write_metadata(buffer: IO[bytes], metadata: Metadata) -> None:
    encoded_metadata = metadata.SerializeToString()
    write_uint32(buffer, len(encoded_metadata))
    buffer.write(encoded_metadata)


def write_record(buffer: IO[bytes], record: Record) -> None:
    """
    Encode Record, compute its checksum, wrap it in an Envelope which is written
    to `buffer`, preceded by its byte length.
    """
    # Encode the record, compute its checksum, and wrap in an Envelope.
    encoded_record = record.SerializeToString()
    envelope = Envelope(
        record=encoded_record,
        checksum=xxh64(encoded_record).intdigest(),
    )

    # Write the encoded envelope to the buffer, preceded by its byte size.
    encoded_envelope = envelope.SerializeToString()
    write_uint32(buffer, len(encoded_envelope))
    buffer.write(encoded_envelope)


def write_backup(
    buffer: IO[bytes],
    metadata: Metadata,
    records: Iterable[Record],
) -> None:
    write_metadata(buffer, metadata)
    for record in records:
        write_record(buffer, record)
