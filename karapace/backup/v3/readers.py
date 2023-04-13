"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .errors import InvalidChecksum
from .schema_pb2 import Envelope, Metadata, Record
from typing import Generator, IO
from xxhash import xxh64

import struct


def read_uint32(buffer: IO[bytes]) -> int:
    return struct.unpack(">I", buffer.read(4))[0]


def read_uint64(buffer: IO[bytes]) -> int:
    return struct.unpack(">Q", buffer.read(8))[0]


def read_metadata(buffer: IO[bytes]) -> Metadata:
    metadata_length = read_uint32(buffer)
    metadata = Metadata()
    metadata.ParseFromString(buffer.read(metadata_length))
    return metadata


def read_record(buffer: IO[bytes]) -> Record:
    """
    Decode a length-value encoded envelope from `buffer`, verify its checksum is
    intact and return the wrapped record decoded.

    :raises DecodeRecordError:
    """
    envelope_size = read_uint32(buffer)

    envelope = Envelope()
    envelope.ParseFromString(buffer.read(envelope_size))

    # Verify record is intact.
    if envelope.checksum != xxh64(envelope.record).intdigest():
        raise InvalidChecksum

    record = Record()
    record.ParseFromString(envelope.record)
    return record


def read_records(buffer: IO[bytes]) -> Generator[Record, None, None]:
    while True:
        # Attempt to peek into buffer, and break out of loop if it's exhausted.
        position = buffer.tell()
        if buffer.read(1) == b"":
            break
        buffer.seek(position)
        yield read_record(buffer)
