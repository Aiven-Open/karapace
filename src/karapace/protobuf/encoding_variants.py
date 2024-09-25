"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Workaround to encode/decode indexes in protobuf messages
# Based on https://developers.google.com/protocol-buffers/docs/encoding#varints

from io import BytesIO
from karapace.protobuf.exception import IllegalArgumentException
from typing import List

ZERO_BYTE = b"\x00"


def read_varint(bio: BytesIO) -> int:
    """Read a variable-length integer."""
    varint = 0
    read_bytes = 0

    while True:
        char = bio.read(1)
        if len(char) == 0:
            if read_bytes == 0:
                return 0
            raise EOFError(f"EOF while reading varint, value is {varint} so far")

        byte = ord(char)
        varint += (byte & 0x7F) << (7 * read_bytes)

        read_bytes += 1

        if not byte & 0x80:
            return varint


def read_indexes(bio: BytesIO) -> List[int]:
    try:
        size: int = read_varint(bio)
    except EOFError:
        # TODO: change exception
        # pylint: disable=raise-missing-from
        raise IllegalArgumentException("problem with reading binary data")
    if size == 0:
        return [0]
    return [read_varint(bio) for _ in range(size)]


def write_varint(bio: BytesIO, value: int) -> int:
    if value < 0:
        raise ValueError(f"value must not be negative, got {value}")

    if value == 0:
        bio.write(ZERO_BYTE)
        return 1

    written_bytes = 0
    while value > 0:
        to_write = value & 0x7F
        value = value >> 7

        if value > 0:
            to_write |= 0x80

        bio.write(bytearray(to_write)[0])
        written_bytes += 1

    return written_bytes


def write_indexes(bio: BytesIO, indexes: List[int]) -> None:
    for i in indexes:
        write_varint(bio, i)
