"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Workaround to encode/decode indexes in protobuf messages
# Based on https://developers.google.com/protocol-buffers/docs/encoding#varints

from __future__ import annotations
from io import BytesIO
from karapace.protobuf.exception import IllegalArgumentException
from typing import List, Final, Sequence

ZERO_BYTE: Final = b"\x00"


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
        size = read_varint(bio)
    except EOFError:
        # TODO: change exception
        raise IllegalArgumentException("problem with reading binary data") from None
    return [read_varint(bio) for _ in range(size)]


def write_varint(bio: BytesIO, value: int) -> None:
    if value < 0:
        raise ValueError(f"value must not be negative, got {value}")

    if value == 0:
        bio.write(ZERO_BYTE)
        return

    while value > 0:
        to_write = value & 0x7F
        value = value >> 7

        if value > 0:
            to_write |= 0x80

        bio.write(to_write.to_bytes(1, "little"))


def write_indexes(bio: BytesIO, indexes: Sequence[int]) -> None:
    write_varint(bio, len(indexes))
    for i in indexes:
        write_varint(bio, i)
