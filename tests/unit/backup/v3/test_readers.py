"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.backup.v3.readers import read_uint32, read_uint64
from typing import IO

import pytest


@pytest.mark.parametrize(
    ("byte_value", "expected_result"),
    (
        (b"\x00\x00\x00\x00", 0),
        (b"\xff\xff\xff\xff", 2**32 - 1),
        (b"\x00\x00\x00C", 67),
    ),
)
def test_read_uint32(
    buffer: IO[bytes],
    byte_value: bytes,
    expected_result: int,
) -> None:
    buffer.write(byte_value)
    buffer.seek(0)
    assert read_uint32(buffer) == expected_result


@pytest.mark.parametrize(
    ("byte_value", "expected_result"),
    (
        (b"\x00\x00\x00\x00\x00\x00\x00\x00", 0),
        (b"\xff\xff\xff\xff\xff\xff\xff\xff", 2**64 - 1),
        (b"\x00\x00\x00\x00\x00\x00\x00C", 67),
    ),
)
def test_read_uint64(
    buffer: IO[bytes],
    byte_value: bytes,
    expected_result: int,
) -> None:
    buffer.write(byte_value)
    buffer.seek(0)
    assert read_uint64(buffer) == expected_result
