from __future__ import annotations
import io
from hypothesis import given, example
from hypothesis.strategies import integers, lists

from karapace.protobuf.varint import write_varint, read_varint, read_indexes, \
    write_indexes

varint_values = integers(min_value=0)


@given(varint_values)
@example(0)
@example(1)
def test_can_roundtrip_varint(value: int) -> None:
    with io.BytesIO() as buffer:
        write_varint(buffer, value)
        buffer.seek(0)
        result = read_varint(buffer)
        assert result == value
        # Assert buffer is exhausted.
        assert buffer.read(1) == b""


@given(lists(elements=varint_values))
@example([])
@example([1, 2, 3])
def test_can_roundtrip_indexes(value: list[int]) -> None:
    with io.BytesIO() as buffer:
        write_indexes(buffer, value)
        buffer.seek(0)
        result = read_indexes(buffer)
        assert result == value
        # Assert buffer is exhausted.
        assert buffer.read(1) == b""
