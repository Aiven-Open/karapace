"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from hypothesis import given
from hypothesis.strategies import integers
from karapace.core.backup.backends.v3.errors import (
    IntegerAboveBound,
    IntegerBelowBound,
    InvalidChecksum,
    InvalidHeader,
    TooFewRecords,
    TooManyRecords,
)
from karapace.core.backup.backends.v3.readers import read_metadata, read_record, read_records, read_uint32, read_uint64
from karapace.core.backup.backends.v3.schema import DataFile, Header, Metadata, Record
from karapace.core.backup.backends.v3.writers import (
    UINT32_RANGE,
    UINT64_RANGE,
    write_metadata,
    write_record,
    write_uint32,
    write_uint64,
)
from tests.unit.backup.backends.v3.conftest import setup_buffer
from typing import IO
from xxhash import xxh64

import datetime
import io
import pytest
import time
import uuid


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


class TestWriteUint32:
    @pytest.mark.parametrize(
        ("value", "expected_bytes"),
        (
            (0, b"\x00\x00\x00\x00"),
            (2**32 - 1, b"\xff\xff\xff\xff"),
            (67, b"\x00\x00\x00C"),
        ),
    )
    def test_can_write_valid_value(
        self,
        buffer: IO[bytes],
        value: int,
        expected_bytes: bytes,
    ) -> None:
        write_uint32(buffer, value)
        buffer.seek(0)
        assert buffer.read(4) == expected_bytes

    def test_raises_integer_out_bound_for_too_small_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerBelowBound,
            match=r"^Value is too small for valid range\(0, 4294967296\)$",
        ):
            write_uint32(buffer, -1)

    def test_raises_integer_out_of_bound_for_too_big_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerAboveBound,
            match=r"^Value is too large for valid range\(0, 4294967296\)$",
        ):
            write_uint32(buffer, 2**32)


@given(
    integers(min_value=UINT32_RANGE.start, max_value=UINT32_RANGE.stop - 1),
)
def test_uint32_roundtrip(value: int) -> None:
    with setup_buffer() as buffer:
        write_uint32(buffer, value)
        buffer.seek(0)
        assert read_uint32(buffer) == value


class TestWriteUint64:
    @pytest.mark.parametrize(
        ("value", "expected_bytes"),
        (
            (0, b"\x00\x00\x00\x00\x00\x00\x00\x00"),
            (2**64 - 1, b"\xff\xff\xff\xff\xff\xff\xff\xff"),
            (67, b"\x00\x00\x00\x00\x00\x00\x00C"),
        ),
    )
    def test_write_uint64(
        self,
        buffer: IO[bytes],
        value: int,
        expected_bytes: bytes,
    ) -> None:
        write_uint64(buffer, value)
        buffer.seek(0)
        assert buffer.read(8) == expected_bytes

    def test_raises_integer_out_bound_for_too_small_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerBelowBound,
            match=r"^Value is too small for valid range\(0, 18446744073709551616\)$",
        ):
            write_uint64(buffer, -1)

    def test_raises_integer_out_of_bound_for_too_big_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerAboveBound,
            match=r"^Value is too large for valid range\(0, 18446744073709551616\)$",
        ):
            write_uint64(buffer, 2**64)


@given(
    integers(min_value=UINT64_RANGE.start, max_value=UINT64_RANGE.stop - 1),
)
def test_uint64_roundtrip(value: int) -> None:
    with setup_buffer() as buffer:
        write_uint64(buffer, value)
        buffer.seek(0)
        assert read_uint64(buffer) == value


class TestMetadata:
    def test_metadata_roundtrip(self, buffer: IO[bytes]) -> None:
        instance = Metadata(
            version=3,
            tool_name="Karapace",
            tool_version="1.2.3",
            started_at=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0),
            finished_at=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0),
            record_count=123,
            topic_name="some-topic",
            topic_id=uuid.uuid4(),
            partition_count=1,
            replication_factor=2,
            topic_configurations={"cleanup.policy": "compact", "min.insync.replicas": "2"},
            data_files=(
                DataFile(
                    filename="some-topic:0.data",
                    partition=0,
                    checksum=b"abc123",
                    record_count=123,
                    start_offset=127,
                    end_offset=250,
                ),
            ),
        )
        write_metadata(buffer, instance)
        buffer.seek(0)
        assert read_metadata(buffer) == instance

    def test_raises_invalid_header_for_unknown_marker(self, buffer: IO[bytes]) -> None:
        buffer.write(b"abcd")
        buffer.seek(0)
        with pytest.raises(
            InvalidHeader,
            match=r"^Expected to read V3 header in metadata file, found: b'abcd'$",
        ):
            read_metadata(buffer)


class TestRecordSerialisation:
    def test_seeded_roundtrip(self, buffer: IO[bytes]) -> None:
        checksum_seed = b"foo bar baz"
        writer_checksum = xxh64(checksum_seed)
        reader_checksum = xxh64(checksum_seed)
        instance = Record(
            key=b"some-key",
            value=b"some-value",
            headers=(
                Header(
                    key=b"some-header",
                    value=b"some-header-value",
                ),
                Header(
                    key=b"other-header",
                    value=b"other-header-value",
                ),
            ),
            offset=123,
            timestamp=round(time.time()),
            checksum_checkpoint=writer_checksum.digest(),
        )
        write_record(buffer, instance, writer_checksum)
        buffer.seek(0)
        assert read_record(buffer, reader_checksum) == instance
        assert writer_checksum.intdigest() == writer_checksum.intdigest()

    def test_unseeded_roundtrip(self, buffer: IO[bytes]) -> None:
        writer_checksum = xxh64()
        reader_checksum = xxh64()
        instance = Record(
            key=b"some-key",
            value=b"some-value",
            headers=(
                Header(
                    key=b"some-header",
                    value=b"some-header-value",
                ),
                Header(
                    key=b"other-header",
                    value=b"other-header-value",
                ),
            ),
            offset=123,
            timestamp=round(time.time()),
            checksum_checkpoint=None,
        )
        write_record(buffer, instance, writer_checksum)
        buffer.seek(0)
        assert read_record(buffer, reader_checksum) == instance
        assert writer_checksum.intdigest() == writer_checksum.intdigest()

    def test_reader_raises_invalid_checksum_for_mismatch(self, buffer: IO[bytes]) -> None:
        writer_checksum = xxh64(b"writer seed")
        reader_checksum = xxh64(b"reader seed")
        instance = Record(
            key=b"some-key",
            value=b"some-value",
            headers=(),
            offset=123,
            timestamp=round(time.time()),
            checksum_checkpoint=writer_checksum.digest(),
        )
        write_record(buffer, instance, writer_checksum)
        buffer.seek(0)

        with pytest.raises(InvalidChecksum):
            read_record(buffer, reader_checksum)

    def test_read_records(self, buffer: IO[bytes]) -> None:
        writer_checksum = xxh64()
        reader_checksum = xxh64()

        instance_a = Record(
            key=b"some-key",
            value=b"some-value",
            headers=(
                Header(
                    key=b"some-header",
                    value=b"some-header-value",
                ),
                Header(
                    key=b"other-header",
                    value=b"other-header-value",
                ),
            ),
            offset=123,
            timestamp=round(time.time()),
            checksum_checkpoint=None,
        )
        write_record(buffer, instance_a, writer_checksum)

        instance_b = Record(
            key=b"some-key",
            value=b"some-value",
            headers=(
                Header(
                    key=b"some-header",
                    value=b"some-header-value",
                ),
                Header(
                    key=b"other-header",
                    value=b"other-header-value",
                ),
            ),
            offset=567,
            timestamp=round(time.time()),
            checksum_checkpoint=writer_checksum.digest(),
        )
        write_record(buffer, instance_b, writer_checksum)

        buffer.seek(0)
        result_a, result_b = read_records(
            buffer=buffer,
            num_records=2,
            running_checksum=reader_checksum,
        )

        assert reader_checksum.intdigest() == writer_checksum.intdigest()
        assert result_a == instance_a
        assert result_b == instance_b


def test_read_records_raises_too_few_records(buffer: io.BytesIO) -> None:
    record = Record(
        key=b"some-key",
        value=b"some-value",
        headers=(),
        offset=123,
        timestamp=round(time.time()),
        checksum_checkpoint=None,
    )
    write_record(buffer, record, xxh64())

    buffer.seek(0)
    with pytest.raises(TooFewRecords):
        tuple(
            read_records(
                buffer=buffer,
                num_records=2,
                running_checksum=xxh64(),
            )
        )


def test_read_records_raises_too_many_records(buffer: io.BytesIO) -> None:
    checksum = xxh64()

    record = Record(
        key=b"some-key",
        value=b"some-value",
        headers=(),
        offset=123,
        timestamp=round(time.time()),
        checksum_checkpoint=None,
    )
    write_record(buffer, record, checksum)
    record = Record(
        key=b"some-key",
        value=b"some-value",
        headers=(),
        offset=124,
        timestamp=round(time.time()),
        checksum_checkpoint=None,
    )
    write_record(buffer, record, checksum)

    buffer.write(b"0")

    buffer.seek(0)
    with pytest.raises(TooManyRecords):
        tuple(
            read_records(
                buffer=buffer,
                num_records=2,
                running_checksum=xxh64(),
            )
        )
