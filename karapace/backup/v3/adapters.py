"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .schema_pb2 import Header, Record
from kafka.consumer.fetcher import ConsumerRecord


# FIXME: Don't do this.
def as_backup_record(record: ConsumerRecord) -> Record:
    return Record(
        key=record.key,
        value=record.value,
        headers=[Header(key=header.key, value=header.value) for header in record.headers],
        partition=record.partition,
        offset=record.offset,
        # FIXME: Unclear what relation to record.timestamp_type is and should be ...
        timestamp_ms=record.timestamp,
    )


# FIXME: Scratch this.
# def from_backup_record(record: Record) -> ConsumerRecord:
#     return ConsumerRecord(
#         key=record.key,
#         value=record.value,
#         headers=[
#             (header.key, header.value)
#             for header in record.headers
#         ],
#         partition=record.partition,
#         offset=record.offset,
#         timestamp=record.timestamp_ms,
#         # FIXME: Unclear what to do here.
#         timestamp_type=...,
#     )
