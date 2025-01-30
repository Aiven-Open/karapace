"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
from karapace.core.anonymize_schemas import anonymize_avro
from karapace.backup.backends.reader import BaseItemsBackupReader
from karapace.backup.backends.writer import BaseKVBackupWriter, StdOut
from karapace.core.utils import json_decode, json_encode
from pathlib import Path
from typing import Any, ClassVar, Final, IO

import base64
import contextlib

V2_MARKER: Final = b"/V2\n"


def serialize_record(key_bytes: bytes | None, value_bytes: bytes | None) -> str:
    key = base64.b16encode(key_bytes).decode("utf8") if key_bytes is not None else "null"
    value = base64.b16encode(value_bytes).decode("utf8") if value_bytes is not None else "null"
    return f"{key}\t{value}\n"


class _BaseV2Writer(BaseKVBackupWriter):
    @classmethod
    @contextlib.contextmanager
    def safe_writer(
        cls,
        target: Path | StdOut,
        allow_overwrite: bool,
    ) -> Generator[IO[str], None, None]:
        with super().safe_writer(target, allow_overwrite) as buffer:
            buffer.write(V2_MARKER.decode())
            try:
                yield buffer
            finally:
                pass


class SchemaBackupV2Writer(_BaseV2Writer):
    @staticmethod
    def serialize_record(
        key_bytes: bytes | None,
        value_bytes: bytes | None,
    ) -> str:
        return serialize_record(key_bytes, value_bytes)


class AnonymizeAvroWriter(_BaseV2Writer):
    @staticmethod
    def serialize_record(
        key_bytes: bytes | None,
        value_bytes: bytes | None,
    ) -> str:
        if key_bytes is None:
            raise RuntimeError("Cannot Avro-encode message with key_bytes=None")
        if value_bytes is None:
            raise RuntimeError("Cannot Avro-encode message with value_bytes=None")
        # Check that the message has key `schema` and type is Avro schema.
        # The Avro schemas may have `schemaType` key, if not present the schema is Avro.

        key = json_decode(key_bytes, dict[str, str])
        value = json_decode(value_bytes, dict[str, str])

        if value and "schema" in value and value.get("schemaType", "AVRO") == "AVRO":
            original_schema: Any = json_decode(value["schema"])
            anonymized_schema = anonymize_avro.anonymize(original_schema)
            if anonymized_schema:
                value["schema"] = json_encode(anonymized_schema, compact=True, sort_keys=False)
        if value and "subject" in value:
            value["subject"] = anonymize_avro.anonymize_name(value["subject"])
        # The schemas topic contain all changes to schema metadata.
        if key.get("subject", None):
            key["subject"] = anonymize_avro.anonymize_name(key["subject"])
        return serialize_record(
            json_encode(key, compact=True, binary=True),
            json_encode(value, compact=True, binary=True),
        )


class SchemaBackupV2Reader(BaseItemsBackupReader):
    marker_size: ClassVar = 4

    @staticmethod
    def items_from_file(fp: IO[str]) -> Generator[Sequence[str], None, None]:
        for line in fp:
            hex_key, hex_value = (val.strip() for val in line.split("\t"))
            key = base64.b16decode(hex_key).decode("utf8") if hex_key != "null" else hex_key
            value = base64.b16decode(hex_value).decode("utf8") if hex_value != "null" else hex_value
            yield key, value
