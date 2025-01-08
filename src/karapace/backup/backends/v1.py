"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Generator
from karapace.backup.backends.reader import BaseItemsBackupReader
from karapace.utils import json_decode
from typing import IO


class SchemaBackupV1Reader(BaseItemsBackupReader):
    @staticmethod
    def items_from_file(fp: IO[str]) -> Generator[list[str], None, None]:
        raw_msg = fp.read()
        values = json_decode(raw_msg, list[list[str]])
        if not values:
            return
        yield from values
