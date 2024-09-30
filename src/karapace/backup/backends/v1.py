"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from karapace.backup.backends.reader import BaseItemsBackupReader
from karapace.utils import json_decode
from typing import Generator, IO, List


class SchemaBackupV1Reader(BaseItemsBackupReader):
    @staticmethod
    def items_from_file(fp: IO[str]) -> Generator[list[str], None, None]:
        raw_msg = fp.read()
        values = json_decode(raw_msg, List[List[str]])
        if not values:
            return
        yield from values
