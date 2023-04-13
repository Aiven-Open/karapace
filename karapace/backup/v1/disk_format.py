"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from karapace.utils import json_decode
from typing import Generator, IO, List, Tuple


def items_from_file(fp: IO) -> Generator[tuple[str, str], None, None]:
    raw_msg = fp.read()
    # json_decode cannot really produce tuples. Typing was added in hindsight here,
    # and it looks like _handle_restore_message has been lying about the type of
    # item for some time already.
    values = json_decode(raw_msg, List[Tuple[str, str]])
    if not values:
        return
    yield from values
