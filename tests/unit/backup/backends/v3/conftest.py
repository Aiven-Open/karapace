"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Iterator
from contextlib import closing

import contextlib
import io
import pytest


def buffer() -> Iterator[io.BytesIO]:
    with closing(io.BytesIO()) as managed_buffer:
        yield managed_buffer
        # Make sure buffer is exhausted.
        assert managed_buffer.read(1) == b"", "buffer not exhausted"


buffer_fixture = pytest.fixture(
    scope="function",
    name="buffer",
)(buffer)
setup_buffer = contextlib.contextmanager(buffer)
