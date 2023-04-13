"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from typing import Protocol


class RunningChecksum(Protocol):
    def update(self, data: bytes) -> None:
        ...

    def digest(self) -> bytes:
        ...
