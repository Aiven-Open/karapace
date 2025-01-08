"""
karapace

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeVar
from typing_extensions import dataclass_transform

T = TypeVar("T")


@dataclass_transform(
    frozen_default=True,
    kw_only_default=True,
)
def default_dataclass(cls: type[T]) -> type[T]:
    return dataclass(
        frozen=True,
        slots=True,
        kw_only=True,
    )(cls)
