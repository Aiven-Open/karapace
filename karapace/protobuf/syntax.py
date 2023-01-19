"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-runtime/src/commonMain/kotlin/com/squareup/wire/Syntax.kt

from enum import Enum
from karapace.protobuf.exception import IllegalArgumentException


class Syntax(Enum):
    PROTO_2 = "proto2"
    PROTO_3 = "proto3"

    @classmethod
    def _missing_(cls, value) -> None:
        raise IllegalArgumentException(f"unexpected syntax: {value}")

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value
