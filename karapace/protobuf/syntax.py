# Ported from square/wire:
# wire-library/wire-runtime/src/commonMain/kotlin/com/squareup/wire/Syntax.kt

from enum import Enum


class Syntax(Enum):
    PROTO_2 = "proto2"
    PROTO_3 = "proto3"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value
