# Ported from square/wire:
# wire-library/wire-runtime/src/commonMain/kotlin/com/squareup/wire/Syntax.kt

from enum import Enum
from karapace.protobuf.exception import IllegalArgumentException


class Syntax(Enum):
    PROTO_2 = "proto2"
    PROTO_3 = "proto3"

    @classmethod
    def _missing_(cls, string):
        raise IllegalArgumentException(f"unexpected syntax: {string}")

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
