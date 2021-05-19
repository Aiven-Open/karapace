from enum import Enum
from karapace.protobuf.exception import IllegalArgumentException


class Syntax(Enum):
    PROTO_2 = "proto2"
    PROTO_3 = "proto3"

    @classmethod
    def _missing_(cls, string):
        raise IllegalArgumentException(f"unexpected syntax: {string}")
