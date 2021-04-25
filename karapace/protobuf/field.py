# TODO: ...
from enum import Enum


class Field:
    class Label(Enum):
        OPTIONAL = 1
        REQUIRED = 2
        REPEATED = 3
        ONE_OF = 4
