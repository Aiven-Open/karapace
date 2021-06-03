# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/Field.kt

# TODO: We ported only code which is used by schema parser. Full class may be ported if required
from enum import Enum


class Field:
    class Label(Enum):
        OPTIONAL = 1
        REQUIRED = 2
        REPEATED = 3
        ONE_OF = 4
