# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/TypeElement.kt
from dataclasses import dataclass
from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement
from typing import List


@dataclass
class TypeElement:
    location: Location
    name: str
    documentation: str
    options: List[OptionElement]
    nested_types: List[object]

    def to_schema(self) -> str:
        pass

    def __repr__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"

    def __str__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"
