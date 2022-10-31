# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/TypeElement.kt
from dataclasses import dataclass
from karapace.protobuf.location import Location
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.protobuf.option_element import OptionElement


@dataclass
class TypeElement:
    location: Location
    name: str
    documentation: str
    options: List["OptionElement"]
    nested_types: List["TypeElement"]

    def to_schema(self) -> str:
        """Convert the object to valid protobuf syntax.

        This must be implemented by subclasses.
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"

    def __str__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"
