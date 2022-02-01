# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/EnumConstantElement.kt
from dataclasses import dataclass, field
from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.utils import append_documentation, append_options
from typing import List


@dataclass
class EnumConstantElement:
    location: Location
    name: str
    tag: int
    documentation: str = ""
    options: List[OptionElement] = field(default_factory=list)

    def to_schema(self) -> str:
        result = []
        append_documentation(result, self.documentation)
        result.append(f"{self.name} = {self.tag}")
        if self.options:
            result.append(" ")
            append_options(result, self.options)
        result.append(";\n")
        return "".join(result)
