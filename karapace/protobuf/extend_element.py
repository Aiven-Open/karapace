# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ExtendElement.kt
from dataclasses import dataclass
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented
from typing import List


@dataclass
class ExtendElement:
    location: Location
    name: str
    documentation: str = ""
    fields: List[FieldElement] = None

    def to_schema(self) -> str:
        result = []
        append_documentation(result, self.documentation)
        result.append(f"extend {self.name} {{")
        if self.fields:
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())

        result.append("}\n")
        return "".join(result)
