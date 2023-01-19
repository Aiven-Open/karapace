"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/GroupElement.kt
from dataclasses import dataclass
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented
from typing import List, Optional


@dataclass
class GroupElement:
    label: Optional[Field.Label]
    location: Location
    name: str
    tag: int
    documentation: str = ""
    fields: List[FieldElement] = None

    def to_schema(self) -> str:
        result = []
        append_documentation(result, self.documentation)

        if self.label:
            result.append(f"{str(self.label.name).lower()} ")
        result.append(f"group {self.name} = {self.tag} {{")
        if self.fields:
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())
        result.append("}\n")
        return "".join(result)
