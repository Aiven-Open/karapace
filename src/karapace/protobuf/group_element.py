"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/GroupElement.kt
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


@dataclass
class GroupElement:
    label: Field.Label | None
    location: Location
    name: str
    tag: int
    documentation: str = ""
    fields: Sequence[FieldElement] | None = None

    def to_schema(self) -> str:
        result: list[str] = []
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
