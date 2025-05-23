"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ExtendElement.kt
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from karapace.core.protobuf.field_element import FieldElement
from karapace.core.protobuf.location import Location
from karapace.core.protobuf.utils import append_documentation, append_indented


@dataclass
class ExtendElement:
    location: Location
    name: str
    documentation: str = ""
    fields: Sequence[FieldElement] | None = None

    def to_schema(self) -> str:
        result: list[str] = []
        append_documentation(result, self.documentation)
        result.append(f"extend {self.name} {{")
        if self.fields:
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())

        result.append("}\n")
        return "".join(result)
