"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ExtendElement.kt
from __future__ import annotations

from dataclasses import dataclass
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.location import Location
from karapace.protobuf.type_tree import TypeTree
from karapace.protobuf.utils import append_documentation, append_indented
from typing import Sequence


@dataclass
class ExtendElement:
    location: Location
    name: str
    documentation: str = ""
    fields: Sequence[FieldElement] | None = None

    def with_full_path_expanded(self, type_tree: TypeTree) -> ExtendElement:
        # read again carefully there -> https://protobuf.com/docs/language-spec#fully-qualified-references
        full_path_fields = [field.with_full_path_expanded(type_tree) for field in self.fields] if self.fields else None
        return ExtendElement(
            location=self.location,
            name=self.name,
            documentation=self.documentation,
            fields=full_path_fields,
        )

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
