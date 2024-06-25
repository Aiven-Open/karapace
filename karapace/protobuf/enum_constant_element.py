"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/EnumConstantElement.kt

from __future__ import annotations

from dataclasses import dataclass, field
from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.type_tree import TypeTree
from karapace.protobuf.utils import append_documentation, append_options


@dataclass
class EnumConstantElement:
    location: Location
    name: str
    tag: int
    documentation: str = ""
    options: list[OptionElement] = field(default_factory=list)

    def with_full_path_expanded(self, type_tree: TypeTree) -> EnumConstantElement:
        full_path_options = [option.with_full_path_expanded(type_tree) for option in self.options]
        return EnumConstantElement(
            location=self.location,
            name=self.name,
            tag=self.tag,
            documentation=self.documentation,
            options=full_path_options,
        )

    def to_schema(self) -> str:
        result: list[str] = []
        append_documentation(result, self.documentation)
        result.append(f"{self.name} = {self.tag}")
        if self.options:
            result.append(" ")
            append_options(result, self.options)
        result.append(";\n")
        return "".join(result)
