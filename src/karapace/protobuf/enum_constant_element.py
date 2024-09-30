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
from karapace.protobuf.utils import append_documentation, append_options


@dataclass
class EnumConstantElement:
    location: Location
    name: str
    tag: int
    documentation: str = ""
    options: list[OptionElement] = field(default_factory=list)

    def to_schema(self) -> str:
        result: list[str] = []
        append_documentation(result, self.documentation)
        result.append(f"{self.name} = {self.tag}")
        if self.options:
            result.append(" ")
            append_options(result, self.options)
        result.append(";\n")
        return "".join(result)
