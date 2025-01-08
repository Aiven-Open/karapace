"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ReservedElement.kt
from __future__ import annotations

from dataclasses import dataclass
from karapace.protobuf.kotlin_wrapper import KotlinRange
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation
from karapace.utils import assert_never


@dataclass
class ReservedElement:
    location: Location
    values: list[str | int | KotlinRange]
    documentation: str = ""

    def to_schema(self) -> str:
        result: list[str] = []
        append_documentation(result, self.documentation)
        result.append("reserved ")

        for index, value in enumerate(self.values):
            if index > 0:
                result.append(", ")

            if isinstance(value, str):
                result.append(f'"{value}"')
            elif isinstance(value, int):
                result.append(f"{value}")
            elif isinstance(value, KotlinRange):
                result.append(f"{value.minimum} to {value.maximum}")
            else:
                assert_never(value)
        result.append(";\n")
        return "".join(result)
