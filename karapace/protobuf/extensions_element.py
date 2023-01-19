"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ExtensionsElement.kt
from dataclasses import dataclass
from karapace.protobuf.kotlin_wrapper import KotlinRange
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, MAX_TAG_VALUE
from typing import List, Union


@dataclass
class ExtensionsElement:
    location: Location
    documentation: str = ""
    values: List[Union[int, KotlinRange]] = None

    def to_schema(self) -> str:
        result = []
        append_documentation(result, self.documentation)
        result.append("extensions ")

        formatted_values = []
        for value in self.values:
            if isinstance(value, int):
                formatted_values.append(str(value))
            elif isinstance(value, KotlinRange):
                max_value = str(value.maximum) if value.maximum < MAX_TAG_VALUE else "max"
                formatted_values.append(f"{value.minimum} to {max_value}")
            else:
                raise ValueError(f"values should be a list of integers or KotlinRange, got {value!r}")

        result.append(", ".join(formatted_values))
        result.append(";\n")
        return "".join(result)
