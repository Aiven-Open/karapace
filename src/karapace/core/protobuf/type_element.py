"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/TypeElement.kt
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from karapace.core.protobuf.location import Location
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.core.protobuf.compare_result import CompareResult
    from karapace.core.protobuf.compare_type_storage import CompareTypes
    from karapace.core.protobuf.option_element import OptionElement


@dataclass
class TypeElement:
    location: Location
    name: str
    documentation: str
    options: Sequence[OptionElement]
    nested_types: Sequence[TypeElement]

    def to_schema(self) -> str:
        """Convert the object to valid protobuf syntax.

        This must be implemented by subclasses.
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"

    def __str__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"

    def compare(self, other: TypeElement, result: CompareResult, types: CompareTypes) -> None:
        pass
