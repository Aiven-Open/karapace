"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/TypeElement.kt
from __future__ import annotations

from dataclasses import dataclass
from karapace.protobuf.location import Location
from karapace.protobuf.type_tree import TypeTree
from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.protobuf.compare_result import CompareResult
    from karapace.protobuf.compare_type_storage import CompareTypes
    from karapace.protobuf.option_element import OptionElement


@dataclass
class TypeElement:
    location: Location
    # https://protobuf.dev/reference/protobuf/proto3-spec/
    # name cannot contain anything else than letters|numbers or `_`
    name: str
    documentation: str
    options: Sequence[OptionElement]
    nested_types: Sequence[TypeElement]

    def with_full_path_expanded(self, type_tree: TypeTree) -> TypeElement:
        pass

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
