"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/EnumElement.kt
from __future__ import annotations

from collections.abc import Sequence
from itertools import chain
from karapace.core.protobuf.compare_result import CompareResult, Modification
from karapace.core.protobuf.compare_type_storage import CompareTypes
from karapace.core.protobuf.enum_constant_element import EnumConstantElement
from karapace.core.protobuf.location import Location
from karapace.core.protobuf.option_element import OptionElement
from karapace.core.protobuf.type_element import TypeElement
from karapace.core.protobuf.utils import append_documentation, append_indented


class EnumElement(TypeElement):
    def __init__(
        self,
        location: Location,
        name: str,
        documentation: str = "",
        options: Sequence[OptionElement] | None = None,
        constants: Sequence[EnumConstantElement] | None = None,
    ) -> None:
        # Enums do not allow nested type declarations.
        super().__init__(location, name, documentation, options or [], [])
        self.constants = constants or []

    def to_schema(self) -> str:
        result: list[str] = []
        append_documentation(result, self.documentation)
        result.append(f"enum {self.name} {{")

        if self.options or self.constants:
            result.append("\n")

        if self.options:
            for option in self.options:
                append_indented(result, option.to_schema_declaration())

        if self.constants:
            for constant in self.constants:
                append_indented(result, constant.to_schema())

        result.append("}\n")
        return "".join(result)

    def compare(self, other: TypeElement, result: CompareResult, types: CompareTypes) -> None:
        self_tags = {}
        other_tags = {}
        if types:
            pass

        if not isinstance(other, EnumElement):
            result.add_modification(Modification.TYPE_ALTER)
            return

        for constant in self.constants:
            self_tags[constant.tag] = constant

        for constant in other.constants:
            other_tags[constant.tag] = constant

        for tag in chain(self_tags.keys(), other_tags.keys() - self_tags.keys()):
            result.push_path(str(tag))
            self_tag = self_tags.get(tag)
            other_tag = other_tags.get(tag)
            if self_tag is None:
                result.add_modification(Modification.ENUM_CONSTANT_ADD)
            elif other_tag is None:
                result.add_modification(Modification.ENUM_CONSTANT_DROP)
            else:
                if self_tag.name != other_tag.name:
                    result.add_modification(Modification.ENUM_CONSTANT_ALTER)
            result.pop_path()
