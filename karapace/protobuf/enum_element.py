# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/EnumElement.kt
from itertools import chain
from karapace.protobuf.compare_result import CompareResult, Modification
from karapace.protobuf.compare_type_storage import CompareTypes
from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.location import Location
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.utils import append_documentation, append_indented


class EnumElement(TypeElement):
    def __init__(
        self, location: Location, name: str, documentation: str = "", options: list = None, constants: list = None
    ) -> None:
        # Enums do not allow nested type declarations.
        super().__init__(location, name, documentation, options or [], [])
        self.constants = constants or []

    def to_schema(self) -> str:
        result: list = []
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

    def compare(self, other: 'EnumElement', result: CompareResult, types: CompareTypes) -> None:
        self_tags: dict = {}
        other_tags: dict = {}
        constant: EnumConstantElement
        if types:
            pass

        for constant in self.constants:
            self_tags[constant.tag] = constant

        for constant in other.constants:
            other_tags[constant.tag] = constant

        for tag in chain(self_tags.keys(), other_tags.keys() - self_tags.keys()):
            result.push_path(str(tag))
            if self_tags.get(tag) is None:
                result.add_modification(Modification.ENUM_CONSTANT_ADD)
            elif other_tags.get(tag) is None:
                result.add_modification(Modification.ENUM_CONSTANT_DROP)
            else:
                if self_tags.get(tag).name == other_tags.get(tag).name:
                    result.add_modification(Modification.ENUM_CONSTANT_ALTER)
            result.pop_path()
