# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/EnumElement.kt

from karapace.protobuf.location import Location
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.utils import append_documentation, append_indented


class EnumElement(TypeElement):
    def __init__(self, location: Location, name: str, documentation: str = "", options: list = None, constants: list = None):
        # Enums do not allow nested type declarations.
        super().__init__(location, name, documentation, options or [], [])
        self.constants = constants or []

    def to_schema(self) -> str:
        result: list = list()
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
