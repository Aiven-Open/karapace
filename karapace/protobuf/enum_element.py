from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


class EnumElement(TypeElement):
    constants: list = list()

    def __init__(self,
                 location: Location,
                 name: str,
                 documentation: str,
                 options: list,
                 constants: list
                 ):
        self.location = location
        self.name = name
        self.documentation = documentation
        self.options = options
        self.constants = constants
        # Enums do not allow nested type declarations.
        self.nested_types = list()

    def to_schema(self) -> str:
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"enum {self.name} {{")

        if self.options and len(self.options) or self.constants and len(self.constants):
            result.append("\n")

        if self.options and len(self.options):
            for option in self.options:
                append_indented(result, option.to_schema_declaration())

        if self.constants and len(self.constants):
            for constant in self.constants:
                append_indented(result, constant.to_schema())

        result.append("}\n")
        return "".join(result)
