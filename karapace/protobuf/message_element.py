from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


class MessageElement(TypeElement):
    reserveds: list = []
    fields: list = []
    one_ofs: list = []
    extensions: list = []
    groups: list = []

    def __init__(self,
                 location: Location,
                 name: str,
                 documentation: str,
                 nested_types: list,
                 options: list,
                 reserveds: list,
                 fields: list,
                 one_ofs: list,
                 extensions: list,
                 groups: list,
                 ):
        self.location = location
        self.name = name
        self.documentation = documentation
        self.nested_types = nested_types
        self.options = options
        self.reserveds = reserveds
        self.fields = fields
        self.one_ofs = one_ofs
        self.extensions = extensions
        self.groups = groups

    def to_schema(self) -> str:
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"message {self.name} {{")
        if self.reserveds and len(self.reserveds):
            result.append("\n")
            for reserved in self.reserveds:
                append_indented(result, reserved.to_schema())

        if self.options and len(self.options):
            result.append("\n")
            for option in self.options:
                append_indented(result, option.to_schema_declaration())

        if self.fields and len(self.fields):
            for field in self.fields:
                result.append("\n")
                append_indented(result, field.to_schema())

        if self.one_ofs and len(self.one_ofs):
            for one_of in self.one_ofs:
                result.append("\n")
                append_indented(result, one_of.to_schema())

        if self.groups and len(self.groups):
            for group in self.groups:
                result.append("\n")
                append_indented(result, group.to_schema())

        if self.extensions and len(self.extensions):
            result.append("\n")
            for extension in self.extensions:
                append_indented(result, extension.to_schema())

        if self.nested_types and len(self.nested_types):
            result.append("\n")
            for nested_type in self.nested_types:
                append_indented(result, nested_type.to_schema())

        result.append("}\n")
        return "".join(result)
