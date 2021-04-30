from karapace.protobuf.utils import append_documentation, append_indented


class OneOfElement:
    name: str
    documentation: str = ""
    fields: list = list()
    groups: list = list()
    options: list = list()

    def __init__(self, name: str, documentation: str, fields: list, groups: list, options: list):
        self.name = name
        self.documentation = documentation
        self.fields = fields
        self.groups = groups
        self.options = options

    def to_schema(self) -> str:
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"oneof {self.name} {{")
        if self.options and len(self.options):
            for option in self.options:
                append_indented(result, option.to_schema_declaration())

        if self.fields and len(self.fields):
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())
        if self.groups and len(self.groups):
            result.append("\n")
            for group in self.groups:
                append_indented(result, group.to_schema())
        result.append("}\n")
        return "".join(result)
