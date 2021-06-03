# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/OneOfElement.kt

from karapace.protobuf.utils import append_documentation, append_indented


class OneOfElement:
    name: str
    documentation: str = ""
    fields: list = []
    groups: list = []
    options: list = []

    def __init__(self, name: str, documentation: str = "", fields=None, groups=None, options=None):
        self.name = name
        self.documentation = documentation

        if fields:
            self.fields = fields
        if options:
            self.options = options
        if groups:
            self.groups = groups

    def to_schema(self) -> str:
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"oneof {self.name} {{")
        if self.options:
            for option in self.options:
                append_indented(result, option.to_schema_declaration())

        if self.fields:
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())
        if self.groups:
            result.append("\n")
            for group in self.groups:
                append_indented(result, group.to_schema())
        result.append("}\n")
        return "".join(result)
