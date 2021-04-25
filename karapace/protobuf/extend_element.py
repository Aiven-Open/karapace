from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


class ExtendElement:
    location: Location
    name: str
    documentation: str
    fields: list

    def __init__(self, location: Location, name: str, documentation: str, fields: list):
        self.location = location
        self.name = name
        self.documentation = documentation
        self.fields = fields

    def to_schema(self):
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"extend {self.name} {{")
        if self.fields:
            result.append("\n")
        for field in self.fields:
            append_indented(result, field.to_schema_declaration())

        result.append("}\n")
        return result
