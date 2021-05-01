from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


class ServiceElement:
    location: Location
    name: str
    documentation: str
    rpcs: list
    options: list

    def __init__(self, location: Location, name: str, documentation: str, rpcs: list, options: list):
        self.location = location
        self.name = name
        self.documentation = documentation
        self.rpcs = rpcs
        self.options = options

    def to_schema(self):
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"service {self.name} {{")
        if self.options:
            result.append("\n")
        for option in self.options:
            append_indented(result, option.to_schema_declaration())

        if self.rpcs:
            result.append('\n')
            for rpc in self.rpcs:
                append_indented(result, rpc.to_schema())

        result.append("}\n")
        return result
