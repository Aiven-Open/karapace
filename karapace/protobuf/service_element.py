# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ServiceElement.kt

from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


class ServiceElement:
    def __init__(self, location: Location, name: str, documentation: str = "", rpcs: list = None, options: list = None):
        self.location = location
        self.name = name
        self.documentation = documentation
        self.rpcs = rpcs or []
        self.options = options or []

    def to_schema(self) -> str:
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
        return "".join(result)