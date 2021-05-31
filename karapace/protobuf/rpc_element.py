# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/RpcElement.kt

from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


class RpcElement:
    location: Location
    name: str
    documentation: str
    request_type: str
    response_type: str
    request_streaming: bool
    response_streaming: bool
    options: list

    def __init__(
        self,
        location: Location,
        name: str,
        documentation: str,
        request_type: str,
        response_type: str,
        request_streaming: bool,
        response_streaming: bool,
        options: list,
    ):
        self.location = location
        self.name = name
        self.documentation = documentation
        self.request_type = request_type
        self.response_type = response_type
        self.request_streaming = request_streaming
        self.response_streaming = response_streaming
        self.options = options

    def to_schema(self) -> str:
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"rpc {self.name} (")

        if self.request_streaming:
            result.append("stream ")
        result.append(f"{self.request_type}) returns (")

        if self.response_streaming:
            result.append("stream ")
        result.append(f"{self.response_type})")

        if self.options:
            result.append(" {\n")
            for option in self.options:
                append_indented(result, option.to_schema_declaration())
            result.append("}")
        result.append(";\n")
        return "".join(result)
