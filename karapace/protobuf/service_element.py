# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ServiceElement.kt
from dataclasses import dataclass

from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.utils import append_documentation, append_indented
from typing import List


@dataclass
class ServiceElement:
    location: Location
    name: str
    documentation: str = ""
    rpcs: List[RpcElement] = None
    options: List[OptionElement] = None

    def to_schema(self) -> str:
        result: List[str] = []
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
