"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ServiceElement.kt
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from karapace.core.protobuf.location import Location
from karapace.core.protobuf.option_element import OptionElement
from karapace.core.protobuf.rpc_element import RpcElement
from karapace.core.protobuf.utils import append_documentation, append_indented


@dataclass
class ServiceElement:
    location: Location
    name: str
    documentation: str = ""
    rpcs: Sequence[RpcElement] | None = None
    options: Sequence[OptionElement] | None = None

    def to_schema(self) -> str:
        result: list[str] = []
        append_documentation(result, self.documentation)
        result.append(f"service {self.name} {{")
        if self.options:
            result.append("\n")
            for option in self.options:
                append_indented(result, option.to_schema_declaration())
        if self.rpcs:
            result.append("\n")
            for rpc in self.rpcs:
                append_indented(result, rpc.to_schema())

        result.append("}\n")
        return "".join(result)
