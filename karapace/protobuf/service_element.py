"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ServiceElement.kt
from __future__ import annotations

from dataclasses import dataclass
from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.type_tree import TypeTree
from karapace.protobuf.utils import append_documentation, append_indented
from typing import Sequence


@dataclass
class ServiceElement:
    location: Location
    name: str
    documentation: str = ""
    rpcs: Sequence[RpcElement] | None = None
    options: Sequence[OptionElement] | None = None

    def with_full_path_expanded(self, type_tree: TypeTree) -> ServiceElement:
        full_path_options = [option.with_full_path_expanded(type_tree) for option in self.options] if self.options else []
        full_path_rpcs = [rpc.with_full_path_expanded(type_tree) for rpc in self.rpcs] if self.rpcs else None
        return ServiceElement(
            location=self.location,
            name=self.name,
            documentation=self.documentation,
            rpcs=full_path_rpcs,
            options=full_path_options,
        )

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
