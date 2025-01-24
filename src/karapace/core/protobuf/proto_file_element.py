"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Sequence

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ProtoFileElement.kt
from karapace.core.dependency import Dependency
from karapace.core.protobuf.compare_result import CompareResult, Modification
from karapace.core.protobuf.compare_type_storage import CompareTypes
from karapace.core.protobuf.extend_element import ExtendElement
from karapace.core.protobuf.location import Location
from karapace.core.protobuf.option_element import OptionElement
from karapace.core.protobuf.service_element import ServiceElement
from karapace.core.protobuf.syntax import Syntax
from karapace.core.protobuf.type_element import TypeElement
from typing import NewType


def _collect_dependencies_types(compare_types: CompareTypes, dependencies: dict[str, Dependency] | None, is_self: bool):
    for dep in dependencies.values():
        types: list[TypeElement] = dep.schema.schema.proto_file_element.types
        sub_deps = dep.schema.schema.dependencies
        package_name = dep.schema.schema.proto_file_element.package_name
        type_: TypeElement
        for type_ in types:
            if is_self:
                compare_types.add_self_type(package_name, type_)
            else:
                compare_types.add_other_type(package_name, type_)
        if sub_deps is None:
            return
        _collect_dependencies_types(compare_types, sub_deps, is_self)


TypeName = NewType("TypeName", str)
PackageName = NewType("PackageName", str)


class ProtoFileElement:
    types: Sequence[TypeElement]
    services: Sequence[ServiceElement]
    extend_declarations: Sequence[ExtendElement]

    def __init__(
        self,
        location: Location,
        package_name: PackageName | None = None,
        syntax: Syntax | None = None,
        imports: Sequence[TypeName] | None = None,
        public_imports: Sequence[TypeName] | None = None,
        types: Sequence[TypeElement] | None = None,
        services: Sequence[ServiceElement] | None = None,
        extend_declarations: Sequence[ExtendElement] | None = None,
        options: Sequence[OptionElement] | None = None,
    ) -> None:
        if types is None:
            types = list()
        self.location = location
        self.package_name = package_name
        self.syntax = syntax
        self.options = options or []
        self.extend_declarations = extend_declarations or []
        self.services = services or []
        self.types = types or []
        self.public_imports = public_imports or []
        self.imports = imports or []

    def to_schema(self) -> str:
        strings: list = [
            "// Proto schema formatted by Wire, do not edit.\n",
            "// Source: ",
            str(self.location.with_path_only()),
            "\n",
        ]
        if self.syntax:
            strings.append("\n")
            strings.append('syntax = "')
            strings.append(str(self.syntax))
            strings.append('";\n')

        if self.package_name:
            strings.append("\n")
            strings.append("package " + str(self.package_name) + ";\n")

        if self.imports or self.public_imports:
            strings.append("\n")

            for file in self.imports:
                strings.append('import "' + str(file) + '";\n')

            for file in self.public_imports:
                strings.append('import public "' + str(file) + '";\n')

        if self.options:
            strings.append("\n")
            for option in self.options:
                strings.append(str(option.to_schema_declaration()))

        if self.types:
            for type_element in self.types:
                strings.append("\n")
                strings.append(str(type_element.to_schema()))

        if self.extend_declarations:
            for extend_declaration in self.extend_declarations:
                strings.append("\n")
                strings.append(str(extend_declaration.to_schema()))

        if self.services:
            for service in self.services:
                strings.append("\n")
                strings.append(str(service.to_schema()))

        return "".join(strings)

    @staticmethod
    def empty(path) -> "ProtoFileElement":
        return ProtoFileElement(Location("", path))

    # TODO: there maybe be faster comparison workaround
    def __eq__(self, other: "ProtoFileElement") -> bool:  # type: ignore
        a = self.to_schema()
        b = other.to_schema()

        return a == b

    def __repr__(self) -> str:
        return self.to_schema()

    def compare(
        self,
        other: "ProtoFileElement",
        result: CompareResult,
        self_dependencies: dict[str, Dependency] | None = None,
        other_dependencies: dict[str, Dependency] | None = None,
    ) -> CompareResult:
        from karapace.core.protobuf.compare_type_lists import compare_type_lists

        if self.package_name != other.package_name:
            result.add_modification(Modification.PACKAGE_ALTER)
        # TODO: do we need syntax check?
        if self.syntax != other.syntax:
            result.add_modification(Modification.SYNTAX_ALTER)

        compare_types = CompareTypes(self.package_name, other.package_name, result)
        if self_dependencies:
            _collect_dependencies_types(compare_types, self_dependencies, True)

        if other_dependencies:
            _collect_dependencies_types(compare_types, other_dependencies, False)
        return compare_type_lists(self.types, other.types, result, compare_types)
