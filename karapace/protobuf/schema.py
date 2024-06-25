"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.dataclasses import default_dataclass

# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/Schema.kt
# Ported partially for required functionality.
from karapace.dependency import Dependency, DependencyVerifierResult
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.exception import IllegalArgumentException
from karapace.protobuf.known_dependency import DependenciesHardcoded, KnownDependency
from karapace.protobuf.location import DEFAULT_LOCATION
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.proto_parser import ProtoParser
from karapace.protobuf.serialization import deserialize, serialize
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.type_tree import SourceFileReference, TypeTree
from karapace.protobuf.utils import append_documentation, append_indented
from karapace.schema_references import Reference
from typing import Mapping, Sequence

import binascii


def add_slashes(text: str) -> str:
    escape_dict = {
        "\a": "\\a",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
        "\v": "\\v",
        "'": "\\'",
        '"': '\\"',
        "\\": "\\\\",
    }
    trans_table = str.maketrans(escape_dict)
    return text.translate(trans_table)


def message_element_string(element: MessageElement) -> str:
    result = []
    append_documentation(result, element.documentation)
    result.append(f"message {element.name} {{")
    if element.reserveds:
        result.append("\n")
        for reserved in element.reserveds:
            append_indented(result, reserved.to_schema())

    if element.options:
        result.append("\n")
        for option in element.options:
            append_indented(result, option_element_string(option))

    if element.fields:
        result.append("\n")
        for field in element.fields:
            append_indented(result, field.to_schema())

    if element.one_ofs:
        result.append("\n")
        for one_of in element.one_ofs:
            append_indented(result, one_of.to_schema())

    if element.groups:
        result.append("\n")
        for group in element.groups:
            append_indented(result, group.to_schema())

    if element.extensions:
        result.append("\n")
        for extension in element.extensions:
            append_indented(result, extension.to_schema())

    if element.nested_types:
        result.append("\n")
        for nested_type in element.nested_types:
            if isinstance(nested_type, MessageElement):
                append_indented(result, message_element_string(nested_type))

        for nested_type in element.nested_types:
            if isinstance(nested_type, EnumElement):
                append_indented(result, enum_element_string(nested_type))

    result.append("}\n")
    return "".join(result)


def enum_element_string(element: EnumElement) -> str:
    return element.to_schema()


def option_element_string(option: OptionElement) -> str:
    result: str
    if option.kind == OptionElement.Kind.STRING:
        name: str
        if option.is_parenthesized:
            name = f"({option.name})"
        else:
            name = option.name
        value = add_slashes(str(option.value))
        result = f'{name} = "{value}"'
    else:
        result = option.to_schema()

    return f"option {result};\n"


@default_dataclass
class UsedType:
    parent_object_qualified_name: str
    used_attribute_type: str


def _add_new_type_recursive(
    parent_tree: TypeTree,
    remaining_tokens: list[str],
    file: str,
    inserted_elements: int,
    type_provider: TypeElement,
) -> None:
    """
    Add the new types to the TypeTree recursively,
    nb: this isn't a pure function, the remaining_tokens object it's mutated, call it with carefully.
    """
    if remaining_tokens:
        token = remaining_tokens.pop()
        for child in parent_tree.children:
            if child.token == token:
                return _add_new_type_recursive(
                    child, remaining_tokens, file, inserted_elements, type_provider
                )  # add a reference from which object/file was coming from

        new_leaf = TypeTree(
            token=token,
            children=[],
            source_reference=(
                None if remaining_tokens else SourceFileReference(reference=file, import_order=inserted_elements)
            ),
            type_provider=None if remaining_tokens else type_provider,
        )
        parent_tree.children.append(new_leaf)
        return _add_new_type_recursive(new_leaf, remaining_tokens, file, inserted_elements, type_provider)
    return None


def add_new_type(
    root_tree: TypeTree,
    full_path_type: str,
    file: str,
    inserted_elements: int,
    type_provider: TypeElement,
) -> None:
    _add_new_type_recursive(
        root_tree, full_path_type.split("."), file, inserted_elements, type_provider
    )  # one more it's added after that instruction


class ProtobufSchema:
    def __init__(
        self,
        schema: str,
        references: Sequence[Reference] | None = None,
        dependencies: Mapping[str, Dependency] | None = None,
        proto_file_element: ProtoFileElement | None = None,
    ) -> None:
        if type(schema).__name__ != "str":
            raise IllegalArgumentException("Non str type of schema string")
        self.cache_string = ""

        if proto_file_element is not None:
            self.proto_file_element = proto_file_element
        else:
            try:
                self.proto_file_element = deserialize(schema)
            except binascii.Error:  # If not base64 formatted
                self.proto_file_element = ProtoParser.parse(DEFAULT_LOCATION, schema)

        self.references = references
        self.dependencies = dependencies

    def type_in_tree(self, tree: TypeTree, remaining_tokens: list[str]) -> TypeTree | None:
        if remaining_tokens:
            to_seek = remaining_tokens.pop()

            for child in tree.children:
                if child.token == to_seek:
                    return self.type_in_tree(child, remaining_tokens)
            return None
        return tree

    def record_name(self) -> str | None:
        if len(self.proto_file_element.types) == 0:
            return None

        package_name = (
            self.proto_file_element.package_name + "." if self.proto_file_element.package_name not in [None, ""] else ""
        )

        first_element = None
        first_enum = None

        for inspected_type in self.proto_file_element.types:
            if isinstance(inspected_type, MessageElement):
                first_element = inspected_type
                break

            if first_enum is None and isinstance(inspected_type, EnumElement):
                first_enum = inspected_type

        naming_element = first_element if first_element is not None else first_enum

        return package_name + naming_element.name

    def type_exist_in_tree(self, tree: TypeTree, remaining_tokens: list[str]) -> bool:
        return self.type_in_tree(tree, remaining_tokens) is not None

    def recursive_imports(self) -> set[str]:
        imports = set(self.proto_file_element.imports)

        if self.dependencies:
            for key in self.dependencies:
                imports = imports | self.dependencies[key].get_schema().schema.recursive_imports()

        return imports

    def are_type_usage_valid(self, root_type_tree: TypeTree, used_types: list[UsedType]) -> tuple[bool, str | None]:
        # Please note that this check only ensures the requested type exists. However, for performance reasons, it works in
        # the opposite way of how specificity works in Protobuf. In Protobuf, the type is matched not only to check if it
        # exists, but also based on the order of search: local definition comes before imported types. In this code, we
        # first check imported types and then the actual used type. If we combine the second and third checks and add a
        # monotonic number that inserts a reference of where and when the type was declared and imported, we can uniquely
        # and precisely identify the involved type with a lookup. Unfortunately, with the current structure, this is not
        # possible. This would require an additional refactor of the second condition going to reconstruct also here the
        # type tree and applying it into the existing one (that with the monotonic counter would be enough to make sure the
        # algorithm can match and keep track of the references based on the import order)
        for used_type in used_types:
            parent_type_fully_qualified_name = used_type.parent_object_qualified_name[1:]
            attribute_type = used_type.used_attribute_type

            is_parent_object_declared = self.type_exist_in_tree(
                root_type_tree,
                list(
                    parent_type_fully_qualified_name.split("."),
                ),
            )
            is_attribute_type_primitive = attribute_type in DependenciesHardcoded.index

            # NB: this is a direct translation the previous parsing code, in theory we can construct the
            # same tree starting from the imports and just append the new declarations the tree, we just have
            # to add the values associated to the map after seeking for all the values associated with the imports.
            # e.g. if the user import `google/protobuf/struct.proto`
            # we lookup in the KnownDependencies and we can construct the same postfix token trie by adding:
            # "google/protobuf/struct.proto": [
            #    "google.protobuf.Struct",
            #    "google.protobuf.Value",
            #    "google.protobuf.NullValue",
            #    "google.protobuf.ListValue",
            # ]
            # left as todo.

            if is_attribute_type_primitive:
                is_attribute_type_included_in_server = True
            elif attribute_type in KnownDependency.index_simple or attribute_type in KnownDependency.index:
                required_import = KnownDependency.index_simple.get(attribute_type, None)
                if required_import is None:
                    required_import = KnownDependency.index[attribute_type]

                is_attribute_type_included_in_server = required_import in self.recursive_imports()
            else:
                is_attribute_type_included_in_server = False

            is_attribute_primitive_or_declared_before = is_attribute_type_included_in_server or self.type_exist_in_tree(
                root_type_tree,
                list(attribute_type.lstrip(".").split(".")),
            )

            if not is_parent_object_declared or not is_attribute_primitive_or_declared_before:
                return False, attribute_type if is_parent_object_declared else used_type.parent_object_qualified_name
        return True, None

    def verify_schema_dependencies(self) -> DependencyVerifierResult:
        declared_types_root_tree = self.types_tree()
        used_types_list = self.used_types()
        are_declarations_valid, maybe_wrong_declaration = self.are_type_usage_valid(
            declared_types_root_tree, used_types_list
        )
        if are_declarations_valid:
            return DependencyVerifierResult(True)
        return DependencyVerifierResult(False, f'type "{maybe_wrong_declaration}" is not defined')

    def nested_type_tree(
        self,
        root_tree: TypeTree,
        parent_name: str,
        nested_type: TypeElement,
        filename: str,
        inserted_types: int,
    ) -> int:
        nested_component_full_path_name = parent_name + "." + nested_type.name.removeprefix(parent_name + ".")
        add_new_type(root_tree, nested_component_full_path_name, filename, inserted_types, nested_type)
        inserted_types += 1
        for child in nested_type.nested_types:
            self.nested_type_tree(root_tree, nested_component_full_path_name, child, filename, inserted_types)
            inserted_types += 1

        return inserted_types

    def types_tree_recursive(
        self,
        root_tree: TypeTree,
        inserted_types: int,
        filename: str,
    ) -> int:
        # verify that the import it's the same as the order of importing
        if self.dependencies:
            for dependency in self.dependencies:
                inserted_types = (
                    self.dependencies[dependency]
                    .get_schema()
                    .schema.types_tree_recursive(root_tree, inserted_types, dependency)
                )

        # we can add an incremental number and a reference to the file
        # to get back which is the file who a certain declaration it's referring to
        # basically during a lookup you traverse all the leaf after the last point
        # in common with the declaration and you take the node with the minimum numer,
        # the reference in that node it's the file used in the body. Not done yet since
        # now isn't required.

        package_name = self.proto_file_element.package_name or ""
        for element_type in self.proto_file_element.types:
            type_name = element_type.name.removeprefix(self.proto_file_element.package_name + ".")
            full_name = package_name + "." + type_name
            add_new_type(root_tree, full_name, filename, inserted_types, element_type)
            inserted_types += 1

            for nested_type in element_type.nested_types:
                inserted_types = self.nested_type_tree(root_tree, full_name, nested_type, filename, inserted_types)

        return inserted_types

    def types_tree(self) -> TypeTree:
        root_tree = TypeTree(
            token=".",
            children=[],
            source_reference=None,
        )
        self.types_tree_recursive(root_tree, 0, "main_schema_file")
        return root_tree

    @staticmethod
    def used_type(parent: str, element_type: str) -> list[UsedType]:
        if element_type.find("map<") == 0:
            end = element_type.find(">")
            virgule = element_type.find(",")
            key_element_type = element_type[4:virgule]
            value_element_type = element_type[virgule + 1 : end]
            value_element_type = value_element_type.strip()
            return [
                UsedType(parent_object_qualified_name=parent, used_attribute_type=key_element_type),
                UsedType(parent_object_qualified_name=parent, used_attribute_type=value_element_type),
            ]
        return [UsedType(parent_object_qualified_name=parent, used_attribute_type=element_type)]

    @staticmethod
    def dependencies_one_of(
        package_name: str,
        parent_name: str,
        one_of: OneOfElement,
    ) -> list[UsedType]:
        parent = package_name + "." + parent_name
        dependencies = []
        for field in one_of.fields:
            dependencies.append(
                UsedType(
                    parent_object_qualified_name=parent,
                    used_attribute_type=field.element_type,
                )
            )
        return dependencies

    def used_types(self) -> list[UsedType]:
        dependencies_used_types = []
        if self.dependencies:
            for key in self.dependencies:
                dependencies_used_types += self.dependencies[key].get_schema().schema.used_types()

        used_types = []

        package_name = self.proto_file_element.package_name
        if package_name is None:
            package_name = ""
        else:
            package_name = "." + package_name
        for element_type in self.proto_file_element.types:
            type_name = element_type.name.removeprefix(self.proto_file_element.package_name + ".")
            full_name = package_name + "." + type_name
            if isinstance(element_type, MessageElement):
                for one_of in element_type.one_ofs:
                    used_types += self.dependencies_one_of(package_name, type_name, one_of)
                for field in element_type.fields:
                    used_types += self.used_type(full_name, field.element_type)
            for nested_type in element_type.nested_types:
                used_types += self.nested_used_type(package_name, type_name, nested_type)

        return used_types

    def nested_used_type(
        self,
        package_name: str,
        parent_name: str,
        element_type: TypeElement,
    ) -> list[str]:
        used_types = []

        if isinstance(element_type, MessageElement):
            for one_of in element_type.one_ofs:
                # The parent name for nested one of fields is the parent and element type name.
                # The field type name is handled in process_one_of function.
                one_of_parent_name = parent_name + "." + element_type.name
                used_types += self.dependencies_one_of(package_name, one_of_parent_name, one_of)
            for field in element_type.fields:
                used_types += self.used_type(
                    package_name + "." + parent_name, field.element_type.removeprefix(element_type.name + ".")
                )
        for nested_type in element_type.nested_types:
            used_types += self.nested_used_type(package_name, parent_name + "." + element_type.name, nested_type)

        return used_types

    def __str__(self) -> str:
        if not self.cache_string:
            self.cache_string = self.to_schema()
        return self.cache_string

    # str() does normalization of whitespaces and element ordering
    def __eq__(self, other) -> bool:
        return str(self) == str(other)

    def to_schema(self) -> str:
        strings = []
        shm: ProtoFileElement = self.proto_file_element
        if shm.syntax:
            strings.append('syntax = "')
            strings.append(str(shm.syntax))
            strings.append('";\n')

        if shm.package_name:
            strings.append("package " + str(shm.package_name) + ";\n")

        if shm.imports or shm.public_imports:
            strings.append("\n")

            for file in shm.imports:
                strings.append('import "' + str(file) + '";\n')

            for file in shm.public_imports:
                strings.append('import public "' + str(file) + '";\n')

        if shm.options:
            strings.append("\n")
            for option in shm.options:
                strings.append(option_element_string(option))

        if shm.types:
            strings.append("\n")
            for type_element in shm.types:
                if isinstance(type_element, MessageElement):
                    strings.append(message_element_string(type_element))
            for type_element in shm.types:
                if isinstance(type_element, EnumElement):
                    strings.append(enum_element_string(type_element))

        if shm.extend_declarations:
            strings.append("\n")
            for extend_declaration in shm.extend_declarations:
                strings.append(str(extend_declaration.to_schema()))

        if shm.services:
            strings.append("\n")
            for service in shm.services:
                strings.append(str(service.to_schema()))

        return "".join(strings)

    def compare(self, other: ProtobufSchema, result: CompareResult) -> CompareResult:
        return self.proto_file_element.compare(
            other.proto_file_element,
            result,
            self_dependencies=self.dependencies,
            other_dependencies=other.dependencies,
        )

    def serialize(self) -> str:
        return serialize(self.proto_file_element)
