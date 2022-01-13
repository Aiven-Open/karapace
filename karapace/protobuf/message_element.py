# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/MessageElement.kt
# compatibility routine added
from itertools import chain
from karapace.protobuf.compare_result import CompareResult, Modification
from karapace.protobuf.compare_type_storage import CompareTypes
from karapace.protobuf.extensions_element import ExtensionsElement
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.group_element import GroupElement
from karapace.protobuf.location import Location
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.reserved_document import ReservedElement
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.utils import append_documentation, append_indented
from typing import List


class MessageElement(TypeElement):
    def __init__(
        self,
        location: Location,
        name: str,
        documentation: str = "",
        nested_types: List[str] = None,
        options: List[OptionElement] = None,
        reserveds: List[ReservedElement] = None,
        fields: List[FieldElement] = None,
        one_ofs: List[OneOfElement] = None,
        extensions: List[ExtensionsElement] = None,
        groups: List[GroupElement] = None,
    ) -> None:
        super().__init__(location, name, documentation, options or [], nested_types or [])
        self.reserveds = reserveds or []
        self.fields = fields or []
        self.one_ofs = one_ofs or []
        self.extensions = extensions or []
        self.groups = groups or []

    def to_schema(self) -> str:
        result = []
        append_documentation(result, self.documentation)
        result.append(f"message {self.name} {{")
        if self.reserveds:
            result.append("\n")
            for reserved in self.reserveds:
                append_indented(result, reserved.to_schema())

        if self.options:
            result.append("\n")
            for option in self.options:
                append_indented(result, option.to_schema_declaration())

        if self.fields:
            for field in self.fields:
                result.append("\n")
                append_indented(result, field.to_schema())

        if self.one_ofs:
            for one_of in self.one_ofs:
                result.append("\n")
                append_indented(result, one_of.to_schema())

        if self.groups:
            for group in self.groups:
                result.append("\n")
                append_indented(result, group.to_schema())

        if self.extensions:
            result.append("\n")
            for extension in self.extensions:
                append_indented(result, extension.to_schema())

        if self.nested_types:
            result.append("\n")
            for nested_type in self.nested_types:
                append_indented(result, nested_type.to_schema())

        result.append("}\n")
        return "".join(result)

    def compare(self, other: 'MessageElement', result: CompareResult, types: CompareTypes) -> None:

        if types.lock_message(self):
            field: FieldElement
            subfield: FieldElement
            one_of: OneOfElement
            self_tags = {}
            other_tags = {}
            self_one_ofs = {}
            other_one_ofs = {}

            for field in self.fields:
                self_tags[field.tag] = field

            for field in other.fields:
                other_tags[field.tag] = field

            for one_of in self.one_ofs:
                self_one_ofs[one_of.name] = one_of

            for one_of in other.one_ofs:
                other_one_ofs[one_of.name] = one_of

            for field in other.one_ofs:
                result.push_path(str(field.name))
                convert_count = 0
                for subfield in field.fields:
                    tag = subfield.tag
                    if self_tags.get(tag):
                        self_tags.pop(tag)
                        convert_count += 1
                if convert_count > 1:
                    result.add_modification(Modification.FEW_FIELDS_CONVERTED_TO_ONE_OF)
                result.pop_path()

            # Compare fields
            for tag in chain(self_tags.keys(), other_tags.keys() - self_tags.keys()):
                result.push_path(str(tag))

                if self_tags.get(tag) is None:
                    result.add_modification(Modification.FIELD_ADD)
                elif other_tags.get(tag) is None:
                    result.add_modification(Modification.FIELD_DROP)
                else:
                    self_tags[tag].compare(other_tags[tag], result, types)

                result.pop_path()
            # Compare OneOfs
            for name in chain(self_one_ofs.keys(), other_one_ofs.keys() - self_one_ofs.keys()):
                result.push_path(str(name))

                if self_one_ofs.get(name) is None:
                    result.add_modification(Modification.ONE_OF_ADD)
                elif other_one_ofs.get(name) is None:
                    result.add_modification(Modification.ONE_OF_DROP)
                else:
                    self_one_ofs[name].compare(other_one_ofs[name], result, types)

                result.pop_path()

            # TODO Compare NestedTypes must be there.

            types.unlock_message(self)
