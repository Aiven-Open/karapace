# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/FieldElement.kt
from karapace.protobuf.compare_restult import CompareResult, CompareTypes, Modification
from karapace.protobuf.exception import IllegalArgumentException
from karapace.protobuf.field import Field
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_type import ProtoType
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.utils import append_documentation, append_options


class FieldElement:
    def __init__(
            self,
            location: Location,
            label: Field.Label = None,
            element_type: str = "",
            name: str = None,
            default_value: str = None,
            json_name: str = None,
            tag: int = None,
            documentation: str = "",
            options: list = None
    ):
        self.location = location
        self.label = label
        self.element_type = element_type
        self.name = name
        self.default_value = default_value
        self.json_name = json_name
        self.tag = tag
        self.documentation = documentation
        self.options = options or []

    def to_schema(self) -> str:
        result: list = list()
        append_documentation(result, self.documentation)

        if self.label:
            result.append(f"{self.label.name.lower()} ")

        result.append(f"{self.element_type} {self.name} = {self.tag}")

        options_with_default = self.options_with_special_values()
        if options_with_default:
            result.append(' ')
            append_options(result, options_with_default)
        result.append(";\n")

        return "".join(result)

    def options_with_special_values(self) -> list:
        """ Both `default` and `json_name` are defined in the schema like options but they are actually
        not options themselves as they're missing from `google.protobuf.FieldOptions`.
        """

        options: list = self.options.copy()

        if self.default_value:
            proto_type: ProtoType = ProtoType.get2(self.element_type)
            options.append(OptionElement("default", proto_type.to_kind(), self.default_value, False))

        if self.json_name:
            options.append(OptionElement("json_name", OptionElement.Kind.STRING, self.json_name, False))

        return options

    # Only non-repeated scalar types and Enums support default values.

    def compare(self, other: 'FieldElement', result: CompareResult, types: CompareTypes):
        # TODO: serge

        if self.name != other.name:
            result.add_modification(Modification.FIELD_NAME_ALTER)
        if self.label != other.label:
            result.add_modification(Modification.FIELD_LABEL_ALTER)

        self.compare_type(ProtoType.get2(self.element_type), ProtoType.get2(other.element_type), result, types)

    def compare_map(self, self_map: ProtoType, other_map: ProtoType, result: CompareResult, types: CompareTypes):
        self.compare_type(self_map.key_type, other_map.key_type, result, types)
        self.compare_type(self_map.value_type, other_map.value_type, result, types)

    def compare_message(self, self_type: ProtoType, other_type: ProtoType, result: CompareResult, types: CompareTypes):
        # TODO ...

        self_type_element: MessageElement = types.get_self_type(self_type.__str__())
        other_type_element: MessageElement = types.get_other_type(other_type.__str__())

        self_type_name = types.self_type_name(self_type)
        other_type_name = types.other_type_name(other_type)

        if self_type_name is None:
            raise IllegalArgumentException(f"Cannot determine message type {self_type}")

        if other_type_name is None:
            raise IllegalArgumentException(f"Cannot determine message type {other_type}")

        if self_type_name != other_type_name:
            result.add_modification(Modification.FIELD_TYPE_ALTER)

        self_type_element.compare(other_type_element, result, types)


    def compare_type(self, self_type: ProtoType, other_type: ProtoType, result: CompareResult, types: CompareTypes):

        if self_type.is_scalar == other_type.is_scalar and \
                self_type.is_map == other_type.is_map:
            if self_type.is_map:
                self.compare_map(self_type, other_type, result, types)
            elif self_type.is_scalar:
                if self_type.compatibility_kind() != other_type.compatibility_kind():
                    result.add_modification(Modification.FIELD_KIND_ALTER)
            else:
                self.compare_message(self_type, other_type, result, types)
        else:
            result.add_modification(Modification.FIELD_KIND_ALTER)
