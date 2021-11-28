# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/FieldElement.kt
from karapace.protobuf.compare_result import CompareResult, Modification
from karapace.protobuf.compare_type_storage import TypeRecordMap
from karapace.protobuf.field import Field
from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_type import ProtoType
from karapace.protobuf.utils import append_documentation, append_options


class FieldElement:
    from karapace.protobuf.compare_type_storage import CompareTypes

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
    ) -> None:
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

    def compare(self, other: 'FieldElement', result: CompareResult, types: CompareTypes) -> None:

        if self.name != other.name:
            result.add_modification(Modification.FIELD_NAME_ALTER)

        self.compare_type(ProtoType.get2(self.element_type), ProtoType.get2(other.element_type), other.label, result, types)

    def compare_map(self, self_map: ProtoType, other_map: ProtoType, result: CompareResult, types: CompareTypes) -> None:
        self.compare_type(self_map.key_type, other_map.key_type, "", result, types)
        self.compare_type(self_map.value_type, other_map.value_type, "", result, types)

    def compare_type(
        self, self_type: ProtoType, other_type: ProtoType, other_label: str, result: CompareResult, types: CompareTypes
    ) -> None:
        from karapace.protobuf.enum_element import EnumElement
        self_type_record = types.get_self_type(self_type)
        other_type_record = types.get_other_type(other_type)
        self_is_scalar: bool = False
        other_is_scalar: bool = False

        if isinstance(self_type_record, TypeRecordMap):
            self_type = self_type_record.map_type()

        if isinstance(other_type_record, TypeRecordMap):
            other_type = other_type_record.map_type()

        self_is_enum: bool = False
        other_is_enum: bool = False

        if self_type_record and isinstance(self_type_record.type_element, EnumElement):
            self_is_enum = True

        if other_type_record and isinstance(other_type_record.type_element, EnumElement):
            other_is_enum = True

        if self_type.is_scalar or self_is_enum:
            self_is_scalar = True

        if other_type.is_scalar or other_is_enum:
            other_is_scalar = True
        if self_is_scalar == other_is_scalar and \
                self_type.is_map == other_type.is_map:
            if self_type.is_map:
                self.compare_map(self_type, other_type, result, types)
            elif self_is_scalar:
                self_compatibility_kind = self_type.compatibility_kind(self_is_enum)
                other_compatibility_kind = other_type.compatibility_kind(other_is_enum)
                if other_label == '':
                    other_label = None
                if self.label != other_label \
                        and self_compatibility_kind in \
                        [ProtoType.CompatibilityKind.VARIANT,
                         ProtoType.CompatibilityKind.DOUBLE,
                         ProtoType.CompatibilityKind.FLOAT,
                         ProtoType.CompatibilityKind.FIXED64,
                         ProtoType.CompatibilityKind.FIXED32,
                         ProtoType.CompatibilityKind.SVARIANT]:
                    result.add_modification(Modification.FIELD_LABEL_ALTER)
                if self_compatibility_kind != other_compatibility_kind:
                    result.add_modification(Modification.FIELD_KIND_ALTER)
            else:
                self.compare_message(self_type, other_type, result, types)
        else:
            result.add_modification(Modification.FIELD_KIND_ALTER)

    @classmethod
    def compare_message(
        cls, self_type: ProtoType, other_type: ProtoType, result: CompareResult, types: CompareTypes
    ) -> None:
        from karapace.protobuf.message_element import MessageElement
        self_type_record = types.get_self_type(self_type)
        other_type_record = types.get_other_type(other_type)
        self_type_element: MessageElement = self_type_record.type_element
        other_type_element: MessageElement = other_type_record.type_element

        if types.self_type_short_name(self_type) != types.other_type_short_name(other_type):
            result.add_modification(Modification.FIELD_NAME_ALTER)
        else:
            self_type_element.compare(other_type_element, result, types)
