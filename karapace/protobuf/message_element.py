# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/MessageElement.kt

from karapace.protobuf.location import Location
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.utils import append_documentation, append_indented


class MessageElement(TypeElement):
    def __init__(
        self,
        location: Location,
        name: str,
        documentation: str = "",
        nested_types: list = None,
        options: list = None,
        reserveds: list = None,
        fields: list = None,
        one_ofs: list = None,
        extensions: list = None,
        groups: list = None,
    ):
        super().__init__(location, name, documentation, options or [], nested_types or [])
        self.reserveds = reserveds or []
        self.fields = fields or []
        self.one_ofs = one_ofs or []
        self.extensions = extensions or []
        self.groups = groups or []

    def to_schema(self) -> str:
        result: list = list()
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