# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/OneOfElement.kt
from itertools import chain
from karapace.protobuf.compare_result import CompareResult, Modification
from karapace.protobuf.compare_type_storage import CompareTypes
from karapace.protobuf.utils import append_documentation, append_indented


class OneOfElement:
    def __init__(self, name: str, documentation: str = "", fields=None, groups=None, options=None) -> None:
        self.name = name
        self.documentation = documentation
        self.fields = fields or []
        self.options = options or []
        self.groups = groups or []

    def to_schema(self) -> str:
        result: list = []
        append_documentation(result, self.documentation)
        result.append(f"oneof {self.name} {{")
        if self.options:
            result.append("\n")
            for option in self.options:
                append_indented(result, option.to_schema_declaration())

        if self.fields:
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())
        if self.groups:
            result.append("\n")
            for group in self.groups:
                append_indented(result, group.to_schema())
        result.append("}\n")
        return "".join(result)

    def compare(self, other: 'OneOfElement', result: CompareResult, types: CompareTypes) -> None:
        self_tags: dict = {}
        other_tags: dict = {}

        for field in self.fields:
            self_tags[field.tag] = field
        for field in other.fields:
            other_tags[field.tag] = field

        for tag in chain(self_tags.keys(), other_tags.keys() - self_tags.keys()):
            result.push_path(str(tag))

            if self_tags.get(tag) is None:
                result.add_modification(Modification.ONE_OF_FIELD_ADD)
            elif other_tags.get(tag) is None:
                result.add_modification(Modification.ONE_OF_FIELD_DROP)
            else:
                self_tags[tag].compare(other_tags[tag], result, types)
            result.pop_path()
