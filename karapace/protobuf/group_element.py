from typing import Union

from karapace.protobuf.field import Field
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented


class GroupElement:
    label: Field.Label
    location: Location
    name: str
    tag: int
    documentation: str = ""
    fields: list = list()

    def __init__(self, label: Union[None, Field.Label], location: Location, name: str, tag: int, documentation: str,
                 fields: list):
        self.label = label
        self.location = location
        self.name = name
        self.tag = tag
        self.documentation = documentation
        self.fields = fields

    def to_schema(self) -> str:
        result: list = []
        append_documentation(result, self.documentation)

        # TODO: compare lower() to lowercase() and toLowerCase(Locale.US) Kotlin
        if self.label:
            result.append(f"{str(self.label.name).lower()} ")
        result.append(f"group {self.name} = {self.tag} {{")
        if self.fields and len(self.fields):
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())
        result.append("}\n")
        return "".join(result)
