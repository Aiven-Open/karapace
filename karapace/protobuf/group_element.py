# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/GroupElement.kt
from karapace.protobuf.field import Field
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_indented
from typing import Union


class GroupElement:
    def __init__(
        self,
        label: Union[None, Field.Label],
        location: Location,
        name: str,
        tag: int,
        documentation: str = "",
        fields: list = None
    ) -> None:
        self.label = label
        self.location = location
        self.name = name
        self.tag = tag

        self.fields = fields or []
        self.documentation = documentation

    def to_schema(self) -> str:
        result: list = []
        append_documentation(result, self.documentation)

        # TODO: compare lower() to lowercase() and toLowerCase(Locale.US) Kotlin
        if self.label:
            result.append(f"{str(self.label.name).lower()} ")
        result.append(f"group {self.name} = {self.tag} {{")
        if self.fields:
            result.append("\n")
            for field in self.fields:
                append_indented(result, field.to_schema())
        result.append("}\n")
        return "".join(result)
