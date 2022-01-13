# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/EnumConstantElement.kt
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_options


class EnumConstantElement:
    def __init__(
        self,
        location: Location,
        name: str,
        tag: int,
        documentation: str = "",
        options: Optional[list] = None,
    ) -> None:
        self.location = location
        self.name = name

        self.tag = tag
        self.options = options or []
        self.documentation = documentation or ""

    def to_schema(self) -> str:
        result = []
        append_documentation(result, self.documentation)
        result.append(f"{self.name} = {self.tag}")
        if self.options:
            result.append(" ")
            append_options(result, self.options)
        result.append(";\n")
        return "".join(result)
