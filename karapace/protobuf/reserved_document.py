# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ReservedElement.kt

from karapace.protobuf.kotlin_wrapper import KotlinRange
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation


class ReservedElement:
    def __init__(self, location: Location, documentation: str = "", values: list = None) -> None:
        self.location = location
        self.documentation = documentation
        """ A [String] name or [Int] or [IntRange] tag. """
        self.values = values or []

    def to_schema(self) -> str:
        result = []
        append_documentation(result, self.documentation)
        result.append("reserved ")

        for index in range(len(self.values)):
            value = self.values[index]
            if index > 0:
                result.append(", ")

            if isinstance(value, str):
                result.append(f"\"{value}\"")
            elif isinstance(value, int):
                result.append(f"{value}")
            elif isinstance(value, KotlinRange):
                result.append(f"{value.minimum} to {value.maximum}")
            else:
                raise AssertionError()
        result.append(";\n")
        return "".join(result)
