# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ExtensionsElement.kt

from karapace.protobuf.kotlin_wrapper import KotlinRange
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, MAX_TAG_VALUE


class ExtensionsElement:
    location: Location
    documentation: str = ""
    """ An [Int] or [IntRange] tag. """
    values: list

    def __init__(self, location: Location, documentation: str, values: list):
        self.location = location
        self.documentation = documentation
        self.values = values

    def to_schema(self) -> str:
        result: list = []
        append_documentation(result, self.documentation)
        result.append("extensions ")

        for index in range(0, len(self.values)):
            value = self.values[index]
            if index > 0:
                result.append(", ")
            if isinstance(value, int):
                result.append(str(value))
            # TODO: maybe replace Kotlin IntRange by list?
            elif isinstance(value, KotlinRange):
                result.append(f"{value.minimum} to ")
                last_value = value.maximum
                if last_value < MAX_TAG_VALUE:
                    result.append(str(last_value))
                else:
                    result.append("max")
            else:
                raise AssertionError()

        result.append(";\n")
        return "".join(result)
