from karapace.protobuf.kotlin_wrapper import IntRange
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

        for index in range(len(self.values)):
            value = self.values[index]
            if index > 0:
                result.append(", ")
            if isinstance(value, int):
                result.append(value)
            # TODO: maybe replace Kotlin IntRange by list?
            elif isinstance(value, IntRange):
                result.append(f"{value[0]} to ")
                last_value = value[len(value) - 1]
                if last_value < MAX_TAG_VALUE:
                    result.append(last_value)
                else:
                    result.append("max")
            else:
                raise AssertionError()

        result.append(";\n")
        return "".join(result)
