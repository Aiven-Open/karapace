from karapace.protobuf.kotlin_wrapper import IntRange
from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation


class ReservedElement:
    location: Location
    documentation: str
    """ A [String] name or [Int] or [IntRange] tag. """
    values: list

    def __init__(self, location: Location, documentation: str, values: list):
        self.location = location
        self.documentation = documentation
        self.values = values

    def to_schema(self) -> str:
        result: list = list()
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
            elif isinstance(value, IntRange):
                last_index = len(value) - 1
                result.append(f"{value[0]} to {value[last_index]}")
            else:
                raise AssertionError()
        result.append(";\n")
        return "".join(result)
