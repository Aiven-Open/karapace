# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/OptionElement.kt

from enum import Enum
# from karapace.protobuf.kotlin_wrapper import *
# from karapace.protobuf.kotlin_wrapper import *
from karapace.protobuf.utils import append_indented, append_options, try_to_schema


class ListOptionElement(list):
    pass


class OptionElement:
    class Kind(Enum):
        STRING = 1
        BOOLEAN = 2
        NUMBER = 3
        ENUM = 4
        MAP = 5
        LIST = 6
        OPTION = 7

    def __init__(self, name: str, kind: Kind, value, is_parenthesized: bool = None) -> None:
        self.name = name
        self.kind = kind
        self.value = value
        """ If true, this [OptionElement] is a custom option. """
        self.is_parenthesized = is_parenthesized or False
        self.formattedName = f"({self.name})" if is_parenthesized else self.name

    def to_schema(self) -> str:
        aline = None
        if self.kind == self.Kind.STRING:
            aline = f"{self.formattedName} = \"{self.value}\""
        elif self.kind in [self.Kind.BOOLEAN, self.Kind.NUMBER, self.Kind.ENUM]:
            aline = f"{self.formattedName} = {self.value}"
        elif self.kind == self.Kind.OPTION:
            aline = f"{self.formattedName}.{try_to_schema(self.value)}"
        elif self.kind == self.Kind.MAP:
            aline = [f"{self.formattedName} = {{\n", self.format_option_map(self.value), "}"]
        elif self.kind == self.Kind.LIST:
            aline = [f"{self.formattedName} = ", self.append_options(self.value)]

        if isinstance(aline, list):
            return "".join(aline)
        return aline

    def to_schema_declaration(self) -> str:
        return f"option {self.to_schema()};\n"

    @staticmethod
    def append_options(options: list) -> str:
        data: list = []
        append_options(data, options)
        return "".join(data)

    def format_option_map(self, value: dict) -> str:
        keys = list(value.keys())
        last_index = len(keys) - 1
        result: list = list()
        for index, key in enumerate(keys):
            endl = "," if (index != last_index) else ""
            append_indented(result, f"{key}: {self.format_option_map_value(value[key])}{endl}")
        return "".join(result)

    def format_option_map_value(self, value) -> str:
        aline = value
        if isinstance(value, str):
            aline = f"\"{value}\""
        elif isinstance(value, dict):
            aline = ["{\n", self.format_option_map(value), "}"]
        elif isinstance(value, list):
            aline = ["[\n", self.format_list_map_value(value), "]"]

        if isinstance(aline, list):
            return "".join(aline)
        if isinstance(aline, str):
            return aline
        return value

    def format_list_map_value(self, value) -> str:

        last_index = len(value) - 1
        result: list = []
        for index, elm in enumerate(value):
            endl = "," if (index != last_index) else ""
            append_indented(result, f"{self.format_option_map_value(elm)}{endl}")
        return "".join(result)

    def __repr__(self) -> str:
        return self.to_schema()

    def __eq__(self, other) -> bool:
        return str(self) == str(other)


PACKED_OPTION_ELEMENT = OptionElement("packed", OptionElement.Kind.BOOLEAN, value="true", is_parenthesized=False)
