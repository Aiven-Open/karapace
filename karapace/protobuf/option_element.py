# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/OptionElement.kt

from enum import Enum
# from karapace.protobuf.kotlin_wrapper import *
# from karapace.protobuf.kotlin_wrapper import *
from karapace.protobuf.utils import append_indented


def try_to_schema(obj: object) -> str:
    try:
        return obj.to_schema()
    except AttributeError:
        if isinstance(obj, str):
            return obj
        raise AttributeError


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

    name: str
    kind: Kind
    value = None
    """ If true, this [OptionElement] is a custom option. """
    is_parenthesized: bool

    def __init__(self, name: str, kind: Kind, value, is_parenthesized: bool = None):
        self.name = name
        self.kind = kind
        self.value = value
        self.is_parenthesized = is_parenthesized
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

    def to_schema_declaration(self):
        return f"option {self.to_schema()};\n"

    @staticmethod
    def append_options(options: list):
        data: list = list()
        count = len(options)
        if count == 1:
            data.append('[')
            data.append(try_to_schema(options[0]))
            data.append(']')
            return "".join(data)

        data.append("[\n")
        for i in range(0, count):
            if i < count:
                endl = ","
            else:
                endl = ""
            append_indented(data, try_to_schema(options[i]) + endl)
        data.append(']')
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

    def __repr__(self):
        return self.to_schema()

    def __eq__(self, other):
        return str(self) == str(other)