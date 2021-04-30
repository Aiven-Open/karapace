from enum import Enum
from karapace.protobuf.kotlin_wrapper import *
from karapace.protobuf.utils import append_indented


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

    def __init__(self, name: str, kind: Kind, value, is_parenthesized: bool):
        self.name = name
        self.kind = kind
        self.value = value
        self.is_parenthesized = is_parenthesized
        self.formattedName = f"({self.name})" if is_parenthesized else self.name

    def to_schema(self) -> str:
        aline = {
            self.kind == self.Kind.STRING: f"{self.formattedName} = \"{self.value}\"",
            self.kind in [self.Kind.BOOLEAN, self.Kind.NUMBER, self.Kind.ENUM]: f"{self.formattedName} = {self.value}",
            self.kind == self.Kind.OPTION: f"{self.formattedName}.{self.value.to_schema()}",
            self.kind == self.Kind.MAP: list([f"{self.formattedName} = {{\n",
                                              self.format_option_map(self.value),
                                              "}"
                                              ]),
            self.kind == self.Kind.LIST: list([f"{self.formattedName} = ",
                                               self.append_options(self.value)
                                               ])
        }[True]
        if type(aline) is list:
            return "".join(aline)
        else:
            return aline

    def to_schema_declaration(self):
        return f"option {self.to_schema()};\n"

    @staticmethod
    def append_options(options: list):
        data: list = list()
        count = len(options)
        if count == 1:
            data.append('[')
            data.append(options[0].to_schema())
            data.append(']')
            return "".join(data)

        data.append("[\n")
        for i in range(0, count):
            if i < count - 1:
                endl = ","
            else:
                endl = ""
            append_indented(data, options[i].to_schema() + endl)
        data.append(']')
        return "".join(data)

    def format_option_map(self, value: dict) -> str:
        keys = list(value.keys())
        last_index = len(keys) - 1
        result: StringBuilder = StringBuilder()
        for index in range(len(keys)):
            endl = "," if (index != last_index) else ""
            result.append_indented(f"{keys[index]}: {self.format_option_map_value(value[keys[index]])}{endl}")
        return "".join(result)

    def format_option_map_value(self, value) -> str:
        aline = {
            type(value) is str: f"\"{value}\"",
            type(value) is dict: list(["{\n",
                                       self.format_option_map_value(value),
                                       "}"
                                       ]),
            type(value) is list: list(["[\n",
                                       self.format_list_map_value(value),
                                       "]"
                                       ])
        }[True]

        if type(aline) is list:
            return "".join(aline)
        if type(aline) is str:
            return aline
        return value

    def format_list_map_value(self, value) -> str:
        keys = value.keys()
        last_index = len(value) - 1
        result: StringBuilder = StringBuilder()
        for index in range(len(keys)):
            endl = "," if (index != last_index) else ""
            result.append_indented(f"{self.format_option_map_value(value[keys[index]])}{endl}")
        return "".join(result)

    # TODO: REMOVE WHEN ALL CLEAN
    """ companion object {
    internal PACKED_OPTION_ELEMENT =
        OptionElement("packed", BOOLEAN, value = "true", is_parenthesized = false)

    @JvmOverloads
    def create(
      name: String,
      kind: Kind,
      value: Any,
      is_parenthesized: Boolean = false
    ) = OptionElement(name, kind, value, is_parenthesized)
  }
}
 
 """
