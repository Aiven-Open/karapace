# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/Schema.kt
# Ported partially for required functionality.
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.proto_parser import ProtoParser
from karapace.protobuf.utils import append_documentation, append_indented

import logging

log = logging.getLogger(__name__)


def add_slashes(text: str) -> str:
    escape_dict = {
        '\a': '\\a',
        '\b': '\\b',
        '\f': '\\f',
        '\n': '\\n',
        '\r': '\\r',
        '\t': '\\t',
        '\v': '\\v',
        '\'': "\\'",
        '\"': '\\"',
        '\\': '\\\\'
    }
    result: str = ""
    for char in text:
        c = escape_dict.get(char)
        result += c if c is not None else char
    return result


def message_element_string(element: MessageElement) -> str:
    result: list = list()
    append_documentation(result, element.documentation)
    result.append(f"message {element.name} {{")
    if element.reserveds:
        result.append("\n")
        for reserved in element.reserveds:
            append_indented(result, reserved.to_schema())

    if element.options:
        result.append("\n")
        for option in element.options:
            append_indented(result, option_element_string(option))

    if element.fields:
        result.append("\n")
        for field in element.fields:
            append_indented(result, field.to_schema())

    if element.one_ofs:
        result.append("\n")
        for one_of in element.one_ofs:
            append_indented(result, one_of.to_schema())

    if element.groups:
        result.append("\n")
        for group in element.groups:
            append_indented(result, group.to_schema())

    if element.extensions:
        result.append("\n")
        for extension in element.extensions:
            append_indented(result, extension.to_schema())

    if element.nested_types:
        result.append("\n")
        for nested_type in element.nested_types:
            if isinstance(nested_type, MessageElement):
                append_indented(result, message_element_string(nested_type))

        for nested_type in element.nested_types:
            if isinstance(nested_type, EnumElement):
                append_indented(result, enum_element_string(nested_type))

    result.append("}\n")
    return "".join(result)


def enum_element_string(element: EnumElement) -> str:
    return element.to_schema()


def option_element_string(option: OptionElement):
    result: str
    if option.kind == OptionElement.Kind.STRING:
        name: str
        if option.is_parenthesized:
            name = f"({option.name})"
        else:
            name = option.name
        value = add_slashes(str(option.value))
        result = f"{name} = \"{value}\""
    else:
        result = option.to_schema()

    return f"option {result};\n"


class ProtobufSchema:
    DEFAULT_LOCATION = Location.get("")

    def __init__(self, schema: str):
        self.dirty = schema
        self.cache_string = ""
        self.schema = ProtoParser.parse(self.DEFAULT_LOCATION, schema)

    def __str__(self) -> str:
        if not self.cache_string:
            self.cache_string = self.to_schema()
        log.warning("CACHE_STRING:%s", self.cache_string)
        return self.cache_string

    def to_json(self) -> str:
        return self.to_schema()

    def to_schema(self):
        strings: list = []
        shm: ProtoFileElement = self.schema
        if shm.syntax:
            strings.append("syntax = \"")
            strings.append(str(shm.syntax))
            strings.append("\";\n")

        if shm.package_name:
            strings.append("package " + str(shm.package_name) + ";\n")

        if shm.imports or shm.public_imports:
            strings.append("\n")

            for file in shm.imports:
                strings.append("import \"" + str(file) + "\";\n")

            for file in shm.public_imports:
                strings.append("import public \"" + str(file) + "\";\n")

        if shm.options:
            strings.append("\n")
            for option in shm.options:
                # strings.append(str(option.to_schema_declaration()))
                strings.append(option_element_string(option))

        if shm.types:
            strings.append("\n")
            for type_element in shm.types:
                if isinstance(type_element, MessageElement):
                    strings.append(message_element_string(type_element))
            for type_element in shm.types:
                if isinstance(type_element, EnumElement):
                    strings.append(enum_element_string(type_element))

        if shm.extend_declarations:
            strings.append("\n")
            for extend_declaration in shm.extend_declarations:
                strings.append(str(extend_declaration.to_schema()))

        if shm.services:
            strings.append("\n")
            for service in shm.services:
                strings.append(str(service.to_schema()))
        return "".join(strings)
