# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ProtoParser.kt

from builtins import str
from enum import Enum
from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.exception import IllegalArgumentException, SchemaParseException
from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.extensions_element import ExtensionsElement
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.group_element import GroupElement
from karapace.protobuf.kotlin_wrapper import KotlinRange, options_to_list
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.option_reader import OptionReader
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.reserved_document import ReservedElement
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.syntax import Syntax
from karapace.protobuf.syntax_reader import SyntaxReader
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.utils import MAX_TAG_VALUE
from typing import List, Union


class Context(Enum):
    FILE = 1
    MESSAGE = 2
    ENUM = 3
    RPC = 4
    EXTEND = 5
    SERVICE = 6

    def permits_package(self) -> bool:
        return self == Context.FILE

    def permits_syntax(self) -> bool:
        return self == Context.FILE

    def permits_import(self) -> bool:
        return self == Context.FILE

    def permits_extensions(self) -> bool:
        return self == Context.MESSAGE

    def permits_rpc(self) -> bool:
        return self == Context.SERVICE

    def permits_one_of(self) -> bool:
        return self == Context.MESSAGE

    def permits_message(self) -> bool:
        return self in [Context.FILE, Context.MESSAGE]

    def permits_service(self) -> bool:
        return self in [Context.FILE]

    def permits_enum(self) -> bool:
        return self in [Context.FILE, Context.MESSAGE]

    def permits_extend(self) -> bool:
        return self in [Context.FILE, Context.MESSAGE]


class ProtoParser:
    def __init__(self, location: Location, data: str) -> None:
        self.location = location
        self.imports: List[str] = []
        self.nested_types: List[str] = []
        self.services: List[str] = []
        self.extends_list: List[str] = []
        self.options: List[str] = []
        self.declaration_count = 0
        self.syntax: Union[Syntax, None] = None
        self.package_name: Union[str, None] = None
        self.prefix = ""
        self.data = data
        self.public_imports: List[str] = []
        self.reader = SyntaxReader(data, location)

    def read_proto_file(self) -> ProtoFileElement:
        while True:
            documentation = self.reader.read_documentation()
            if self.reader.exhausted():
                return ProtoFileElement(
                    self.location, self.package_name, self.syntax, self.imports, self.public_imports, self.nested_types,
                    self.services, self.extends_list, self.options
                )
            declaration = self.read_declaration(documentation, Context.FILE)
            if isinstance(declaration, TypeElement):
                # TODO: add check for exception?
                duplicate = next((x for x in iter(self.nested_types) if x.name == declaration.name), None)
                if duplicate:
                    raise SchemaParseException(
                        f"{declaration.name} ({declaration.location}) is already defined at {duplicate.location}"
                    )
                self.nested_types.append(declaration)

            elif isinstance(declaration, ServiceElement):
                duplicate = next((x for x in iter(self.services) if x.name == declaration.name), None)
                if duplicate:
                    raise SchemaParseException(
                        f"{declaration.name} ({declaration.location}) is already defined at {duplicate.location}"
                    )
                self.services.append(declaration)

            elif isinstance(declaration, OptionElement):
                self.options.append(declaration)

            elif isinstance(declaration, ExtendElement):
                self.extends_list.append(declaration)

    def read_declaration(
        self, documentation: str, context: Context
    ) -> Union[None, OptionElement, ReservedElement, RpcElement, MessageElement, EnumElement, EnumConstantElement,
               ServiceElement, ExtendElement, ExtensionsElement, OneOfElement, GroupElement, FieldElement]:
        index = self.declaration_count
        self.declaration_count += 1

        # Skip unnecessary semicolons, occasionally used after a nested message declaration.
        if self.reader.peek_char(';'):
            return None

        location = self.reader.location()
        label = self.reader.read_word()

        # TODO(benoit) Let's better parse the proto keywords. We are pretty weak when field/constants
        #  are named after any of the label we check here.

        result: Union[None, OptionElement, ReservedElement, RpcElement, MessageElement, EnumElement, EnumConstantElement,
                      ServiceElement, ExtendElement, ExtensionsElement, OneOfElement, GroupElement, FieldElement] = None
        # pylint no-else-return
        if label == "package" and context.permits_package():
            self.package_name = self.reader.read_name()
            self.prefix = f"{self.package_name}."
            self.reader.require(';')
        elif label == "import" and context.permits_import():
            import_string = self.reader.read_string()
            if import_string == "public":
                self.public_imports.append(self.reader.read_string())

            else:
                self.imports.append(import_string)
            self.reader.require(';')
        elif label == "syntax" and context.permits_syntax():
            self.reader.expect_with_location(not self.syntax, location, "too many syntax definitions")
            self.reader.require("=")
            self.reader.expect_with_location(
                index == 0, location, "'syntax' element must be the first declaration in a file"
            )

            syntax_string = self.reader.read_quoted_string()
            try:
                self.syntax = Syntax(syntax_string)
            except IllegalArgumentException as e:
                self.reader.unexpected(str(e), location)
            self.reader.require(";")
            result = None
        elif label == "option":
            result = OptionReader(self.reader).read_option("=")
            self.reader.require(";")
        elif label == "reserved":
            result = self.read_reserved(location, documentation)
        elif label == "message" and context.permits_message():
            result = self.read_message(location, documentation)
        elif label == "enum" and context.permits_enum():
            result = self.read_enum_element(location, documentation)
        elif label == "service" and context.permits_service():
            result = self.read_service(location, documentation)
        elif label == "extend" and context.permits_extend():
            result = self.read_extend(location, documentation)
        elif label == "rpc" and context.permits_rpc():
            result = self.read_rpc(location, documentation)
        elif label == "oneof" and context.permits_one_of():
            result = self.read_one_of(documentation)
        elif label == "extensions" and context.permits_extensions():
            result = self.read_extensions(location, documentation)
        elif context in [Context.MESSAGE, Context.EXTEND]:
            result = self.read_field(documentation, location, label)
        elif context == Context.ENUM:
            result = self.read_enum_constant(documentation, location, label)
        else:
            self.reader.unexpected(f"unexpected label: {label}", location)
        return result

    def read_message(self, location: Location, documentation: str) -> MessageElement:
        """ Reads a message declaration. """
        name: str = self.reader.read_name()
        fields: List[FieldElement] = []
        one_ofs: List[OneOfElement] = []
        nested_types: List[TypeElement] = []
        extensions: List[ExtensionsElement] = []
        options: List[OptionElement] = []
        reserveds: List[ReservedElement] = []
        groups: List[GroupElement] = []

        previous_prefix = self.prefix
        self.prefix = f"{self.prefix}{name}."

        self.reader.require("{")
        while True:
            nested_documentation = self.reader.read_documentation()
            if self.reader.peek_char("}"):
                break
            declared = self.read_declaration(nested_documentation, Context.MESSAGE)

            if isinstance(declared, FieldElement):
                fields.append(declared)
            elif isinstance(declared, OneOfElement):
                one_ofs.append(declared)
            elif isinstance(declared, GroupElement):
                groups.append(declared)
            elif isinstance(declared, TypeElement):
                nested_types.append(declared)
            elif isinstance(declared, ExtensionsElement):
                extensions.append(declared)
            elif isinstance(declared, OptionElement):
                options.append(declared)
            # Extend declarations always add in a global scope regardless of nesting.
            elif isinstance(declared, ExtendElement):
                self.extends_list.append(declared)
            elif isinstance(declared, ReservedElement):
                reserveds.append(declared)

        self.prefix = previous_prefix

        return MessageElement(
            location,
            name,
            documentation,
            nested_types,
            options,
            reserveds,
            fields,
            one_ofs,
            extensions,
            groups,
        )

    def read_extend(self, location: Location, documentation: str) -> ExtendElement:
        """ Reads an extend declaration. """
        name = self.reader.read_name()
        fields: list = []
        self.reader.require("{")
        while True:
            nested_documentation = self.reader.read_documentation()
            if self.reader.peek_char("}"):
                break

            declared = self.read_declaration(nested_documentation, Context.EXTEND)
            if isinstance(declared, FieldElement):
                fields.append(declared)
            # TODO: add else clause to catch unexpected declarations.
            else:
                pass

        return ExtendElement(
            location,
            name,
            documentation,
            fields,
        )

    def read_service(self, location: Location, documentation: str) -> ServiceElement:
        """ Reads a service declaration and returns it. """
        name = self.reader.read_name()
        rpcs = []
        options: list = []
        self.reader.require('{')
        while True:
            rpc_documentation = self.reader.read_documentation()
            if self.reader.peek_char("}"):
                break
            declared = self.read_declaration(rpc_documentation, Context.SERVICE)
            if isinstance(declared, RpcElement):
                rpcs.append(declared)
            elif isinstance(declared, OptionElement):
                options.append(declared)
            # TODO: add else clause to catch unexpected declarations.
            else:
                pass

        return ServiceElement(
            location,
            name,
            documentation,
            rpcs,
            options,
        )

    def read_enum_element(self, location: Location, documentation: str) -> EnumElement:
        """ Reads an enumerated atype declaration and returns it. """
        name = self.reader.read_name()
        constants: list = []
        options: list = []
        self.reader.require("{")
        while True:
            value_documentation = self.reader.read_documentation()
            if self.reader.peek_char("}"):
                break
            declared = self.read_declaration(value_documentation, Context.ENUM)

            if isinstance(declared, EnumConstantElement):
                constants.append(declared)
            elif isinstance(declared, OptionElement):
                options.append(declared)
            # TODO: add else clause to catch unexpected declarations.
            else:
                pass
        return EnumElement(location, name, documentation, options, constants)

    def read_field(self, documentation: str, location: Location, word: str) -> Union[GroupElement, FieldElement]:
        label: Union[None, Field.Label]
        atype: str
        if word == "required":
            self.reader.expect_with_location(
                self.syntax != Syntax.PROTO_3, location, "'required' label forbidden in proto3 field declarations"
            )
            label = Field.Label.REQUIRED
            atype = self.reader.read_data_type()
        elif word == "optional":
            label = Field.Label.OPTIONAL

            atype = self.reader.read_data_type()

        elif word == "repeated":
            label = Field.Label.REPEATED
            atype = self.reader.read_data_type()
        else:
            self.reader.expect_with_location(
                self.syntax == Syntax.PROTO_3 or (word == "map" and self.reader.peek_char() == "<"), location,
                f"unexpected label: {word}"
            )

            label = None
            atype = self.reader.read_data_type_by_name(word)

        self.reader.expect_with_location(not atype.startswith("map<") or not label, location, "'map' type cannot have label")
        if atype == "group":
            return self.read_group(location, documentation, label)
        return self.read_field_with_label(location, documentation, label, atype)

    def read_field_with_label(
        self, location: Location, documentation: str, label: Union[None, Field.Label], atype: str
    ) -> FieldElement:
        """ Reads an field declaration and returns it. """
        name = self.reader.read_name()
        self.reader.require('=')
        tag = self.reader.read_int()

        # Mutable copy to extract the default value, and add packed if necessary.
        options: list = OptionReader(self.reader).read_options()

        default_value = self.strip_default(options)
        json_name = self.strip_json_name(options)
        self.reader.require(';')

        documentation = self.reader.try_append_trailing_documentation(documentation)

        return FieldElement(
            location,
            label,
            atype,
            name,
            default_value,
            json_name,
            tag,
            documentation,
            options_to_list(options),
        )

    def strip_default(self, options: list) -> Union[str, None]:
        """ Defaults aren't options. """
        return self.strip_value("default", options)

    def strip_json_name(self, options: list) -> Union[None, str]:
        """ `json_name` isn't an option. """
        return self.strip_value("json_name", options)

    @staticmethod
    def strip_value(name: str, options: list) -> Union[None, str]:
        """ This finds an option named [name], removes, and returns it.
        Returns None if no [name] option is present.
        """
        result: Union[None, str] = None
        for element in options[:]:
            if element.name == name:
                options.remove(element)
                result = str(element.value)
        return result

    def read_one_of(self, documentation: str) -> OneOfElement:
        name: str = self.reader.read_name()
        fields: list = []
        groups: list = []
        options: list = []

        self.reader.require("{")
        while True:
            nested_documentation = self.reader.read_documentation()
            if self.reader.peek_char("}"):
                break

            location = self.reader.location()
            atype = self.reader.read_data_type()
            if atype == "group":
                groups.append(self.read_group(location, nested_documentation, None))
            elif atype == "option":
                options.append(OptionReader(self.reader).read_option("="))
                self.reader.require(";")
            else:
                fields.append(self.read_field_with_label(location, nested_documentation, None, atype))

        return OneOfElement(
            name,
            documentation,
            fields,
            groups,
            options,
        )

    def read_group(
        self,
        location: Location,
        documentation: str,
        label: Union[None, Field.Label],
    ) -> GroupElement:
        name = self.reader.read_word()
        self.reader.require("=")
        tag = self.reader.read_int()
        fields: list = []
        self.reader.require("{")

        while True:
            nested_documentation = self.reader.read_documentation()
            if self.reader.peek_char("}"):
                break

            field_location = self.reader.location()
            field_label = self.reader.read_word()
            field = self.read_field(nested_documentation, field_location, field_label)
            if isinstance(field, FieldElement):
                fields.append(field)
            else:
                self.reader.unexpected(f"expected field declaration, was {field}")

        return GroupElement(label, location, name, tag, documentation, fields)

    def read_reserved(self, location: Location, documentation: str) -> ReservedElement:
        """ Reads a reserved tags and names list like "reserved 10, 12 to 14, 'foo';". """
        values: list = []
        while True:
            ch = self.reader.peek_char()
            if ch in ["\"", "'"]:
                values.append(self.reader.read_quoted_string())
            else:
                tag_start = self.reader.read_int()
                ch = self.reader.peek_char()
                if ch in [",", ";"]:
                    values.append(tag_start)
                else:
                    self.reader.expect_with_location(self.reader.read_word() == "to", location, "expected ',', ';', or 'to'")
                    tag_end = self.reader.read_int()
                    values.append(KotlinRange(tag_start, tag_end))

            ch = self.reader.read_char()
            # pylint: disable=no-else-break
            if ch == ";":
                break
            elif ch == ",":
                continue
            else:
                self.reader.unexpected("expected ',' or ';'")
        a = False
        if values:
            a = True

        self.reader.expect_with_location(a, location, "'reserved' must have at least one field name or tag")
        my_documentation = self.reader.try_append_trailing_documentation(documentation)

        return ReservedElement(location, my_documentation, values)

    def read_extensions(self, location: Location, documentation: str) -> ExtensionsElement:
        """ Reads extensions like "extensions 101;" or "extensions 101 to max;". """
        values: list = []
        while True:
            start: int = self.reader.read_int()
            ch = self.reader.peek_char()
            end: int
            if ch in [",", ";"]:
                values.append(start)
            else:
                self.reader.expect_with_location(self.reader.read_word() == "to", location, "expected ',', ';' or 'to'")
                s = self.reader.read_word()
                if s == "max":
                    end = MAX_TAG_VALUE
                else:
                    end = int(s)
                values.append(KotlinRange(start, end))

            ch = self.reader.read_char()
            # pylint: disable=no-else-break
            if ch == ";":
                break
            elif ch == ",":
                continue
            else:
                self.reader.unexpected("expected ',' or ';'")

        return ExtensionsElement(location, documentation, values)

    def read_enum_constant(self, documentation: str, location: Location, label: str) -> EnumConstantElement:
        """ Reads an enum constant like "ROCK = 0;". The label is the constant name. """
        self.reader.require('=')
        tag = self.reader.read_int()

        options: list = OptionReader(self.reader).read_options()
        self.reader.require(';')

        documentation = self.reader.try_append_trailing_documentation(documentation)

        return EnumConstantElement(
            location,
            label,
            tag,
            documentation,
            options,
        )

    def read_rpc(self, location: Location, documentation: str) -> RpcElement:
        """ Reads an rpc and returns it. """
        name = self.reader.read_name()

        self.reader.require('(')
        request_streaming = False

        word = self.reader.read_word()
        if word == "stream":
            request_streaming = True
            request_type = self.reader.read_data_type()
        else:
            request_type = self.reader.read_data_type_by_name(word)

        self.reader.require(')')

        self.reader.expect_with_location(self.reader.read_word() == "returns", location, "expected 'returns'")

        self.reader.require('(')
        response_streaming = False

        word = self.reader.read_word()
        if word == "stream":
            response_streaming = True
            response_type = self.reader.read_data_type()
        else:
            response_type = self.reader.read_data_type_by_name(word)

        self.reader.require(')')

        options: list = []
        if self.reader.peek_char('{'):
            while True:
                rpc_documentation = self.reader.read_documentation()
                if self.reader.peek_char('}'):
                    break
                declared = self.read_declaration(rpc_documentation, Context.RPC)
                if isinstance(declared, OptionElement):
                    options.append(declared)
                # TODO: add else clause to catch unexpected declarations.
                else:
                    pass

        else:
            self.reader.require(';')

        return RpcElement(
            location, name, documentation, request_type, response_type, request_streaming, response_streaming, options
        )

    @staticmethod
    def parse(location: Location, data: str) -> ProtoFileElement:
        """ Parse a named `.proto` schema. """
        proto_parser = ProtoParser(location, data)
        return proto_parser.read_proto_file()
