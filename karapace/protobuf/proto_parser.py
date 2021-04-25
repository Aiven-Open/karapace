from builtins import str

from enum import Enum

from typing import List, Any, Union

from io import StringIO

from karapace.protobuf.location import Location
from karapace.protobuf.option_reader import OptionReader
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.syntax import Syntax
from karapace.protobuf.syntax_reader import SyntaxReader
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.exception import error
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.extend_element import ExtendElement


from enum import Enum



class TypeElement :
    location: Location
    name: str
    documentation: str
    options: list
    nested_types: list
    def to_schema(self):
        pass


class SyntaxReader:
    pass


class Context :
    FILE = "file"





class ProtoParser:
    location: Location
    reader: SyntaxReader
    public_imports: list
    imports: list
    nested_types: list
    services: list
    extends_list: list
    options: list
    declaration_count: int = 0
    syntax: Syntax = None
    package_name: str = None
    prefix: str = ""

    def __int__(self, location: Location, data:str):
        self.reader = SyntaxReader(data, location)

    def read_proto_file(self) -> ProtoFileElement:
        while True:
            documentation  = self.reader.read_documentation()
            if self.reader.exhausted():
                return ProtoFileElement(self.location, self.package_name, self.syntax, self.imports,
                                        self.public_imports, self.nested_types, self.services, self.extends_list,
                                        self.options)
            declaration = self.read_declaration(documentation, Context.FILE)
            if type(declaration) is TypeElement :
                # TODO: must add check for execption
                duplicate = next((x for x in iter(self.nested_types) if x.name == declaration.name), None)
                if duplicate :
                    error(f"{declaration.name} ({declaration.location}) is already defined at {duplicate.location}")
                self.nested_types.append(declaration)

            if type(declaration) is ServiceElement :
                duplicate = next((x for x in iter(self.services) if x.name == declaration.name), None)
                if duplicate :
                    error(f"{declaration.name} ({declaration.location}) is already defined at {duplicate.location}")
                self.services.append(declaration)

            if type(declaration) is OptionElement:
                    self.options.append(declaration)

            if type(declaration) is ExtendElement :
                self.extends_list.append(declaration)



    def read_declaration(self, documentation: str, context: Context):
        self.declaration_count += 1
        index = self.declaration_count

        # Skip unnecessary semicolons, occasionally used after a nested message declaration.
        if self.reader.peek_char(';') : return None
        

        location = self.reader.location()
        label = self.reader.read_word()

        # TODO(benoit) Let's better parse the proto keywords. We are pretty weak when field/constants
        #  are named after any of the label we check here.

        result = None
        if label == "package" and context.permits_package()  :
            self.package_name = self.reader.readName()
            self.prefix = f"{self.package_name}."
            self.reader.require(';')
            return result
        elif  label == "import" and context.permits_import()  :
            import_string = self.reader.read_string()
            if import_string == "public" :
                self.public_imports.append(self.reader.read_string())
            
            else :
                self.imports.append(import_string)
            self.reader.require(';')
            return result
        elif label == "syntax" and context.permits_syntax()  :
            self.reader.expect(self.syntax == None, location, "too many syntax definitions" )
            self.reader.require('=')
            self.reader.expect(index == 0, location ,"'syntax' element must be the first declaration in a file")

            syntax_string = self.reader.read_quoted_string()
            try :
                syntax = Syntax(syntax_string)
            except Exception as e:
                # TODO: } catch (e: IllegalArgumentException) { ???
                self.reader.unexpected(str(e), location)
            self.reader.require(';')
            return result
        elif label == "option"  :
            result = OptionReader(self.reader).read_option('=')
            self.reader.require(';')
            return result
        elif label == "reserved" :
            return self.read_reserved(location, documentation)
        elif label == "message" and context.permits_message() :
            return self.read_message(location, documentation)
        elif label == "enum" and context.permits_enum() :
            return self.read_enum_element(location, documentation)
        elif label == "service" and context.permits_service() :
            return self.read_service(location, documentation)
        elif label == "extend" and context.permits_extend() :
            return self.read_extend(location, documentation)
        elif label == "rpc" and context.permits_rpc()  :
            return self.seread_rpc(location, documentation)
        elif label == "oneof" and context.permits_one_of()  :
            return self.read_one_of(documentation)
        elif label == "extensions" and context.permits_extensions()  :
            return self.read_extensions(location, documentation)
        elif context == Context.MESSAGE or context == Context.EXTEND  :
            return self.read_field(documentation, location, label)
        elif context == Context.ENUM  :
            return self.read_enum_constant(documentation, location, label)
        else :
            self.reader.unexpected("unexpected label: $label", location)


    """ Reads a message declaration. """
    def read_message( self, location: Location,    documentation: String  )-> MessageElement : 
        name :str = self.reader.readName()
        fields:list = list()
        one_ofs:list = list()
        nestedTypes:list = list()
        extensions :list = list()
        options :list = list()
        reserveds :list = list()
        groups :list = list()

        previousPrefix = self.prefix
        self.prefix = f"{self.prefix}{name}."

        self.reader.require('{')
        while True :
            nested_documentation = self.reader.read_documentation()
            if self.reader.peek_char('}'):
                break

            declared = self.read_declaration(nested_documentation, Context.MESSAGE) :
            type_declared = type(declared)
            if type_declared  is FieldElement : 
                fields.append(declared)
            elif type_declared  is OneOfElement : 
                one_ofs.append(declared)
            elif type_declared  is GroupElement : 
                groups.append(declared)
            elif type_declared  is TypeElement : 
                nestedTypes.append(declared)
            elif type_declared  is ExtensionsElement : 
                extensions.append(declared)
            elif type_declared  is OptionElement :
                options.append(declared)
            # Extend declarations always add in a global scope regardless of nesting.
            elif type_declared  is ExtendElement :
                self.extends_list.append(declared)
            elif type_declared  is ReservedElement :
                reserveds.append(declared)
 