from karapace.protobuf.location import Location
from karapace.protobuf.syntax import Syntax


class ProtoFileElement:
    location: Location
    package_name: str
    syntax: Syntax
    imports: list
    public_imports: list
    types: list
    services: list
    extend_declarations: list
    options: list

    def __init__(
        self,
        location: Location,
        package_name: str = None,
        syntax: Syntax = None,
        imports=None,
        public_imports=None,
        types=None,
        services=None,
        extend_declarations=None,
        options=None
    ):

        if not options:
            options = []
        if not extend_declarations:
            extend_declarations = []
        if not services:
            services = []
        if not types:
            types = []
        if not public_imports:
            public_imports = []
        if not imports:
            imports = []
        self.location = location
        self.package_name = package_name
        self.syntax = syntax
        self.imports = imports
        self.public_imports = public_imports
        self.types = types
        self.services = services
        self.extend_declarations = extend_declarations
        self.options = options

    def to_schema(self):
        strings: list = [
            "// Proto schema formatted by Wire, do not edit.\n", "// Source: ",
            str(self.location.with_path_only()), "\n"
        ]
        if self.syntax:
            strings.append("\n")
            strings.append("syntax = \"")
            strings.append(str(self.syntax))
            strings.append("\";\n")

        if self.package_name:
            strings.append("\n")
            strings.append("package " + str(self.package_name) + ";\n")

        if self.imports or self.public_imports:
            strings.append("\n")

            for file in self.imports:
                strings.append("import \"" + str(file) + "\";\n")

            for file in self.public_imports:
                strings.append("import public \"" + str(file) + "\";\n")

        if self.options:
            strings.append("\n")
            for option in self.options:
                strings.append(str(option.to_schema_declaration()))

        if self.types:
            for type_element in self.types:
                strings.append("\n")
                strings.append(str(type_element.to_schema()))

        if self.extend_declarations:
            for extend_declaration in self.extend_declarations:
                strings.append("\n")
                strings.append(str(extend_declaration.to_schema()))

        if self.services:
            for service in self.services:
                strings.append("\n")
                strings.append(str(service.to_schema()))

        return "".join(strings)

    @staticmethod
    def empty(path):
        return ProtoFileElement(Location.get(path))

    # TODO: there maybe be faster comparison workaround
    def __eq__(self, other: 'ProtoFileElement'):
        a = self.to_schema()
        b = other.to_schema()
        # sys.stderr.write("\n\nTESTA=[")
        # sys.stderr.write(a)
        # sys.stderr.write("]\n\nTESTB=[")
        # sys.stderr.write(b)
        # sys.stderr.write("]\n\n")

        return a == b

    def __repr__(self):
        return self.to_schema()

    #    return str(self.location) == str(other.location) and \
    #           self.package_name == other.package_name and \
    #           str(self.syntax) == str(other.syntax) and \
    #           str(self.imports) == str(other.imports) and \
    #           str(self.public_imports) == str(self.public_imports) and \
    #           str(self.types) == str(self.types) and \
    #           str(self.services) == str(self.services) and \
    #           str(self.extend_declarations) == str(self.extend_declarations) and \
    #           str(self.options) == str(self.options)
