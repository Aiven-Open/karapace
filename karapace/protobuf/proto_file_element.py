# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ProtoFileElement.kt

from karapace.protobuf.location import Location
from karapace.protobuf.syntax import Syntax


class ProtoFileElement:
    def __init__(
        self,
        location: Location,
        package_name: str = None,
        syntax: Syntax = None,
        imports: list = None,
        public_imports: list = None,
        types=None,
        services: list = None,
        extend_declarations: list = None,
        options: list = None
    ):
        if types is None:
            types = []
        self.location = location
        self.package_name = package_name
        self.syntax = syntax
        self.options = options or []
        self.extend_declarations = extend_declarations or []
        self.services = services or []
        self.types = types or []
        self.public_imports = public_imports or []
        self.imports = imports or []

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
    def __eq__(self, other: 'ProtoFileElement'):  # type: ignore
        a = self.to_schema()
        b = other.to_schema()

        return a == b

    def __repr__(self):
        return self.to_schema()
