# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/ProtoFileElement.kt
from karapace.protobuf.compare_result import CompareResult, Modification
from karapace.protobuf.compare_type_storage import CompareTypes
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.exception import IllegalStateException
from karapace.protobuf.location import Location
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.syntax import Syntax
from karapace.protobuf.type_element import TypeElement


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

    def compare(self, other: 'ProtoFileElement', result: CompareResult) -> CompareResult:

        if self.package_name != other.package_name:
            result.add_modification(Modification.PACKAGE_ALTER)
        # TODO: do we need syntax check?
        if self.syntax != other.syntax:
            result.add_modification(Modification.SYNTAX_ALTER)

        self_types: dict = dict()
        other_types: dict = dict()
        self_indexes: dict = dict()
        other_indexes: dict = dict()
        compare_types = CompareTypes(self.package_name, other.package_name, result)
        type_: TypeElement
        i = 0
        for type_ in self.types:
            self_types[type_.name] = type_
            self_indexes[type_.name] = i
            package_name = self.package_name if self.package_name else ''
            compare_types.add_self_type(package_name, type_)
            i += 1

        i = 0
        for type_ in other.types:
            other_types[type_.name] = type_
            other_indexes[type_.name] = i
            package_name = other.package_name if other.package_name else ''
            compare_types.add_other_type(package_name, type_)
            i += 1

        for name in list(self_types.keys()) + list(set(other_types.keys()) - set(self_types.keys())):

            result.push_path(name, True)

            if self_types.get(name) is None and other_types.get(name) is not None:
                if isinstance(other_types[name], MessageElement):
                    result.add_modification(Modification.MESSAGE_ADD)
                elif isinstance(other_types[name], EnumElement):
                    result.add_modification(Modification.ENUM_ADD)
                else:
                    raise IllegalStateException("Instance of element is not applicable")
            elif self_types.get(name) is not None and other_types.get(name) is None:
                if isinstance(self_types[name], MessageElement):
                    result.add_modification(Modification.MESSAGE_DROP)
                elif isinstance(self_types[name], EnumElement):
                    result.add_modification(Modification.ENUM_DROP)
                else:
                    raise IllegalStateException("Instance of element is not applicable")
            else:
                if other_indexes[name] != self_indexes[name]:
                    if isinstance(self_types[name], MessageElement):
                        # incompatible type
                        result.add_modification(Modification.MESSAGE_MOVE)
                    else:
                        raise IllegalStateException("Instance of element is not applicable")
                else:
                    if isinstance(self_types[name], MessageElement) \
                            and isinstance(other_types[name], MessageElement):
                        self_types[name].compare(other_types[name], result, compare_types)
                    elif isinstance(self_types[name], EnumElement) \
                            and isinstance(other_types[name], EnumElement):
                        self_types[name].compare(other_types[name], result, compare_types)
                    else:
                        # incompatible type
                        result.add_modification(Modification.TYPE_ALTER)
            result.pop_path(True)

        return result
