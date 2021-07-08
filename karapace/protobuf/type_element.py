# Ported from square/wire:
# wire-library/wire-schema/src/commonMain/kotlin/com/squareup/wire/schema/internal/parser/TypeElement.kt

from karapace.protobuf.location import Location


class TypeElement:
    def __init__(self, location: Location, name: str, documentation: str, options: list, nested_types: list):

        self.location: Location = location
        self.name: str = name
        self.documentation: str = documentation
        self.options: list = options
        self.nested_types: list = nested_types

    def to_schema(self) -> str:
        pass

    def __repr__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"

    def __str__(self) -> str:
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"
