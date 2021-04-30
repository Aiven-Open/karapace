from karapace.protobuf.location import Location


class TypeElement:
    location: Location
    name: str
    documentation: str
    options: list
    nested_types: list

    def to_schema(self) -> str:
        pass
