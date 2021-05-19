from karapace.protobuf.location import Location


class TypeElement:
    location: Location
    name: str
    documentation: str
    options: list
    nested_types: list

    def to_schema(self) -> str:
        pass

    def __repr__(self):
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"

    def __str__(self):
        mytype = type(self)
        return f"{mytype}({self.to_schema()})"
