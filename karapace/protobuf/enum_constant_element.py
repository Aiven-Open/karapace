from karapace.protobuf.location import Location
from karapace.protobuf.utils import append_documentation, append_options


class EnumConstantElement:
    location: Location
    name: str
    tag: int
    documentation: str
    options: list = []

    def __init__(
        self,
        location: Location,
        name: str,
        tag: int,
        documentation: str,
        options: list,
    ):
        self.location = location
        self.name = name

        self.tag = tag
        self.options = options
        if not documentation:
            self.documentation = ""
        else:
            self.documentation = documentation

    def to_schema(self) -> str:
        result: list = list()
        append_documentation(result, self.documentation)
        result.append(f"{self.name} = {self.tag}")
        if self.options:
            result.append(" ")
            append_options(result, self.options)
        result.append(";\n")
        return "".join(result)
