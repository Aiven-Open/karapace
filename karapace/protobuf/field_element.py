from karapace.protobuf.field import Field
from karapace.protobuf.location import Location
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_type import ProtoType
from karapace.protobuf.utils import append_documentation, append_options


class FieldElement:
    location: Location
    label: Field.Label
    element_type: str
    name: str
    default_value: str = None
    json_name: str = None
    tag: int = 0
    documentation: str = ""
    options: list = []

    def __init__(
        self,
        location: Location,
        label: Field.Label = None,
        element_type: str = None,
        name: str = None,
        default_value: str = None,
        json_name: str = None,
        tag: int = None,
        documentation: str = None,
        options: list = None
    ):
        self.location = location
        self.label = label
        self.element_type = element_type
        self.name = name
        self.default_value = default_value
        self.json_name = json_name
        self.tag = tag
        self.documentation = documentation
        if not options:
            self.options = []
        else:
            self.options = options

    def to_schema(self):
        result: list = list()
        append_documentation(result, self.documentation)

        if self.label:
            result.append(f"{self.label.name.lower()} ")

        result.append(f"{self.element_type} {self.name} = {self.tag}")

        options_with_default = self.options_with_special_values()
        if options_with_default:
            result.append(' ')
            append_options(result, options_with_default)
        result.append(";\n")

        return "".join(result)

    def options_with_special_values(self) -> list:
        """ Both `default` and `json_name` are defined in the schema like options but they are actually
        not options themselves as they're missing from `google.protobuf.FieldOptions`.
        """

        options = self.options.copy()

        if self.default_value:
            proto_type: ProtoType = ProtoType.get2(self.element_type)
            options.append(OptionElement("default", proto_type.to_kind(), self.default_value, False))
        if self.json_name:
            self.options.append(OptionElement("json_name", OptionElement.Kind.STRING, self.json_name, False))

        return options


# Only non-repeated scalar types and Enums support default values.
