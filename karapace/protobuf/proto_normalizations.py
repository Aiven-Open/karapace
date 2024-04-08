"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.typing import StrEnum


class ProtobufNormalisationOptions(StrEnum):
    sort_options = "sort_options"


def normalize_options_ordered(proto_file_element: ProtoFileElement) -> ProtoFileElement:
    sorted_options = (
        None if proto_file_element.options is None else list(sorted(proto_file_element.options, key=lambda x: x.name))
    )
    return ProtoFileElement(
        location=proto_file_element.location,
        package_name=proto_file_element.package_name,
        syntax=proto_file_element.syntax,
        imports=proto_file_element.imports,
        public_imports=proto_file_element.public_imports,
        types=proto_file_element.types,
        services=proto_file_element.services,
        extend_declarations=proto_file_element.extend_declarations,
        options=sorted_options,
    )


# if other normalizations are added we will switch to a more generic approach:
# def normalize_parsed_file(proto_file_element: ProtoFileElement,
# normalization: ProtobufNormalisationOptions) -> ProtoFileElement:
#    if normalization == ProtobufNormalisationOptions.sort_options:
#        return normalize_options_ordered(proto_file_element)
#    else:
#        assert_never(normalization)
