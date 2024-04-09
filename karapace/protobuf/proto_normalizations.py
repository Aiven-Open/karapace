"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.group_element import GroupElement
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.type_element import TypeElement
from karapace.typing import StrEnum
from typing import List


class ProtobufNormalisationOptions(StrEnum):
    sort_options = "sort_options"


def sort_by_name(element: OptionElement) -> str:
    return element.name


def type_field_element_with_sorted_options(type_field: FieldElement) -> FieldElement:
    sorted_options = None if type_field.options is None else list(sorted(type_field.options, key=sort_by_name))
    return FieldElement(
        location=type_field.location,
        label=type_field.label,
        element_type=type_field.element_type,
        name=type_field.name,
        default_value=type_field.default_value,
        json_name=type_field.json_name,
        tag=type_field.tag,
        documentation=type_field.documentation,
        options=sorted_options,
    )


def enum_constant_element_with_sorted_options(enum_constant: EnumConstantElement) -> EnumConstantElement:
    sorted_options = None if enum_constant.options is None else list(sorted(enum_constant.options, key=sort_by_name))
    return EnumConstantElement(
        location=enum_constant.location,
        name=enum_constant.name,
        tag=enum_constant.tag,
        documentation=enum_constant.documentation,
        options=sorted_options,
    )


def enum_element_with_sorted_options(enum_element: EnumElement) -> EnumElement:
    sorted_options = None if enum_element.options is None else list(sorted(enum_element.options, key=sort_by_name))
    constants_with_sorted_options = (
        None
        if enum_element.constants is None
        else [enum_constant_element_with_sorted_options(constant) for constant in enum_element.constants]
    )
    return EnumElement(
        location=enum_element.location,
        name=enum_element.name,
        documentation=enum_element.documentation,
        options=sorted_options,
        constants=constants_with_sorted_options,
    )


def groups_with_sorted_options(group: GroupElement) -> GroupElement:
    sorted_fields = (
        None if group.fields is None else [type_field_element_with_sorted_options(field) for field in group.fields]
    )
    return GroupElement(
        label=group.label,
        location=group.location,
        name=group.name,
        tag=group.tag,
        documentation=group.documentation,
        fields=sorted_fields,
    )


def one_ofs_with_sorted_options(one_ofs: OneOfElement) -> OneOfElement:
    sorted_options = None if one_ofs.options is None else list(sorted(one_ofs.options, key=sort_by_name))
    sorted_fields = [type_field_element_with_sorted_options(field) for field in one_ofs.fields]
    sorted_groups = [groups_with_sorted_options(group) for group in one_ofs.groups]

    return OneOfElement(
        name=one_ofs.name,
        documentation=one_ofs.documentation,
        fields=sorted_fields,
        groups=sorted_groups,
        options=sorted_options,
    )


def message_element_with_sorted_options(message_element: MessageElement) -> MessageElement:
    sorted_options = None if message_element.options is None else list(sorted(message_element.options, key=sort_by_name))
    sorted_neasted_types = [type_element_with_sorted_options(nested_type) for nested_type in message_element.nested_types]
    sorted_fields = [type_field_element_with_sorted_options(field) for field in message_element.fields]
    sorted_one_ofs = [one_ofs_with_sorted_options(one_of) for one_of in message_element.one_ofs]

    return MessageElement(
        location=message_element.location,
        name=message_element.name,
        documentation=message_element.documentation,
        nested_types=sorted_neasted_types,
        options=sorted_options,
        reserveds=message_element.reserveds,
        fields=sorted_fields,
        one_ofs=sorted_one_ofs,
        extensions=message_element.extensions,
        groups=message_element.groups,
    )


def type_element_with_sorted_options(type_element: TypeElement) -> TypeElement:
    sorted_neasted_types: List[TypeElement] = []

    for nested_type in type_element.nested_types:
        if isinstance(nested_type, EnumElement):
            sorted_neasted_types.append(enum_element_with_sorted_options(nested_type))
        elif isinstance(nested_type, MessageElement):
            sorted_neasted_types.append(message_element_with_sorted_options(nested_type))
        else:
            raise ValueError("Unknown type element")  # tried with assert_never but it did not work

    # doing it here since the subtypes do not declare the nested_types property
    type_element.nested_types = sorted_neasted_types

    if isinstance(type_element, EnumElement):
        return enum_element_with_sorted_options(type_element)

    if isinstance(type_element, MessageElement):
        return message_element_with_sorted_options(type_element)

    raise ValueError("Unknown type element")  # tried with assert_never but it did not work


def extends_element_with_sorted_options(extend_element: ExtendElement) -> ExtendElement:
    sorted_fields = (
        None
        if extend_element.fields is None
        else [type_field_element_with_sorted_options(field) for field in extend_element.fields]
    )
    return ExtendElement(
        location=extend_element.location,
        name=extend_element.name,
        documentation=extend_element.documentation,
        fields=sorted_fields,
    )


def rpc_element_with_sorted_options(rpc: RpcElement) -> RpcElement:
    sorted_options = None if rpc.options is None else list(sorted(rpc.options, key=sort_by_name))
    return RpcElement(
        location=rpc.location,
        name=rpc.name,
        documentation=rpc.documentation,
        request_type=rpc.request_type,
        response_type=rpc.response_type,
        request_streaming=rpc.request_streaming,
        response_streaming=rpc.response_streaming,
        options=sorted_options,
    )


def service_element_with_sorted_options(service_element: ServiceElement) -> ServiceElement:
    sorted_options = None if service_element.options is None else list(sorted(service_element.options, key=sort_by_name))
    sorted_rpc = (
        None if service_element.rpcs is None else [rpc_element_with_sorted_options(rpc) for rpc in service_element.rpcs]
    )

    return ServiceElement(
        location=service_element.location,
        name=service_element.name,
        documentation=service_element.documentation,
        rpcs=sorted_rpc,
        options=sorted_options,
    )


def normalize_options_ordered(proto_file_element: ProtoFileElement) -> ProtoFileElement:
    sorted_types = [type_element_with_sorted_options(type_element) for type_element in proto_file_element.types]
    sorted_options = (
        None if proto_file_element.options is None else list(sorted(proto_file_element.options, key=sort_by_name))
    )
    sorted_services = (
        None
        if proto_file_element.services is None
        else [service_element_with_sorted_options(service) for service in proto_file_element.services]
    )
    sorted_extend_declarations = (
        None
        if proto_file_element.extend_declarations is None
        else [extends_element_with_sorted_options(extend) for extend in proto_file_element.extend_declarations]
    )

    return ProtoFileElement(
        location=proto_file_element.location,
        package_name=proto_file_element.package_name,
        syntax=proto_file_element.syntax,
        imports=proto_file_element.imports,
        public_imports=proto_file_element.public_imports,
        types=sorted_types,
        services=sorted_services,
        extend_declarations=sorted_extend_declarations,
        options=sorted_options,
    )


# if other normalizations are added we will switch to a more generic approach:
# def normalize_parsed_file(proto_file_element: ProtoFileElement,
# normalization: ProtobufNormalisationOptions) -> ProtoFileElement:
#    if normalization == ProtobufNormalisationOptions.sort_options:
#        return normalize_options_ordered(proto_file_element)
#    else:
#        assert_never(normalization)
