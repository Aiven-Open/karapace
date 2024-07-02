"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.dependency import Dependency
from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.group_element import GroupElement
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import PackageName, ProtoFileElement
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.schema import ProtobufSchema, TypeTree
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.type_element import TypeElement
from karapace.schema_references import Reference
from typing import Mapping, Sequence

import abc


def sort_by_name(element: OptionElement) -> str:
    return element.name


class NormalizedRpcElement(RpcElement):
    pass


class NormalizedServiceElement(ServiceElement):
    rpcs: Sequence[NormalizedRpcElement] | None = None


class NormalizedFieldElement(FieldElement):
    pass


class NormalizedExtendElement(ExtendElement):
    fields: Sequence[NormalizedFieldElement] | None = None


class NormalizedTypeElement(TypeElement, abc.ABC):
    nested_types: Sequence[NormalizedTypeElement]


class NormalizedProtoFileElement(ProtoFileElement):
    types: Sequence[NormalizedTypeElement]
    services: Sequence[NormalizedServiceElement]
    extend_declarations: Sequence[NormalizedExtendElement]


class NormalizedMessageElement(MessageElement, NormalizedTypeElement):
    nested_types: Sequence[NormalizedTypeElement]
    fields: Sequence[NormalizedFieldElement]
    one_ofs: Sequence[OneOfElement]
    groups: Sequence[GroupElement]


class NormalizedEnumConstantElement(EnumConstantElement):
    pass


class NormalizedEnumElement(EnumElement, NormalizedTypeElement):
    constants: Sequence[NormalizedEnumConstantElement]


class NormalizedGroupElement(GroupElement):
    fields: Sequence[NormalizedFieldElement] | None = None


class NormalizedProtobufSchema(ProtobufSchema):
    proto_file_element: NormalizedProtoFileElement

    def __init__(
        self,
        schema: str,
        references: Sequence[Reference] | None = None,
        dependencies: Mapping[str, Dependency] | None = None,
        proto_file_element: ProtoFileElement | None = None,
    ) -> None:
        super().__init__(schema, references, dependencies, proto_file_element)
        self.proto_file_element = normalize(self.proto_file_element, self.types_tree())


class NormalizedOneOfElement(OneOfElement):
    fields: Sequence[NormalizedFieldElement]
    groups: Sequence[NormalizedGroupElement]


def type_field_element_with_sorted_options(type_field: FieldElement) -> NormalizedFieldElement:
    sorted_options = None if type_field.options is None else list(sorted(type_field.options, key=sort_by_name))
    return NormalizedFieldElement(
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


def enum_constant_element_with_sorted_options(enum_constant: EnumConstantElement) -> NormalizedEnumConstantElement:
    sorted_options = None if enum_constant.options is None else list(sorted(enum_constant.options, key=sort_by_name))
    return NormalizedEnumConstantElement(
        location=enum_constant.location,
        name=enum_constant.name,
        tag=enum_constant.tag,
        documentation=enum_constant.documentation,
        options=sorted_options,
    )


def enum_element_with_sorted_options(enum_element: EnumElement) -> NormalizedEnumElement:
    sorted_options = None if enum_element.options is None else list(sorted(enum_element.options, key=sort_by_name))
    constants_with_sorted_options = (
        None
        if enum_element.constants is None
        else [enum_constant_element_with_sorted_options(constant) for constant in enum_element.constants]
    )
    return NormalizedEnumElement(
        location=enum_element.location,
        name=enum_element.name,
        documentation=enum_element.documentation,
        options=sorted_options,
        constants=constants_with_sorted_options,
    )


def groups_with_sorted_options(group: GroupElement) -> NormalizedGroupElement:
    sorted_fields = (
        None if group.fields is None else [type_field_element_with_sorted_options(field) for field in group.fields]
    )
    return NormalizedGroupElement(
        label=group.label,
        location=group.location,
        name=group.name,
        tag=group.tag,
        documentation=group.documentation,
        fields=sorted_fields,
    )


def one_ofs_with_sorted_options(one_ofs: OneOfElement) -> NormalizedOneOfElement:
    sorted_options = None if one_ofs.options is None else list(sorted(one_ofs.options, key=sort_by_name))
    sorted_fields = [type_field_element_with_sorted_options(field) for field in one_ofs.fields]
    sorted_groups = [groups_with_sorted_options(group) for group in one_ofs.groups]

    return NormalizedOneOfElement(
        name=one_ofs.name,
        documentation=one_ofs.documentation,
        fields=sorted_fields,
        groups=sorted_groups,
        options=sorted_options,
    )


def message_element_with_sorted_options(message_element: MessageElement) -> NormalizedMessageElement:
    sorted_options = None if message_element.options is None else list(sorted(message_element.options, key=sort_by_name))
    sorted_nested_types = [type_element_with_sorted_options(nested_type) for nested_type in message_element.nested_types]
    sorted_fields = [type_field_element_with_sorted_options(field) for field in message_element.fields]
    sorted_one_ofs = [one_ofs_with_sorted_options(one_of) for one_of in message_element.one_ofs]

    return NormalizedMessageElement(
        location=message_element.location,
        name=message_element.name,
        documentation=message_element.documentation,
        nested_types=sorted_nested_types,
        options=sorted_options,
        reserveds=message_element.reserveds,
        fields=sorted_fields,
        one_ofs=sorted_one_ofs,
        extensions=message_element.extensions,
        groups=message_element.groups,
    )


def type_element_with_sorted_options(type_element: TypeElement) -> NormalizedTypeElement:
    sorted_nested_types: list[TypeElement] = []

    for nested_type in type_element.nested_types:
        if isinstance(nested_type, EnumElement):
            sorted_nested_types.append(enum_element_with_sorted_options(nested_type))
        elif isinstance(nested_type, MessageElement):
            sorted_nested_types.append(message_element_with_sorted_options(nested_type))
        else:
            raise ValueError(f"Unknown type element {type(nested_type)}")  # tried with assert_never but it did not work

    # doing it here since the subtypes do not declare the nested_types property
    type_element.nested_types = sorted_nested_types

    if isinstance(type_element, EnumElement):
        return enum_element_with_sorted_options(type_element)

    if isinstance(type_element, MessageElement):
        return message_element_with_sorted_options(type_element)

    raise ValueError(f"Unknown type element of type {type(type_element)}")  # tried with assert_never but it did not work


def extends_element_with_sorted_options(extend_element: ExtendElement) -> NormalizedExtendElement:
    sorted_fields = (
        None
        if extend_element.fields is None
        else [type_field_element_with_sorted_options(field) for field in extend_element.fields]
    )
    return NormalizedExtendElement(
        location=extend_element.location,
        name=extend_element.name,
        documentation=extend_element.documentation,
        fields=sorted_fields,
    )


def rpc_element_with_sorted_options(rpc: RpcElement) -> NormalizedRpcElement:
    sorted_options = None if rpc.options is None else list(sorted(rpc.options, key=sort_by_name))
    return NormalizedRpcElement(
        location=rpc.location,
        name=rpc.name,
        documentation=rpc.documentation,
        request_type=rpc.request_type,
        response_type=rpc.response_type,
        request_streaming=rpc.request_streaming,
        response_streaming=rpc.response_streaming,
        options=sorted_options,
    )


def service_element_with_sorted_options(service_element: ServiceElement) -> NormalizedServiceElement:
    sorted_options = None if service_element.options is None else list(sorted(service_element.options, key=sort_by_name))
    sorted_rpc = (
        None if service_element.rpcs is None else [rpc_element_with_sorted_options(rpc) for rpc in service_element.rpcs]
    )

    return NormalizedServiceElement(
        location=service_element.location,
        name=service_element.name,
        documentation=service_element.documentation,
        rpcs=sorted_rpc,
        options=sorted_options,
    )


def normalize(proto_file_element: ProtoFileElement, type_tree: TypeTree) -> NormalizedProtoFileElement:
    full_path_proto_file_element = proto_file_element.with_full_path_expanded(type_tree)
    sorted_types: Sequence[NormalizedTypeElement] = [
        type_element_with_sorted_options(type_element) for type_element in full_path_proto_file_element.types
    ]
    sorted_options = list(sorted(full_path_proto_file_element.options, key=sort_by_name))
    sorted_services: Sequence[NormalizedServiceElement] = [
        service_element_with_sorted_options(service) for service in full_path_proto_file_element.services
    ]
    sorted_extend_declarations: Sequence[NormalizedExtendElement] = [
        extends_element_with_sorted_options(extend) for extend in full_path_proto_file_element.extend_declarations
    ]

    return NormalizedProtoFileElement(
        location=full_path_proto_file_element.location,
        package_name=PackageName(full_path_proto_file_element.package_name),
        syntax=full_path_proto_file_element.syntax,
        imports=full_path_proto_file_element.imports,
        public_imports=full_path_proto_file_element.public_imports,
        types=sorted_types,
        services=sorted_services,
        extend_declarations=sorted_extend_declarations,
        options=sorted_options,
    )
