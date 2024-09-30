"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.extend_element import ExtendElement
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.group_element import GroupElement
from karapace.protobuf.known_dependency import KnownDependency
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.protobuf.rpc_element import RpcElement
from karapace.protobuf.schema import ProtobufSchema
from karapace.protobuf.service_element import ServiceElement
from karapace.protobuf.type_element import TypeElement
from karapace.protobuf.type_tree import TypeTree
from karapace.utils import remove_prefix
from typing import Sequence

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

    @staticmethod
    def from_protobuf_schema(protobuf_schema: ProtobufSchema) -> NormalizedProtobufSchema:
        return normalize(protobuf_schema)


class NormalizedOneOfElement(OneOfElement):
    fields: Sequence[NormalizedFieldElement]
    groups: Sequence[NormalizedGroupElement]


def normalize_type_field_element(type_field: FieldElement, package: str, type_tree: TypeTree) -> NormalizedFieldElement:
    sorted_options = None if type_field.options is None else list(sorted(type_field.options, key=sort_by_name))
    field_type_normalized = remove_prefix(remove_prefix(type_field.element_type, "."), f"{package}.")
    reference_in_type_tree = type_tree.type_in_tree(field_type_normalized)
    google_included_type = (
        field_type_normalized in KnownDependency.index_simple or field_type_normalized in KnownDependency.index
    )
    element_type = (
        field_type_normalized
        # we can remove the package from the type only if there aren't clashing names in the same
        # definition with the same relative prefix.
        if (
            reference_in_type_tree is not None
            and reference_in_type_tree.is_uniquely_identifiable_type
            and not google_included_type
        )
        or google_included_type
        else type_field.element_type
    )
    return NormalizedFieldElement(
        location=type_field.location,
        label=type_field.label,
        element_type=element_type,
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


def groups_with_sorted_options(group: GroupElement, package: str, type_tree: TypeTree) -> NormalizedGroupElement:
    sorted_fields = (
        None if group.fields is None else [normalize_type_field_element(field, package, type_tree) for field in group.fields]
    )
    return NormalizedGroupElement(
        label=group.label,
        location=group.location,
        name=group.name,
        tag=group.tag,
        documentation=group.documentation,
        fields=sorted_fields,
    )


def one_ofs_with_sorted_options(one_ofs: OneOfElement, package: str, type_tree: TypeTree) -> NormalizedOneOfElement:
    sorted_options = None if one_ofs.options is None else list(sorted(one_ofs.options, key=sort_by_name))
    sorted_fields = [normalize_type_field_element(field, package, type_tree) for field in one_ofs.fields]
    sorted_groups = [groups_with_sorted_options(group, package, type_tree) for group in one_ofs.groups]

    return NormalizedOneOfElement(
        name=one_ofs.name,
        documentation=one_ofs.documentation,
        fields=sorted_fields,
        groups=sorted_groups,
        options=sorted_options,
    )


def message_element_with_sorted_options(
    message_element: MessageElement, package: str, type_tree: TypeTree
) -> NormalizedMessageElement:
    sorted_options = None if message_element.options is None else list(sorted(message_element.options, key=sort_by_name))
    sorted_nested_types = [
        type_element_with_sorted_options(nested_type, package, type_tree) for nested_type in message_element.nested_types
    ]
    sorted_fields = [normalize_type_field_element(field, package, type_tree) for field in message_element.fields]
    sorted_one_ofs = [one_ofs_with_sorted_options(one_of, package, type_tree) for one_of in message_element.one_ofs]

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


def type_element_with_sorted_options(type_element: TypeElement, package: str, type_tree: TypeTree) -> NormalizedTypeElement:
    sorted_nested_types: list[TypeElement] = []

    for nested_type in type_element.nested_types:
        if isinstance(nested_type, EnumElement):
            sorted_nested_types.append(enum_element_with_sorted_options(nested_type))
        elif isinstance(nested_type, MessageElement):
            sorted_nested_types.append(message_element_with_sorted_options(nested_type, package, type_tree))
        else:
            raise ValueError(f"Unknown type element {type(nested_type)}")  # tried with assert_never but it did not work

    # doing it here since the subtypes do not declare the nested_types property
    type_element.nested_types = sorted_nested_types

    if isinstance(type_element, EnumElement):
        return enum_element_with_sorted_options(type_element)

    if isinstance(type_element, MessageElement):
        return message_element_with_sorted_options(type_element, package, type_tree)

    raise ValueError(f"Unknown type element of type {type(type_element)}")  # tried with assert_never but it did not work


def extends_element_with_sorted_options(
    extend_element: ExtendElement, package: str, type_tree: TypeTree
) -> NormalizedExtendElement:
    sorted_fields = (
        None
        if extend_element.fields is None
        else [normalize_type_field_element(field, package, type_tree) for field in extend_element.fields]
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


def normalize(protobuf_schema: ProtobufSchema) -> NormalizedProtobufSchema:
    proto_file_element = protobuf_schema.proto_file_element
    type_tree = protobuf_schema.types_tree()
    package = proto_file_element.package_name or ""
    sorted_types: Sequence[NormalizedTypeElement] = [
        type_element_with_sorted_options(type_element, package, type_tree) for type_element in proto_file_element.types
    ]
    sorted_options = list(sorted(proto_file_element.options, key=sort_by_name))
    sorted_services: Sequence[NormalizedServiceElement] = [
        service_element_with_sorted_options(service) for service in proto_file_element.services
    ]
    sorted_extend_declarations: Sequence[NormalizedExtendElement] = [
        extends_element_with_sorted_options(extend, package, type_tree) for extend in proto_file_element.extend_declarations
    ]

    normalized_protobuf_element = NormalizedProtoFileElement(
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

    return NormalizedProtobufSchema(
        protobuf_schema.schema,
        protobuf_schema.references,
        protobuf_schema.dependencies,
        normalized_protobuf_element,
    )
