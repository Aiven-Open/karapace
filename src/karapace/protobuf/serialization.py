"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Sequence
from karapace.errors import InvalidSchema
from karapace.protobuf.enum_constant_element import EnumConstantElement
from karapace.protobuf.enum_element import EnumElement
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.kotlin_wrapper import KotlinRange
from karapace.protobuf.location import DEFAULT_LOCATION
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement
from karapace.protobuf.proto_file_element import PackageName, ProtoFileElement, TypeName
from karapace.protobuf.reserved_element import ReservedElement
from karapace.protobuf.syntax import Syntax
from karapace.protobuf.type_element import TypeElement
from types import MappingProxyType
from typing import Any

import base64
import google.protobuf.descriptor
import google.protobuf.descriptor_pb2

_TYPE_MAP = MappingProxyType(
    {
        google.protobuf.descriptor.FieldDescriptor.TYPE_DOUBLE: "double",
        google.protobuf.descriptor.FieldDescriptor.TYPE_FLOAT: "float",
        google.protobuf.descriptor.FieldDescriptor.TYPE_INT32: "int32",
        google.protobuf.descriptor.FieldDescriptor.TYPE_INT64: "int64",
        google.protobuf.descriptor.FieldDescriptor.TYPE_UINT32: "uint32",
        google.protobuf.descriptor.FieldDescriptor.TYPE_UINT64: "uint64",
        google.protobuf.descriptor.FieldDescriptor.TYPE_SINT32: "sint32",
        google.protobuf.descriptor.FieldDescriptor.TYPE_SINT64: "sint64",
        google.protobuf.descriptor.FieldDescriptor.TYPE_FIXED32: "fixed32",
        google.protobuf.descriptor.FieldDescriptor.TYPE_FIXED64: "fixed64",
        google.protobuf.descriptor.FieldDescriptor.TYPE_SFIXED32: "sfixed32",
        google.protobuf.descriptor.FieldDescriptor.TYPE_SFIXED64: "sfixed64",
        google.protobuf.descriptor.FieldDescriptor.TYPE_BOOL: "bool",
        google.protobuf.descriptor.FieldDescriptor.TYPE_STRING: "string",
        google.protobuf.descriptor.FieldDescriptor.TYPE_BYTES: "bytes",
    }
)
_REVERSE_TYPE_MAP = MappingProxyType({v: k for k, v in _TYPE_MAP.items()})


def _deserialize_field(field: Any) -> FieldElement:
    label = None
    if (field.HasField("proto3_optional") and field.proto3_optional) or Field.Label(field.label) != Field.Label.OPTIONAL:
        label = Field.Label(field.label)
    if field.HasField("type_name"):
        element_type = field.type_name
    else:
        if not field.HasField("type"):
            raise InvalidSchema(f"field {field.name} has no type_name nor type")
        if field.type not in _TYPE_MAP:
            raise NotImplementedError(f"Unsupported field type {field.type}")
        element_type = _TYPE_MAP[field.type]
    return FieldElement(DEFAULT_LOCATION, label=label, element_type=element_type, name=field.name, tag=field.number)


def _deserialize_enum(enumtype: Any) -> EnumElement:
    constants: list[EnumConstantElement] = []
    for c in enumtype.value:
        options: list[OptionElement] = list()
        if c.options.deprecated:
            options.append(OptionElement("deprecated", OptionElement.Kind.BOOLEAN, "true"))
        constants.append(EnumConstantElement(DEFAULT_LOCATION, c.name, c.number, "", options))
    return EnumElement(DEFAULT_LOCATION, enumtype.name, "", None, constants)


def _deserialize_msg(msgtype: Any) -> MessageElement:
    reserved_values: list[str | int | KotlinRange] = []
    reserveds: list[ReservedElement] = []
    nested_types: list[TypeElement] = []
    fields: list[FieldElement] = []

    for reserved in msgtype.reserved_range:
        if reserved.end == reserved.start + 1:
            reserved_values.append(reserved.start)
        else:
            reserved_values.append(KotlinRange(reserved.start, reserved.end - 1))
    reserved_values += msgtype.reserved_name
    if len(reserved_values) > 0:
        reserveds.append(ReservedElement(location=DEFAULT_LOCATION, values=reserved_values))

    for nested in msgtype.nested_type:
        nested_types.append(_deserialize_msg(nested))
    for nested_enum in msgtype.enum_type:
        nested_types.append(_deserialize_enum(nested_enum))

    one_ofs: list[OneOfElement | None] = [OneOfElement(oneof.name) for oneof in msgtype.oneof_decl]

    for f in msgtype.field:
        sf = _deserialize_field(f)
        is_oneof = f.HasField("oneof_index")
        is_proto3_optional = f.HasField("oneof_index") and f.HasField("proto3_optional") and f.proto3_optional
        if is_proto3_optional:
            # Every proto3 optional field is placed into a one-field oneof, called a "synthetic" oneof,
            # as it was not present in the source .proto file.
            # This will make sure that we don't interpret those optionals as oneof.
            one_ofs[f.oneof_index] = None
            fields.append(sf)
        elif is_oneof:
            one_ofs[f.oneof_index].fields.append(sf)
        else:
            fields.append(sf)

    one_ofs_filtered: list[OneOfElement] = [oneof for oneof in one_ofs if oneof is not None]
    return MessageElement(
        DEFAULT_LOCATION,
        msgtype.name,
        nested_types=nested_types,
        reserveds=reserveds,
        fields=fields,
        one_ofs=one_ofs_filtered,
    )


def _deserialize_options(options: Any) -> list[OptionElement]:
    result: list[OptionElement] = []
    if options.HasField("java_package"):
        result.append(OptionElement("java_package", OptionElement.Kind.STRING, options.java_package))
    if options.HasField("java_outer_classname"):
        result.append(OptionElement("java_outer_classname", OptionElement.Kind.STRING, options.java_outer_classname))
    if options.HasField("optimize_for"):
        result.append(OptionElement("optimize_for", OptionElement.Kind.ENUM, options.optimize_for))
    if options.HasField("java_multiple_files"):
        result.append(OptionElement("java_multiple_files", OptionElement.Kind.BOOLEAN, options.java_multiple_files))
    if options.HasField("go_package"):
        result.append(OptionElement("go_package", OptionElement.Kind.STRING, options.go_package))
    if options.HasField("cc_generic_services"):
        result.append(OptionElement("cc_generic_services", OptionElement.Kind.BOOLEAN, options.cc_generic_services))
    if options.HasField("java_generic_services"):
        result.append(OptionElement("java_generic_services", OptionElement.Kind.BOOLEAN, options.java_generic_services))
    if options.HasField("py_generic_services"):
        result.append(OptionElement("py_generic_services", OptionElement.Kind.BOOLEAN, options.py_generic_services))
    if options.HasField("java_generate_equals_and_hash"):
        result.append(
            OptionElement("java_generate_equals_and_hash", OptionElement.Kind.BOOLEAN, options.java_generate_equals_and_hash)
        )
    if options.HasField("deprecated"):
        result.append(OptionElement("deprecated", OptionElement.Kind.BOOLEAN, options.deprecated))
    if options.HasField("java_string_check_utf8"):
        result.append(OptionElement("java_string_check_utf8", OptionElement.Kind.BOOLEAN, options.java_string_check_utf8))
    if options.HasField("cc_enable_arenas"):
        result.append(OptionElement("cc_enable_arenas", OptionElement.Kind.BOOLEAN, options.cc_enable_arenas))
    if options.HasField("objc_class_prefix"):
        result.append(OptionElement("objc_class_prefix", OptionElement.Kind.STRING, options.objc_class_prefix))
    if options.HasField("csharp_namespace"):
        result.append(OptionElement("csharp_namespace", OptionElement.Kind.STRING, options.csharp_namespace))
    if options.HasField("swift_prefix"):
        result.append(OptionElement("swift_prefix", OptionElement.Kind.STRING, options.swift_prefix))
    if options.HasField("php_class_prefix"):
        result.append(OptionElement("php_class_prefix", OptionElement.Kind.STRING, options.php_class_prefix))
    if options.HasField("php_namespace"):
        result.append(OptionElement("php_namespace", OptionElement.Kind.STRING, options.php_namespace))
    if options.HasField("php_generic_services"):
        result.append(OptionElement("php_generic_services", OptionElement.Kind.BOOLEAN, options.php_generic_services))
    if options.HasField("php_metadata_namespace"):
        result.append(OptionElement("php_metadata_namespace", OptionElement.Kind.STRING, options.php_metadata_namespace))
    if options.HasField("ruby_package"):
        result.append(OptionElement("ruby_package", OptionElement.Kind.STRING, options.ruby_package))
    return result


def deserialize(schema_b64: str) -> ProtoFileElement:
    serialized_pb = base64.b64decode(schema_b64, validate=True)
    proto = google.protobuf.descriptor_pb2.FileDescriptorProto()
    proto.ParseFromString(serialized_pb)
    imports: list[TypeName] = []
    public_imports: list[TypeName] = []
    for index, dep in enumerate(proto.dependency):
        if index in proto.public_dependency:
            public_imports.append(TypeName(dep))
        else:
            imports.append(TypeName(dep))
    types: list[TypeElement] = []
    for enumtype in proto.enum_type:
        types.append(_deserialize_enum(enumtype))
    for msgtype in proto.message_type:
        types.append(_deserialize_msg(msgtype))
    options: list[OptionElement] = _deserialize_options(proto.options)
    syntax = None
    if proto.syntax:
        syntax = Syntax(proto.syntax)

    return ProtoFileElement(
        DEFAULT_LOCATION,
        package_name=PackageName(proto.package),
        syntax=syntax,
        imports=imports,
        public_imports=public_imports,
        types=types,
        options=options,
    )


_LABEL_MAP = MappingProxyType(
    {
        Field.Label.OPTIONAL: google.protobuf.descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        Field.Label.REQUIRED: google.protobuf.descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED,
        Field.Label.REPEATED: google.protobuf.descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED,
    }
)


def _serialize_field_label(label: Field.Label) -> google.protobuf.descriptor_pb2.FieldDescriptorProto.Label.ValueType:
    if label not in _LABEL_MAP:
        raise NotImplementedError(f"Unsupported field label {label}")
    return _LABEL_MAP[label]


def _serialize_field(field: FieldElement) -> google.protobuf.descriptor_pb2.FieldDescriptorProto:
    d = google.protobuf.descriptor_pb2.FieldDescriptorProto()
    if field.label is not None:
        d.label = _serialize_field_label(field.label)
        if field.label == Field.Label.OPTIONAL:
            d.proto3_optional = True
    else:
        d.label = _serialize_field_label(Field.Label.OPTIONAL)
    if field.element_type in _REVERSE_TYPE_MAP:
        d.type = _REVERSE_TYPE_MAP[field.element_type]
    else:
        d.type_name = field.element_type
    if field.name is not None:
        d.name = field.name
    if field.tag is not None:
        d.number = field.tag
    return d


def _serialize_enumtype(e: EnumElement) -> google.protobuf.descriptor_pb2.EnumDescriptorProto:
    result = google.protobuf.descriptor_pb2.EnumDescriptorProto()
    result.name = e.name
    for c in e.constants:
        c2 = google.protobuf.descriptor_pb2.EnumValueDescriptorProto()
        c2.name = c.name
        c2.number = c.tag
        c2.options.deprecated = any(o.name == "deprecated" and o.value == "true" for o in c.options)
        result.value.append(c2)
    return result


def _serialize_msgtype(t: MessageElement) -> google.protobuf.descriptor_pb2.DescriptorProto:
    d = google.protobuf.descriptor_pb2.DescriptorProto()
    d.name = t.name
    for nt in t.nested_types:
        if isinstance(nt, MessageElement):
            d.nested_type.append(_serialize_msgtype(nt))
        elif isinstance(nt, EnumElement):
            d.enum_type.append(_serialize_enumtype(nt))
        else:
            raise NotImplementedError(f"Unsupported nested type of {nt}")
    for r in t.reserveds:
        for v in r.values:
            if isinstance(v, int):
                rr = google.protobuf.descriptor_pb2.DescriptorProto.ReservedRange()
                rr.start = v
                rr.end = v + 1
                d.reserved_range.append(rr)
            elif isinstance(v, KotlinRange):
                rr = google.protobuf.descriptor_pb2.DescriptorProto.ReservedRange()
                rr.start = v.minimum
                rr.end = v.maximum + 1
                d.reserved_range.append(rr)
            elif isinstance(v, str):
                d.reserved_name.append(v)
    for field in t.fields:
        d.field.append(_serialize_field(field))
    for oneof in t.one_ofs:
        oneof2 = google.protobuf.descriptor_pb2.OneofDescriptorProto()
        oneof2.name = oneof.name
        oneof_index = len(d.oneof_decl)
        d.oneof_decl.append(oneof2)
        for field in oneof.fields:
            sf = _serialize_field(field)
            sf.oneof_index = oneof_index
            d.field.append(sf)
    return d


def _serialize_options(options: Sequence[OptionElement], result: google.protobuf.descriptor_pb2.FileOptions) -> None:
    for opt in options:
        if opt.name == ("java_package"):
            result.java_package = opt.value
        if opt.name == ("java_outer_classname"):
            result.java_outer_classname = opt.value
        if opt.name == ("optimize_for"):
            result.optimize_for = opt.value
        if opt.name == ("java_multiple_files"):
            result.java_multiple_files = opt.value
        if opt.name == ("go_package"):
            result.go_package = opt.value
        if opt.name == ("cc_generic_services"):
            result.cc_generic_services = opt.value
        if opt.name == ("java_generic_services"):
            result.java_generic_services = opt.value
        if opt.name == ("py_generic_services"):
            result.py_generic_services = opt.value
        if opt.name == ("java_generate_equals_and_hash"):
            result.java_generate_equals_and_hash = opt.value
        if opt.name == ("deprecated"):
            result.deprecated = opt.value
        if opt.name == ("java_string_check_utf8"):
            result.java_string_check_utf8 = opt.value
        if opt.name == ("cc_enable_arenas"):
            result.cc_enable_arenas = opt.value
        if opt.name == ("objc_class_prefix"):
            result.objc_class_prefix = opt.value
        if opt.name == ("csharp_namespace"):
            result.csharp_namespace = opt.value
        if opt.name == ("swift_prefix"):
            result.swift_prefix = opt.value
        if opt.name == ("php_class_prefix"):
            result.php_class_prefix = opt.value
        if opt.name == ("php_namespace"):
            result.php_namespace = opt.value
        if opt.name == ("php_generic_services"):
            result.php_generic_services = opt.value
        if opt.name == ("php_metadata_namespace"):
            result.php_metadata_namespace = opt.value
        if opt.name == ("ruby_package"):
            result.ruby_package = opt.value


def serialize(schema: ProtoFileElement) -> str:
    fd = google.protobuf.descriptor_pb2.FileDescriptorProto()
    if schema.syntax is not None:
        fd.syntax = schema.syntax.value
    if schema.package_name is not None:
        fd.package = schema.package_name
    if schema.options is not None:
        _serialize_options(schema.options, fd.options)

    for index, dep in enumerate(schema.public_imports):
        fd.dependency.append(str(dep))
        fd.public_dependency.append(index)
    for dep in schema.imports:
        fd.dependency.append(str(dep))

    for t in schema.types:
        if isinstance(t, MessageElement):
            fd.message_type.append(_serialize_msgtype(t))
        elif isinstance(t, EnumElement):
            fd.enum_type.append(_serialize_enumtype(t))
    return base64.b64encode(fd.SerializeToString()).decode("utf-8")
