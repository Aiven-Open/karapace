# -*- coding:utf-8 -*-
"""
This module provide a small Python library for creating dicts from protocol buffers
Module based on code :
https://github.com/wearefair/protobuf-to-dict
LICENSE: https://github.com/wearefair/protobuf-to-dict/blob/master/LICENSE
"""

from dateutil.parser import parse as date_parser
from frozendict import frozendict
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp

import datetime
import six

__all__ = ["protobuf_to_dict", "TYPE_CALLABLE_MAP", "dict_to_protobuf", "REVERSE_TYPE_CALLABLE_MAP"]

Timestamp_type_name = 'Timestamp'


def datetime_to_timestamp(dt):
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def timestamp_to_datetime(ts):
    dt = ts.ToDatetime()
    return dt


EXTENSION_CONTAINER = '___X'

TYPE_CALLABLE_MAP = frozendict({
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_INT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_UINT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_SINT32: int,
    FieldDescriptor.TYPE_SINT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_FIXED32: int,
    FieldDescriptor.TYPE_FIXED64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_SFIXED32: int,
    FieldDescriptor.TYPE_SFIXED64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_BOOL: bool,
    FieldDescriptor.TYPE_STRING: six.text_type,
    FieldDescriptor.TYPE_BYTES: six.binary_type,
    FieldDescriptor.TYPE_ENUM: int,
})


def repeated(type_callable):
    return lambda value_list: [type_callable(value) for value in value_list]


def enum_label_name(field, value, lowercase_enum_lables=False) -> str:
    label = field.enum_type.values_by_number[int(value)].name
    label = label.lower() if lowercase_enum_lables else label
    return label


def _is_map_entry(field) -> bool:
    return (
        field.type == FieldDescriptor.TYPE_MESSAGE and field.message_type.has_options
        and field.message_type.GetOptions().map_entry
    )


def protobuf_to_dict(
    pb,
    type_callable_map=TYPE_CALLABLE_MAP,
    use_enum_labels=False,
    including_default_value_fields=False,
    lowercase_enum_lables=False
) -> dict:
    result_dict = {}
    extensions = {}
    for field, value in pb.ListFields():
        if field.message_type and field.message_type.has_options and field.message_type.GetOptions().map_entry:
            result_dict[field.name] = dict()
            value_field = field.message_type.fields_by_name['value']
            type_callable = _get_field_value_adaptor(
                pb, value_field, type_callable_map, use_enum_labels, including_default_value_fields, lowercase_enum_lables
            )
            for k, v in value.items():
                result_dict[field.name][k] = type_callable(v)
            continue
        type_callable = _get_field_value_adaptor(
            pb, field, type_callable_map, use_enum_labels, including_default_value_fields, lowercase_enum_lables
        )
        if field.label == FieldDescriptor.LABEL_REPEATED:
            type_callable = repeated(type_callable)

        if field.is_extension:
            extensions[str(field.number)] = type_callable(value)
            continue

        result_dict[field.name] = type_callable(value)

    # Serialize default value if including_default_value_fields is True.
    if including_default_value_fields:
        for field in pb.DESCRIPTOR.fields:
            # Singular message fields and oneof fields will not be affected.
            if ((field.label != FieldDescriptor.LABEL_REPEATED and field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE)
                or field.containing_oneof):
                continue

            if field.name in result_dict:
                # Skip the field which has been serailized already.
                continue
            if _is_map_entry(field):
                result_dict[field.name] = {}
            elif field.label == FieldDescriptor.LABEL_REPEATED:
                result_dict[field.name] = []
            elif field.type == FieldDescriptor.TYPE_ENUM and use_enum_labels:
                result_dict[field.name] = enum_label_name(field, field.default_value, lowercase_enum_lables)
            else:
                result_dict[field.name] = field.default_value

    if extensions:
        result_dict[EXTENSION_CONTAINER] = extensions
    return result_dict


def _get_field_value_adaptor(
    pb,
    field,
    type_callable_map=TYPE_CALLABLE_MAP,
    use_enum_labels=False,
    including_default_value_fields=False,
    lowercase_enum_lables=False
):
    if field.message_type and field.message_type.name == Timestamp_type_name:
        return timestamp_to_datetime
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        # recursively encode protobuf sub-message
        return lambda pb: protobuf_to_dict(
            pb,
            type_callable_map=type_callable_map,
            use_enum_labels=use_enum_labels,
            including_default_value_fields=including_default_value_fields,
            lowercase_enum_lables=lowercase_enum_lables,
        )

    if use_enum_labels and field.type == FieldDescriptor.TYPE_ENUM:
        return lambda value: enum_label_name(field, value, lowercase_enum_lables)

    if field.type in type_callable_map:
        return type_callable_map[field.type]

    raise TypeError("Field %s.%s has unrecognised type id %d" % (pb.__class__.__name__, field.name, field.type))


REVERSE_TYPE_CALLABLE_MAP = frozendict({})


def dict_to_protobuf(
    pb_klass_or_instance,
    values,
    type_callable_map=REVERSE_TYPE_CALLABLE_MAP,
    strict=True,
    ignore_none=False,
    use_date_parser_for_fields=None
) -> object:
    """Populates a protobuf model from a dictionary.

    :param ignore_none:
    :param pb_klass_or_instance: a protobuf message class, or an protobuf instance
    :type pb_klass_or_instance: a type or instance of a subclass of google.protobuf.message.Message
    :param dict values: a dictionary of values. Repeated and nested values are
       fully supported.
    :param dict type_callable_map: a mapping of protobuf types to callables for setting
       values on the target instance.
    :param bool strict: complain if keys in the map are not fields on the message.
    :param bool strict: ignore None-values of fields, treat them as empty field
    :param bool strict: when false: accept enums both in lowercase and uppercase
    :param list use_date_parser_for_fields: a list of fields that need to use date_parser
    """
    if isinstance(pb_klass_or_instance, Message):
        instance = pb_klass_or_instance
    else:
        instance = pb_klass_or_instance()
    return _dict_to_protobuf(instance, values, type_callable_map, strict, ignore_none, use_date_parser_for_fields)


def _get_field_mapping(pb, dict_value, strict):
    field_mapping = []
    key: str = ""
    for key, value in dict_value.items():
        if key == EXTENSION_CONTAINER:
            continue
        if key not in pb.DESCRIPTOR.fields_by_name:
            if strict:
                raise KeyError("%s does not have a field called %s" % (type(pb), key))
            continue
        field_mapping.append((pb.DESCRIPTOR.fields_by_name[key], value, getattr(pb, key, None)))

    for ext_num, ext_val in dict_value.get(EXTENSION_CONTAINER, {}).items():
        try:
            ext_num = int(ext_num)
        except ValueError:
            raise ValueError("Extension keys must be integers.")
        # pylint: disable=W0212
        if ext_num not in pb._extensions_by_number:
            if strict:
                raise KeyError("%s does not have a extension with number %s. Perhaps you forgot to import it?" % (pb, key))
            continue
        # pylint: disable=W0212

        ext_field = pb._extensions_by_number[ext_num]
        # noinspection PyUnusedLocal
        pb_val = None
        pb_val = pb.Extensions[ext_field]
        field_mapping.append((ext_field, ext_val, pb_val))

    return field_mapping


def _dict_to_protobuf(pb, value, type_callable_map, strict, ignore_none, use_date_parser_for_fields):
    fields = _get_field_mapping(pb, value, strict)

    for field, input_value, pb_value in fields:
        if ignore_none and input_value is None:
            continue

        if field.label == FieldDescriptor.LABEL_REPEATED:
            if field.message_type and field.message_type.has_options and field.message_type.GetOptions().map_entry:
                key_field = field.message_type.fields_by_name['key']
                value_field = field.message_type.fields_by_name['value']
                for key, value in input_value.items():
                    if value_field.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
                        _dict_to_protobuf(
                            getattr(pb, field.name)[key], value, type_callable_map, strict, ignore_none,
                            use_date_parser_for_fields
                        )
                    else:
                        if ignore_none and value is None:
                            continue
                        try:
                            if key_field.type in type_callable_map:
                                key = type_callable_map[key_field.type](key)
                            if value_field.type in type_callable_map:
                                value = type_callable_map[value_field.type](value)
                            getattr(pb, field.name)[key] = value
                        except Exception as exc:
                            raise RuntimeError(f"type: {type(pb)}, field: {field.name}, value: {value}") from exc
                continue
            for item in input_value:
                if field.type == FieldDescriptor.TYPE_MESSAGE:
                    m = pb_value.add()
                    _dict_to_protobuf(m, item, type_callable_map, strict, ignore_none, use_date_parser_for_fields)
                elif field.type == FieldDescriptor.TYPE_ENUM and isinstance(item, six.string_types):
                    pb_value.append(_string_to_enum(field, item, strict))
                else:
                    pb_value.append(item)
            continue

        if isinstance(input_value, datetime.datetime):
            input_value = datetime_to_timestamp(input_value)
            # Instead of setattr we need to use CopyFrom for composite fields
            # Otherwise we will get AttributeError:
            #   Assignment not allowed to composite field “field name” in protocol message object
            getattr(pb, field.name).CopyFrom(input_value)
            continue
        elif use_date_parser_for_fields and field.name in use_date_parser_for_fields:
            input_value = datetime_to_timestamp(date_parser(input_value))
            getattr(pb, field.name).CopyFrom(input_value)
            continue
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            _dict_to_protobuf(pb_value, input_value, type_callable_map, strict, ignore_none, use_date_parser_for_fields)
            continue

        if field.type in type_callable_map:
            input_value = type_callable_map[field.type](input_value)

        if field.is_extension:
            pb.Extensions[field] = input_value
            continue

        if field.type == FieldDescriptor.TYPE_ENUM and isinstance(input_value, six.string_types):
            input_value = _string_to_enum(field, input_value, strict)

        try:
            setattr(pb, field.name, input_value)
        except Exception as exc:
            raise RuntimeError(f"type: {type(pb)}, field: {field.name}, value: {value}") from exc

    return pb


def _string_to_enum(field, input_value, strict=False):
    try:
        input_value = field.enum_type.values_by_name[input_value].number
    except KeyError:
        if strict:
            raise KeyError("`%s` is not a valid value for field `%s`" % (input_value, field.name))
        return _string_to_enum(field, input_value.upper(), strict=True)
    return input_value


def get_field_names_and_options(pb):
    """
    Return a tuple of field names and options.
    """
    desc = pb.DESCRIPTOR

    for field in desc.fields:
        field_name = field.name
        options_dict = {}
        if field.has_options:
            options = field.GetOptions()
            for subfield, value in options.ListFields():
                options_dict[subfield.name] = value
        yield field, field_name, options_dict


class FieldsMissing(ValueError):
    pass


def validate_dict_for_required_pb_fields(pb, dic):
    """
    Validate that the dictionary has all the required fields for creating a protobuffer object
    from pb class. If a field is missing, raise FieldsMissing.
    In order to mark a field as optional, add [(is_optional) = true] to the field.
    Take a look at the tests for an example.
    """
    missing_fields = []
    for field, field_name, field_options in get_field_names_and_options(pb):
        if not field_options.get('is_optional', False) and field_name not in dic:
            missing_fields.append(field_name)
    if missing_fields:
        raise FieldsMissing('Missing fields: {}'.format(', '.join(missing_fields)))
