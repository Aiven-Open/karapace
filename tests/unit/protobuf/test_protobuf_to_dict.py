"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.timestamp_pb2 import Timestamp
from karapace.core.protobuf.protobuf_to_dict import (
    EXTENSION_CONTAINER,
    FieldsMissing,
    datetime_to_timestamp,
    dict_to_protobuf,
    enum_label_name,
    get_field_names_and_options,
    protobuf_to_dict,
    timestamp_to_datetime,
    validate_dict_for_required_pb_fields,
)

import datetime
import pytest


_POOL = descriptor_pool.DescriptorPool()


_FACTORY = message_factory.MessageFactory(pool=_POOL)


def _build_message_class(file_proto: descriptor_pb2.FileDescriptorProto, message_name: str):
    _POOL.Add(file_proto)
    msg_desc = _POOL.FindMessageTypeByName(f"{file_proto.package}.{message_name}")
    return _FACTORY.GetPrototype(msg_desc)


def _make_file_proto(name: str, package: str) -> descriptor_pb2.FileDescriptorProto:
    fp = descriptor_pb2.FileDescriptorProto()
    fp.name = name
    fp.package = package
    fp.syntax = "proto3"
    return fp


# ---- Fixture-built message classes (created once per unique package) ----


def _scalar_msg_cls():
    fp = _make_file_proto("scalar.proto", "t_scalar")
    msg = fp.message_type.add()
    msg.name = "Scalar"
    for num, (name, ftype) in enumerate(
        [
            ("s", FieldDescriptor.TYPE_STRING),
            ("i32", FieldDescriptor.TYPE_INT32),
            ("b", FieldDescriptor.TYPE_BOOL),
            ("bs", FieldDescriptor.TYPE_BYTES),
            ("d", FieldDescriptor.TYPE_DOUBLE),
        ],
        start=1,
    ):
        f = msg.field.add()
        f.name = name
        f.number = num
        f.type = ftype
        f.label = FieldDescriptor.LABEL_OPTIONAL
    return _build_message_class(fp, "Scalar")


def _repeated_msg_cls():
    fp = _make_file_proto("repeated.proto", "t_repeated")
    msg = fp.message_type.add()
    msg.name = "Rep"
    f = msg.field.add()
    f.name = "tags"
    f.number = 1
    f.type = FieldDescriptor.TYPE_STRING
    f.label = FieldDescriptor.LABEL_REPEATED
    return _build_message_class(fp, "Rep")


def _enum_msg_cls():
    fp = _make_file_proto("enum.proto", "t_enum")
    enum = fp.enum_type.add()
    enum.name = "Color"
    for idx, name in enumerate(["UNKNOWN", "RED", "GREEN"]):
        v = enum.value.add()
        v.name = name
        v.number = idx
    msg = fp.message_type.add()
    msg.name = "EnumHolder"
    f = msg.field.add()
    f.name = "color"
    f.number = 1
    f.type = FieldDescriptor.TYPE_ENUM
    f.type_name = "t_enum.Color"
    f.label = FieldDescriptor.LABEL_OPTIONAL
    return _build_message_class(fp, "EnumHolder")


def _nested_msg_cls():
    fp = _make_file_proto("nested.proto", "t_nested")
    outer = fp.message_type.add()
    outer.name = "Outer"
    inner = outer.nested_type.add()
    inner.name = "Inner"
    inner_f = inner.field.add()
    inner_f.name = "val"
    inner_f.number = 1
    inner_f.type = FieldDescriptor.TYPE_STRING
    inner_f.label = FieldDescriptor.LABEL_OPTIONAL
    outer_f = outer.field.add()
    outer_f.name = "inner"
    outer_f.number = 1
    outer_f.type = FieldDescriptor.TYPE_MESSAGE
    outer_f.type_name = "t_nested.Outer.Inner"
    outer_f.label = FieldDescriptor.LABEL_OPTIONAL
    return _build_message_class(fp, "Outer")


def _map_msg_cls():
    fp = _make_file_proto("map.proto", "t_map")
    msg = fp.message_type.add()
    msg.name = "MapHolder"
    entry = msg.nested_type.add()
    entry.name = "AttrsEntry"
    entry.options.map_entry = True
    key = entry.field.add()
    key.name = "key"
    key.number = 1
    key.type = FieldDescriptor.TYPE_STRING
    key.label = FieldDescriptor.LABEL_OPTIONAL
    val = entry.field.add()
    val.name = "value"
    val.number = 2
    val.type = FieldDescriptor.TYPE_INT32
    val.label = FieldDescriptor.LABEL_OPTIONAL
    f = msg.field.add()
    f.name = "attrs"
    f.number = 1
    f.type = FieldDescriptor.TYPE_MESSAGE
    f.type_name = "t_map.MapHolder.AttrsEntry"
    f.label = FieldDescriptor.LABEL_REPEATED
    return _build_message_class(fp, "MapHolder")


# ---- TYPE_CALLABLE_MAP / helpers ----


def test_repeated_wraps_callable() -> None:
    from karapace.core.protobuf.protobuf_to_dict import repeated

    wrapped = repeated(int)
    assert wrapped(["1", "2", "3"]) == [1, 2, 3]


def test_datetime_timestamp_roundtrip() -> None:
    dt = datetime.datetime(2026, 4, 30, 12, 0, 0)

    ts = datetime_to_timestamp(dt)

    assert isinstance(ts, Timestamp)
    assert timestamp_to_datetime(ts) == dt


# ---- protobuf_to_dict ----


def test_protobuf_to_dict_scalars_with_defaults() -> None:
    cls = _scalar_msg_cls()
    instance = cls()

    result = protobuf_to_dict(instance)

    assert result == {"s": "", "i32": 0, "b": False, "bs": b"", "d": 0.0}


def test_protobuf_to_dict_scalars_set_values() -> None:
    cls = _scalar_msg_cls()
    instance = cls(s="hi", i32=7, b=True, bs=b"xy", d=1.5)

    result = protobuf_to_dict(instance)

    assert result == {"s": "hi", "i32": 7, "b": True, "bs": b"xy", "d": 1.5}


def test_protobuf_to_dict_excludes_defaults_when_disabled() -> None:
    cls = _scalar_msg_cls()
    instance = cls(s="only")

    result = protobuf_to_dict(instance, including_default_value_fields=False)

    assert result == {"s": "only"}


def test_protobuf_to_dict_repeated_field() -> None:
    cls = _repeated_msg_cls()
    instance = cls(tags=["a", "b", "c"])

    assert protobuf_to_dict(instance) == {"tags": ["a", "b", "c"]}


def test_protobuf_to_dict_repeated_default_empty() -> None:
    cls = _repeated_msg_cls()
    instance = cls()

    assert protobuf_to_dict(instance) == {"tags": []}


def test_protobuf_to_dict_enum_as_label() -> None:
    cls = _enum_msg_cls()
    instance = cls(color=1)

    assert protobuf_to_dict(instance) == {"color": "RED"}


def test_protobuf_to_dict_enum_lowercased() -> None:
    cls = _enum_msg_cls()
    instance = cls(color=2)

    assert protobuf_to_dict(instance, lowercase_enum_lables=True) == {"color": "green"}


def test_protobuf_to_dict_enum_as_int_when_disabled() -> None:
    cls = _enum_msg_cls()
    instance = cls(color=1)

    assert protobuf_to_dict(instance, use_enum_labels=False) == {"color": 1}


def test_protobuf_to_dict_nested_message() -> None:
    cls = _nested_msg_cls()
    instance = cls()
    instance.inner.val = "deep"

    result = protobuf_to_dict(instance)

    assert result == {"inner": {"val": "deep"}}


def test_protobuf_to_dict_nested_message_unset() -> None:
    cls = _nested_msg_cls()
    instance = cls()

    # Unset singular message field should be excluded from result (no default)
    assert protobuf_to_dict(instance) == {}


def test_protobuf_to_dict_map_field() -> None:
    cls = _map_msg_cls()
    instance = cls()
    instance.attrs["a"] = 1
    instance.attrs["b"] = 2

    result = protobuf_to_dict(instance)

    assert result == {"attrs": {"a": 1, "b": 2}}


def test_protobuf_to_dict_map_default_empty() -> None:
    cls = _map_msg_cls()
    instance = cls()

    assert protobuf_to_dict(instance) == {"attrs": {}}


# ---- dict_to_protobuf ----


def test_dict_to_protobuf_with_class() -> None:
    cls = _scalar_msg_cls()

    instance = dict_to_protobuf(cls, {"s": "hi", "i32": 9})

    assert isinstance(instance, cls)
    assert instance.s == "hi"
    assert instance.i32 == 9


def test_dict_to_protobuf_with_instance() -> None:
    cls = _scalar_msg_cls()
    existing = cls(s="keep")

    instance = dict_to_protobuf(existing, {"i32": 42})

    assert instance is existing
    assert existing.s == "keep"
    assert existing.i32 == 42


def test_dict_to_protobuf_unknown_key_strict_raises() -> None:
    cls = _scalar_msg_cls()

    with pytest.raises(KeyError):
        dict_to_protobuf(cls, {"unknown": 1})


def test_dict_to_protobuf_unknown_key_non_strict_ignored() -> None:
    cls = _scalar_msg_cls()

    instance = dict_to_protobuf(cls, {"unknown": 1, "s": "ok"}, strict=False)

    assert instance.s == "ok"


def test_dict_to_protobuf_repeated() -> None:
    cls = _repeated_msg_cls()

    instance = dict_to_protobuf(cls, {"tags": ["x", "y"]})

    assert list(instance.tags) == ["x", "y"]


def test_dict_to_protobuf_nested() -> None:
    cls = _nested_msg_cls()

    instance = dict_to_protobuf(cls, {"inner": {"val": "deep"}})

    assert instance.inner.val == "deep"


def test_dict_to_protobuf_enum_by_name() -> None:
    cls = _enum_msg_cls()

    instance = dict_to_protobuf(cls, {"color": "GREEN"})

    assert instance.color == 2


def test_dict_to_protobuf_enum_lowercase_non_strict() -> None:
    cls = _enum_msg_cls()

    instance = dict_to_protobuf(cls, {"color": "red"}, strict=False)

    assert instance.color == 1


def test_dict_to_protobuf_enum_unknown_strict_raises() -> None:
    cls = _enum_msg_cls()

    with pytest.raises(KeyError):
        dict_to_protobuf(cls, {"color": "PURPLE"})


def test_dict_to_protobuf_ignores_none() -> None:
    cls = _scalar_msg_cls()

    instance = dict_to_protobuf(cls, {"s": None, "i32": 5}, ignore_none=True)

    assert instance.s == ""
    assert instance.i32 == 5


def test_dict_to_protobuf_map_field() -> None:
    cls = _map_msg_cls()

    instance = dict_to_protobuf(cls, {"attrs": {"a": 1, "b": 2}})

    assert dict(instance.attrs) == {"a": 1, "b": 2}


def test_extension_container_non_int_key_raises() -> None:
    cls = _scalar_msg_cls()

    with pytest.raises(ValueError):
        dict_to_protobuf(cls, {EXTENSION_CONTAINER: {"not-an-int": 1}})


# ---- enum_label_name / introspection helpers ----


def test_enum_label_name_returns_uppercase_by_default() -> None:
    cls = _enum_msg_cls()
    field = cls.DESCRIPTOR.fields_by_name["color"]

    assert enum_label_name(field, 1) == "RED"
    assert enum_label_name(field, 2, lowercase_enum_lables=True) == "green"


def test_get_field_names_and_options_yields_each_field() -> None:
    cls = _scalar_msg_cls()
    instance = cls()

    names = [name for _, name, _ in get_field_names_and_options(instance)]

    assert names == ["s", "i32", "b", "bs", "d"]


# ---- validate_dict_for_required_pb_fields ----


def test_validate_dict_missing_fields_raises() -> None:
    cls = _scalar_msg_cls()
    instance = cls()

    with pytest.raises(FieldsMissing) as exc_info:
        validate_dict_for_required_pb_fields(instance, {"s": "x"})

    # all other fields are missing
    msg = str(exc_info.value)
    for field in ["i32", "b", "bs", "d"]:
        assert field in msg


def test_validate_dict_all_fields_present_passes() -> None:
    cls = _scalar_msg_cls()
    instance = cls()

    validate_dict_for_required_pb_fields(instance, {"s": "x", "i32": 0, "b": False, "bs": b"", "d": 0.0})
