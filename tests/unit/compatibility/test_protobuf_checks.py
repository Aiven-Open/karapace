"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from avro.compatibility import SchemaCompatibilityType
from karapace.core.compatibility.protobuf.checks import check_protobuf_schema_compatibility
from karapace.core.protobuf.schema import ProtobufSchema


def _schema(body: str) -> ProtobufSchema:
    return ProtobufSchema(schema=body)


def test_identical_schemas_are_compatible() -> None:
    schema = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
  int32 age = 2;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=schema, writer=schema)

    assert result.compatibility is SchemaCompatibilityType.compatible
    assert result.incompatibilities == []


def test_adding_optional_field_is_compatible() -> None:
    writer = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
}
"""
    )
    reader = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
  int32 age = 2;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=reader, writer=writer)

    assert result.compatibility is SchemaCompatibilityType.compatible


def test_renaming_message_is_incompatible() -> None:
    writer = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
}
"""
    )
    reader = _schema(
        """\
syntax = "proto3";
message Human {
  string name = 1;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=reader, writer=writer)

    assert result.compatibility is SchemaCompatibilityType.incompatible
    assert result.incompatibilities
    assert isinstance(result.locations, set)
    assert isinstance(result.messages, set)


def test_changing_field_type_is_incompatible() -> None:
    writer = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
  int32 age = 2;
}
"""
    )
    reader = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
  string age = 2;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=reader, writer=writer)

    assert result.compatibility is SchemaCompatibilityType.incompatible
    assert result.incompatibilities


def test_changing_field_number_is_incompatible() -> None:
    writer = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
  int32 age = 2;
}
"""
    )
    reader = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
  int32 age = 3;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=reader, writer=writer)

    assert result.compatibility is SchemaCompatibilityType.incompatible
    assert result.incompatibilities


def test_removing_field_is_compatible() -> None:
    writer = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
  int32 age = 2;
}
"""
    )
    reader = _schema(
        """\
syntax = "proto3";
message Person {
  string name = 1;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=reader, writer=writer)

    assert result.compatibility is SchemaCompatibilityType.compatible


def test_adding_enum_value_is_compatible() -> None:
    writer = _schema(
        """\
syntax = "proto3";
message Holder {
  enum Color {
    UNKNOWN = 0;
    RED = 1;
  }
  Color color = 1;
}
"""
    )
    reader = _schema(
        """\
syntax = "proto3";
message Holder {
  enum Color {
    UNKNOWN = 0;
    RED = 1;
    GREEN = 2;
  }
  Color color = 1;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=reader, writer=writer)

    assert result.compatibility is SchemaCompatibilityType.compatible


def test_incompatible_result_has_no_duplicates_in_sets() -> None:
    writer = _schema(
        """\
syntax = "proto3";
message Person {
  string a = 1;
  int32 b = 2;
}
"""
    )
    reader = _schema(
        """\
syntax = "proto3";
message Person {
  string a = 1;
  string b = 2;
}
"""
    )

    result = check_protobuf_schema_compatibility(reader=reader, writer=writer)

    assert result.compatibility is SchemaCompatibilityType.incompatible
    # locations/messages are deduplicated sets — verify they stayed sets
    assert len(result.locations) == len(set(result.locations))
    assert len(result.messages) == len(set(result.messages))
