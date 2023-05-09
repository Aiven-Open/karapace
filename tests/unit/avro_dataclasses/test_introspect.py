"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass, Field, field, fields
from enum import Enum
from karapace.avro_dataclasses.introspect import field_schema, record_schema, UnsupportedAnnotation
from karapace.avro_dataclasses.schema import FieldSchema
from typing import Final, Mapping, Optional, Sequence, Tuple

import datetime
import pytest


class Symbols(Enum):
    a = "a"
    b = "b"


@dataclass
class Nested:
    value: bool


@dataclass
class ValidRecord:
    bool_field: bool
    string_field: str
    bytes_field: bytes
    long_field: int = field(metadata={"type": "long"})
    int_field: int
    explicit_int_field: int = field(metadata={"type": "int"})
    none_field: None
    optional_field: Optional[int]
    optional_bytes_field: Optional[bytes]
    enum_field: Symbols
    dt_field: datetime.datetime
    int_array: Tuple[int, ...]
    nested_values: Tuple[Nested, ...]

    enum_field_default: Symbols = Symbols.a
    int_field_default: int = 123
    string_field_default: str = "foo"


valid_fields: Final[Mapping[str, Field]] = {field.name: field for field in fields(ValidRecord)}


@dataclass
class InvalidRecord:
    any_tuple: tuple
    homogenous_short_tuple: Tuple[int]
    homogenous_bi_tuple: Tuple[int, int]
    homogenous_tri_tuple: Tuple[int, int, int]
    any_list: list
    any_sequence: Sequence


invalid_fields: Final[Mapping[str, Field]] = {field.name: field for field in fields(InvalidRecord)}


class TestFieldSchema:
    @pytest.mark.parametrize(
        ("dc_field", "expected_schema"),
        (
            (valid_fields["bool_field"], {"name": "bool_field", "type": "boolean"}),
            (valid_fields["string_field"], {"name": "string_field", "type": "string"}),
            (valid_fields["bytes_field"], {"name": "bytes_field", "type": "bytes"}),
            (valid_fields["long_field"], {"name": "long_field", "type": "long"}),
            (valid_fields["int_field"], {"name": "int_field", "type": "int"}),
            (valid_fields["explicit_int_field"], {"name": "explicit_int_field", "type": "int"}),
            (valid_fields["none_field"], {"name": "none_field", "type": "null"}),
            (valid_fields["optional_field"], {"name": "optional_field", "type": ["int", "null"]}),
            (valid_fields["optional_bytes_field"], {"name": "optional_bytes_field", "type": ["bytes", "null"]}),
            (
                valid_fields["enum_field"],
                {
                    "name": "enum_field",
                    "type": {
                        "name": "Symbols",
                        "type": "enum",
                        "symbols": ["a", "b"],
                    },
                },
            ),
            (
                valid_fields["enum_field_default"],
                {
                    "name": "enum_field_default",
                    "default": "a",
                    "type": {
                        "name": "Symbols",
                        "type": "enum",
                        "symbols": ["a", "b"],
                        "default": "a",
                    },
                },
            ),
            (
                valid_fields["dt_field"],
                {
                    "name": "dt_field",
                    "type": {
                        "logicalType": "timestamp-millis",
                        "type": "long",
                    },
                },
            ),
            (
                valid_fields["int_array"],
                {
                    "name": "int_array",
                    "type": {
                        "name": "one_of_int_array",
                        "type": "array",
                        "items": "int",
                    },
                },
            ),
            (
                valid_fields["nested_values"],
                {
                    "name": "nested_values",
                    "type": {
                        "name": "one_of_nested_values",
                        "type": "array",
                        "items": {
                            "name": "Nested",
                            "type": "record",
                            "fields": [{"name": "value", "type": "boolean"}],
                        },
                    },
                },
            ),
            (
                valid_fields["int_field_default"],
                {
                    "name": "int_field_default",
                    "type": "int",
                    "default": 123,
                },
            ),
            (
                valid_fields["string_field_default"],
                {
                    "name": "string_field_default",
                    "type": "string",
                    "default": "foo",
                },
            ),
        ),
    )
    def test_returns_expected_type(
        self,
        dc_field: Field,
        expected_schema: FieldSchema,
    ) -> None:
        assert field_schema(dc_field) == expected_schema

    @pytest.mark.parametrize(
        ("dc_field",),
        ((field,) for field in fields(InvalidRecord)),
    )
    def test_raises_unsupported_annotation(self, dc_field: Field) -> None:
        with pytest.raises(UnsupportedAnnotation):
            field_schema(dc_field)


class TestRecordSchema:
    def test_can_get_schema(self) -> None:
        assert record_schema(Nested) == {
            "name": "Nested",
            "type": "record",
            "fields": [{"name": "value", "type": "boolean"}],
        }
