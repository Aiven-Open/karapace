"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.dependency import Dependency
from karapace.protobuf.io import crawl_dependencies
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_models import ValidatedTypedSchema
from karapace.schema_type import SchemaType
from karapace.typing import Subject

import textwrap


def test_crawl_dependencies() -> None:
    schema_a = ValidatedTypedSchema.parse(
        schema_type=SchemaType.PROTOBUF,
        schema_str=textwrap.dedent(
            """\
            syntax = "proto3";
            package a;
            message A {
              string foo = 1;
            }
            """
        ),
        references=[],
        dependencies={},
    )
    schema_b = ValidatedTypedSchema.parse(
        schema_type=SchemaType.PROTOBUF,
        schema_str=textwrap.dedent(
            """\
            syntax = "proto3";
            package b;
            message B {
              string foo = 1;
            }
            """
        ),
        references=[],
        dependencies={
            "a": Dependency(
                name="a",
                subject=Subject("a"),
                version="1",
                target_schema=schema_a,
            ),
        },
    )
    schema_c = ProtobufSchema(
        schema="",
        dependencies={
            "b": Dependency(
                name="b",
                subject=Subject("b"),
                version="1:",
                target_schema=schema_b,
            )
        },
    )
    dependencies = crawl_dependencies(schema_c)
    assert dependencies == {
        "a": {
            "schema": schema_a.schema_str,
            "unique_class_name": "c_515eecf243b58d2c34d04be2ead3b2c0",
        },
        "b": {
            "schema": schema_b.schema_str,
            "unique_class_name": "c_df098b6b018617c2b8eb95156535dec6",
        },
    }
