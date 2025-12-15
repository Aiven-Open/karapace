"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import sys
import textwrap
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from karapace.core.dependency import Dependency
from karapace.core.protobuf.io import crawl_dependencies, calculate_class_name
from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.schema_models import ValidatedTypedSchema
from karapace.core.schema_type import SchemaType
from karapace.core.typing import Subject

from karapace.core.protobuf.io import get_protobuf_class_instance


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


def test_get_protobuf_class_instance_creates_and_imports(tmp_path: Path):
    # Minimal protobuf schema
    schema_str = """
        syntax = "proto3";
        package testpkg;
        message TestMessage {
            string foo = 1;
        }
    """

    # Create ProtobufSchema
    schema = ProtobufSchema(schema=schema_str)

    # Simulate config pointing to runtime directory (e.g. ./runtime)
    cfg = SimpleNamespace(protobuf_runtime_directory=str(tmp_path / "runtime"))

    # Calculate expected proto_name (same logic as in get_protobuf_class_instance)
    deps_list = crawl_dependencies(schema)
    root_class_name = ""
    for value in deps_list.values():
        root_class_name = root_class_name + value["unique_class_name"]
    root_class_name = root_class_name + str(schema)
    expected_proto_name = calculate_class_name(root_class_name)

    expected_work_dir = tmp_path / "runtime" / expected_proto_name
    expected_runtime_proto_path = str(expected_work_dir.resolve())

    # Create a custom list class that tracks what gets appended
    class TrackingList(list):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.appended_items = []

        def append(self, item):
            self.appended_items.append(item)
            super().append(item)

    # Replace sys.path with our tracking list
    original_sys_path = sys.path
    tracking_sys_path = TrackingList(original_sys_path)

    # Call function with tracking sys.path to capture what gets added
    with patch("sys.path", tracking_sys_path):
        instance = get_protobuf_class_instance(schema, "TestMessage", cfg)

    # Restore original sys.path
    sys.path = original_sys_path

    # Assert: generated module exists
    generated_dirs = list((tmp_path / "runtime").glob("c_*"))
    assert generated_dirs, "Expected at least one generated runtime directory"

    generated_dir = generated_dirs[0]
    pb2_files = list(generated_dir.glob("*_pb2.py"))
    assert pb2_files, "Expected protoc to generate a *_pb2.py file"

    # Assert: generated directory name matches expected proto_name
    assert (
        generated_dir.name == expected_proto_name
    ), f"Expected directory name {expected_proto_name}, got {generated_dir.name}"

    # Assert: instance is of the expected class
    assert instance.__class__.__name__ == "TestMessage"
    assert hasattr(instance, "foo")

    appended_paths = tracking_sys_path.appended_items
    assert len(appended_paths) > 0, "Expected sys.path.append to be called at least once"

    # Find the runtime_proto_path that was added
    runtime_path_added = None
    for path in appended_paths:
        if expected_proto_name in path or "runtime" in path:
            runtime_path_added = path
            break

    assert runtime_path_added is not None, f"Expected a runtime path to be added to sys.path, but got: {appended_paths}"

    assert runtime_path_added == expected_runtime_proto_path, (
        f"Expected absolute path {expected_runtime_proto_path}, " f"but got {runtime_path_added}. "
    )

    # Clean up to avoid residue
    shutil.rmtree(tmp_path)
