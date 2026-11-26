"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import os
import sys
import textwrap
import threading
import shutil
from concurrent.futures import ProcessPoolExecutor
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import psutil

from karapace.core.config import Config
from karapace.core.dependency import Dependency
from karapace.core.protobuf.io import crawl_dependencies, calculate_class_name
from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.schema_models import ValidatedTypedSchema
from karapace.core.schema_type import SchemaType
from karapace.core.typing import Subject

from karapace.core.protobuf.io import (
    _MP_CONTEXT,
    get_protobuf_class_instance,
    ProtobufDatumReader,
    ProtobufDatumWriter,
    warm_up_protobuf_subprocesses,
)


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

    runtime_directory = str(tmp_path / "runtime")

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
        instance = get_protobuf_class_instance(schema, "TestMessage", runtime_directory)

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


def test_protobuf_child_processes_are_started_by_a_single_threaded_forkserver() -> None:
    warm_up_protobuf_subprocesses()

    with ProcessPoolExecutor(max_workers=1, mp_context=_MP_CONTEXT) as executor:
        forkserver_pid = executor.submit(os.getppid).result(timeout=30)

    # A forked child would have this process as its parent
    assert forkserver_pid != os.getpid()
    # A forkserver with threads, for example after preloading protopace, would bring back the problems of "fork"
    assert psutil.Process(forkserver_pid).num_threads() == 1


class _LockingTokenProvider:
    """A token provider that cannot be pickled, like one caching its token behind a lock."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def token_with_expiry(self, config: str | None = None) -> tuple[str, int | None]:
        with self._lock:
            return "token", None


def test_protobuf_round_trip_with_unpicklable_config(tmp_path: Path) -> None:
    config = Config()
    config.protobuf_runtime_directory = str(tmp_path / "runtime")
    config.sasl_oauth_token_provider_class = _LockingTokenProvider
    config.model_post_init(None)
    schema = ProtobufSchema('syntax = "proto3";\nmessage TestRecord {\n  string s = 1;\n  int32 i = 2;\n}\n')
    datum = {"s": "testdata", "i": 32}

    writer = ProtobufDatumWriter(config, schema)
    bio = BytesIO()
    writer.write_index(bio)
    writer.write(datum, bio)
    bio.seek(0)

    # The child processes get only the runtime directory, the Config with its provider is never pickled
    assert ProtobufDatumReader(config, schema).read(bio) == datum
