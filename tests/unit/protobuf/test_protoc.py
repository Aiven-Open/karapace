"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import importlib
import importlib.util
import logging
import subprocess

from karapace.core.container import KarapaceContainer
from karapace.core.protobuf.io import calculate_class_name
from karapace.core.protobuf.kotlin_wrapper import trim_margin

log = logging.getLogger(__name__)


def test_protoc(karapace_container: KarapaceContainer) -> None:
    proto: str = """
                 |syntax = "proto3";
                 |package com.instaclustr.protobuf;
                 |option java_outer_classname = "SimpleMessageProtos";
                 |message SimpleMessage {
                 |  string content = 1;
                 |  string date_time = 2;
                 |  string content2 = 3;
                 |}
                 |
                 """
    proto = trim_margin(proto)

    directory = karapace_container.config().protobuf_runtime_directory
    proto_name = calculate_class_name(str(proto))
    proto_path = f"{directory}/{proto_name}.proto"
    class_path = f"{directory}/{proto_name}_pb2.py"

    log.info(proto_name)
    with open(proto_path, mode="w", encoding="utf8") as proto_text:
        proto_text.write(str(proto))

    completed_process = subprocess.run(
        args=["protoc", "--python_out=./", proto_path],
        capture_output=True,
        check=True,
    )
    assert completed_process.stdout == b""
    assert completed_process.stderr == b""

    spec = importlib.util.spec_from_file_location(f"{proto_name}_pb2", class_path)
    tmp_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tmp_module)  # type: ignore[union-attr]
