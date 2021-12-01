from karapace.protobuf.io import calculate_class_name
from karapace.protobuf.kotlin_wrapper import trim_margin
from subprocess import PIPE, Popen, TimeoutExpired

import importlib
import importlib.util
import logging

log = logging.getLogger("KarapaceTests")


def test_protoc():
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

    proto_name = calculate_class_name(str(proto))
    try:
        with open(f"{proto_name}.proto", "w") as proto_text:
            proto_text.write(str(proto))
            proto_text.close()
    except Exception as e:  # pylint: disable=broad-except
        log.error("Unexpected exception in statsd send: %s: %s", e.__class__.__name__, e)

    proc = Popen(["protoc", "--python_out=./", f"{proto_name}.proto"], stdout=PIPE, stderr=PIPE, shell=True)
    try:
        out, err = proc.communicate(timeout=10)
        log.info(out)
        log.error(err)
    except TimeoutExpired:
        proc.kill()

    try:
        spec = importlib.util.spec_from_file_location(f"{proto_name}_pb2", f"./{proto_name}_pb2.py")
        tmp_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tmp_module)

    except Exception as e:  # pylint: disable=broad-except
        log.error("Unexpected exception in statsd send: %s: %s", e.__class__.__name__, e)

    assert False
