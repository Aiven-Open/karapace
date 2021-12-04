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
    log.info(proto_name)
    try:
        with open(f"{proto_name}.proto", "w") as proto_text:
            proto_text.write(str(proto))
            proto_text.close()

    except Exception as e:  # pylint: disable=broad-except
        log.error("Unexpected exception in statsd send: %s: %s", e.__class__.__name__, e)
        assert False, f"Cannot write Proto File. Unexpected exception in statsd send: {e.__class__.__name__} + {e}"

    args = ["protoc", "--python_out=./", f"{proto_name}.proto"]
    try:
        proc = Popen(args, stdout=PIPE, stderr=PIPE, shell=False)
    except FileNotFoundError as e:
        assert False, f"Protoc not found. {e}"
    except Exception as e:  # pylint: disable=broad-except
        log.error("Unexpected exception in statsd send: %s: %s", e.__class__.__name__, e)
        assert False, f"Cannot execute protoc. Unexpected exception in statsd send: {e.__class__.__name__} + {e}"
    try:
        out, err = proc.communicate(timeout=10)
        assert out == b''
        assert err == b''
    except TimeoutExpired:
        proc.kill()
        assert False, "Timeout expired"
    module_content = ""
    try:
        with open(f"{proto_name}.proto", "r") as proto_text:
            module_content = proto_text.read()
            proto_text.close()
        print(module_content)

    except Exception as e:  # pylint: disable=broad-except
        log.error("Unexpected exception in statsd send: %s: %s", e.__class__.__name__, e)
        assert False, f"Cannot read Proto File. Unexpected exception in statsd send: {e.__class__.__name__} + {e}"

    spec = importlib.util.spec_from_file_location(f"{proto_name}_pb2", f"./{proto_name}_pb2.py")
    tmp_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tmp_module)


#    try:

#    except Exception as e:  # pylint: disable=broad-except
#        log.error("Unexpected exception in statsd send: %s: %s", e.__class__.__name__, e)
#        assert False, f"Cannot execute protoc. Unexpected exception in statsd send: {e.__class__.__name__} + {e}"
