"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dataclasses import dataclass, field
from functools import cached_property
from karapace.errors import InvalidSchema
from typing import Dict, List

import ctypes
import importlib.util
import timeit

spec = importlib.util.find_spec("protopacelib")
if not spec:
    raise FileNotFoundError("Unable to find protopace shared library")

lib_file = spec.origin
lib = ctypes.CDLL(lib_file)

lib.FormatSchema.argtypes = [
    ctypes.c_char_p,  # schema name
    ctypes.c_char_p,  # schema string
    ctypes.Array,  # dependency names
    ctypes.Array,  # dependency schema strings
    ctypes.c_int,  # number of dependencies
]
lib.FormatSchema.restype = ctypes.c_void_p
lib.CheckCompatibility.restype = ctypes.c_char_p


class FormatResult(ctypes.Structure):
    _fields_ = [
        ("res", ctypes.c_char_p),
        ("err", ctypes.c_char_p),
    ]


@dataclass
class Proto:
    name: str
    schema: str
    dependencies: List["Proto"] = field(default_factory=list)

    @cached_property
    def all_dependencies(self) -> List["Proto"]:
        dependencies: Dict[str, "Proto"] = {}
        for dep in self.dependencies:
            if dep.dependencies:
                dependencies.update([(d.name, d) for d in dep.all_dependencies])
            dependencies[dep.name] = dep
        return list(dependencies.values())


class IncompatibleError(Exception):
    pass


def format_proto(proto: Proto) -> str:
    length = len(proto.all_dependencies)
    c_dependencies = (ctypes.c_char_p * length)(*[d.schema.encode() for d in proto.all_dependencies])
    c_dependency_names = (ctypes.c_char_p * length)(*[d.name.encode() for d in proto.all_dependencies])
    c_name = ctypes.c_char_p(proto.name.encode())
    c_schema = ctypes.c_char_p(proto.schema.encode())
    res_ptr = lib.FormatSchema(c_name, c_schema, c_dependency_names, c_dependencies, length)
    res = FormatResult.from_address(res_ptr)

    if res.err:
        err = res.err
        msg = err.decode()
        lib.FreeResult(ctypes.c_void_p(res_ptr))
        raise InvalidSchema(msg)

    result = res.res.decode()
    lib.FreeResult(ctypes.c_void_p(res_ptr))
    return result


def check_compatibility(proto: Proto, prev_proto: Proto) -> None:
    length = len(proto.all_dependencies)
    c_dependencies = (ctypes.c_char_p * length)(*[d.schema.encode() for d in proto.all_dependencies])
    c_dependency_names = (ctypes.c_char_p * length)(*[d.name.encode() for d in proto.all_dependencies])

    prev_length = len(prev_proto.all_dependencies)
    prev_c_dependencies = (ctypes.c_char_p * prev_length)(*[d.schema.encode() for d in prev_proto.all_dependencies])
    prev_c_dependency_names = (ctypes.c_char_p * prev_length)(*[d.name.encode() for d in prev_proto.all_dependencies])

    err = lib.CheckCompatibility(
        proto.name.encode(),
        proto.schema.encode(),
        c_dependency_names,
        c_dependencies,
        length,
        prev_proto.name.encode(),
        prev_proto.schema.encode(),
        prev_c_dependency_names,
        prev_c_dependencies,
        prev_length,
    )

    if err is not None:
        msg = err.decode()
        raise IncompatibleError(msg)


SCHEMA = """
syntax = "proto3";

package my.awesome.customer.v1;

import "my/awesome/customer/v1/nested_value.proto";
import "google/protobuf/timestamp.proto";

option ruby_package = "My::Awesome::Customer::V1";
option csharp_namespace = "my.awesome.customer.V1";
option go_package = "github.com/customer/api/my/awesome/customer/v1;dspv1";
option java_multiple_files = true;
option java_outer_classname = "EventValueProto";
option java_package = "com.my.awesome.customer.v1";
option objc_class_prefix = "TDD";

message Local {
    message NestedValue {
        string foo = 1;
    }
}

message EventValue {
  NestedValue nested_value = 1;
  google.protobuf.Timestamp created_at = 2;
  Status status = 3;
  Local.NestedValue local_nested_value = 4;
}
"""

DEPENDENCY = """
syntax = "proto3";
package my.awesome.customer.v1;

message NestedValue {
    string value = 1;
}

enum Status {
    UNKNOWN = 0;
    ACTIVE = 1;
    INACTIVE = 2;
}
"""


def _format_time(seconds: float) -> str:
    units = [("s", 1), ("ms", 1e-3), ("µs", 1e-6), ("ns", 1e-9)]
    for unit, factor in units:
        if seconds >= factor:
            return f"{seconds / factor:.3f} {unit}"
    return f"{seconds:.3f} s"


def _time_format() -> None:
    def test() -> None:
        proto = Proto("test.proto", SCHEMA, [Proto("my/awesome/customer/v1/nested_value.proto", DEPENDENCY)])
        format_proto(proto)

    number = 10000
    seconds = timeit.timeit(test, number=number)

    print("----- Format -----")
    print(f"Total time: {_format_time(seconds)}")
    print(f"Execution time per loop: {_format_time(seconds / number)}")


def _time_check_compatibility() -> None:
    def test() -> None:
        proto = Proto("test.proto", SCHEMA, [Proto("my/awesome/customer/v1/nested_value.proto", DEPENDENCY)])
        check_compatibility(proto, proto)

    number = 10000
    seconds = timeit.timeit(test, number=number)

    print("----- Compatibility Check -----")
    print(f"Total time: {_format_time(seconds)}")
    print(f"Execution time per loop: {_format_time(seconds / number)}")


if __name__ == "__main__":
    _time_format()
    _time_check_compatibility()
