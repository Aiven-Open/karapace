"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Generator, Iterable
from io import BytesIO
from karapace.core.config import Config
from karapace.core.protobuf.encoding_variants import read_indexes, write_indexes
from karapace.core.protobuf.exception import (
    IllegalArgumentException,
    ProtobufSchemaResolutionException,
    ProtobufTypeException,
)
from karapace.core.protobuf.message_element import MessageElement
from karapace.core.protobuf.protobuf_to_dict import dict_to_protobuf, protobuf_to_dict
from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.protobuf.type_element import TypeElement
from multiprocessing.queues import Queue
from pathlib import Path
from typing import Final, Protocol, TypeAlias
from typing_extensions import Self

import hashlib
import importlib
import importlib.util
import multiprocessing
import subprocess
import sys

# Protobuf serialization and deserialization run in child processes, see `read_in_forked_multiprocess_process`.
# The start method is set explicitly to "forkserver" so it is the same on every Python version: it is the
# default on Linux since Python 3.14, while older versions default to "fork".
# "fork" must be avoided because the REST proxy is multi-threaded: librdkafka, the asyncio executor and the Go
# runtime of protopace all run threads. The child inherits only the forking thread, and a lock held by another
# thread at fork time (logging, malloc, ...) is never released in the child, which can then deadlock. The Go
# runtime does not support fork without exec at all. Python 3.12+ warns about forking a multi-threaded process.
# See https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
# The forkserver listens on a Unix socket in Python's temporary directory, which must be writable (see the
# troubleshooting page of the documentation).
# The forkserver pickles the arguments of every child, so the children get only the values they need, not the
# `Config`: it can hold objects that cannot be pickled, like the instance of `sasl_oauth_token_provider_class`.
_MP_CONTEXT: Final = multiprocessing.get_context("forkserver")
# The forkserver is a fresh interpreter, and when Karapace runs with `python -m` its default `__main__` preload
# imports nothing from Karapace. Every child would then import this module and its dependencies to unpickle its
# target and arguments, and that counts against the result timeout. Preloading this module lets the children
# inherit it. This module must not load protopace: its Go runtime starts threads and would make the forkserver
# itself multi-threaded, which is why `karapace.core.dependency` imports it lazily.
_MP_CONTEXT.set_forkserver_preload(["karapace.core.protobuf.io"])


def calculate_class_name(name: str) -> str:
    return "c_" + hashlib.md5(name.encode("utf-8")).hexdigest()


def match_schemas(writer_schema: ProtobufSchema, reader_schema: ProtobufSchema) -> bool:
    # TODO (serge): is schema comparison by fields required?

    return str(writer_schema) == str(reader_schema)


def find_message_name(schema: ProtobufSchema, indexes: Iterable[int]) -> str:
    result: list[str] = []
    types = schema.proto_file_element.types
    for index in indexes:
        try:
            message = types[index]
        except IndexError:
            raise IllegalArgumentException(f"Invalid message indexes: {indexes}")

        if message and isinstance(message, MessageElement):
            result.append(message.name)
            types = message.nested_types
        else:
            raise IllegalArgumentException(f"Invalid message indexes: {indexes}")

    # for java we also need package name. But in case we will use protoc
    # for compiling to python we can ignore it at all
    return ".".join(result)


def _crawl_dependencies(
    schema: ProtobufSchema,
) -> Generator[tuple[str, dict[str, str]], None, None]:
    if not schema.dependencies:
        return
    for name, dependency in schema.dependencies.items():
        # todo: https://github.com/aiven/karapace/issues/641
        assert isinstance(dependency.schema.schema, ProtobufSchema)
        yield from _crawl_dependencies(dependency.schema.schema)
        yield (
            name,
            {
                "schema": str(dependency.schema.schema),
                "unique_class_name": calculate_class_name(f"{dependency.version}_{dependency.name}"),
            },
        )


def crawl_dependencies(schema: ProtobufSchema) -> dict[str, dict[str, str]]:
    return dict(_crawl_dependencies(schema))


def replace_imports(string: str, deps_list: dict[str, dict[str, str]] | None) -> str:
    if deps_list is None:
        return string
    for key, value in deps_list.items():
        unique_class_name = value["unique_class_name"] + ".proto"
        string = string.replace('"' + key + '"', f'"{unique_class_name}"')
    return string


class _ProtobufModel(Protocol):
    def ParseFromString(self, buffer: bytes) -> Self: ...

    def SerializeToString(self) -> bytes: ...


def get_protobuf_class_instance(
    schema: ProtobufSchema,
    class_name: str,
    runtime_directory: str,
) -> _ProtobufModel:
    directory = Path(runtime_directory)
    deps_list = crawl_dependencies(schema)
    root_class_name = ""
    for value in deps_list.values():
        root_class_name = root_class_name + value["unique_class_name"]
    root_class_name = root_class_name + str(schema)
    proto_name = calculate_class_name(root_class_name)

    main_proto_filename = f"{proto_name}.proto"
    work_dir = directory / Path(proto_name)
    work_dir.mkdir(exist_ok=True, parents=True)
    class_path = work_dir / Path(f"{proto_name}_pb2.py")

    if not class_path.exists():
        with open(f"{directory}/{proto_name}/{proto_name}.proto", mode="w", encoding="utf8") as proto_text:
            proto_text.write(replace_imports(str(schema), deps_list))

        protoc_arguments = [
            "protoc",
            "--python_out=./",
            main_proto_filename,
        ]
        for value in deps_list.values():
            proto_file_name = value["unique_class_name"] + ".proto"
            dependency_path = f"{directory}/{proto_name}/{proto_file_name}"
            protoc_arguments.append(proto_file_name)
            with open(dependency_path, mode="w", encoding="utf8") as proto_text:
                proto_text.write(replace_imports(value["schema"], deps_list))

        if not class_path.is_file():
            subprocess.run(
                protoc_arguments,
                check=True,
                cwd=work_dir,
            )

    runtime_proto_path = str(work_dir.resolve())
    if runtime_proto_path not in sys.path:
        # todo: This will leave residues on sys.path in case of exceptions. If really must
        # mutate sys.path, we should at least wrap in try-finally.
        sys.path.append(runtime_proto_path)
    spec = importlib.util.spec_from_file_location(f"{proto_name}_pb2", class_path)
    # This is reasonable to assert because we just created this file.
    assert spec is not None
    tmp_module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(tmp_module)
    sys.path.pop()
    class_to_call = getattr(tmp_module, class_name)

    return class_to_call()


def read_data(
    runtime_directory: str,
    writer_schema: ProtobufSchema,
    reader_schema: ProtobufSchema,
    bio: BytesIO,
) -> _ProtobufModel:
    if not match_schemas(writer_schema, reader_schema):
        fail_msg = "Schemas do not match."
        raise ProtobufSchemaResolutionException(fail_msg, writer_schema, reader_schema)

    indexes = read_indexes(bio)
    name = find_message_name(writer_schema, indexes)

    class_instance = get_protobuf_class_instance(writer_schema, name, runtime_directory)
    class_instance.ParseFromString(bio.read())

    return class_instance


def _warm_up_process() -> None:
    pass


def warm_up_protobuf_subprocesses() -> None:
    """Start the forkserver used for protobuf serialization and deserialization.

    The first child process otherwise starts the forkserver and its preload, which can take several seconds
    on a loaded host. Starting a child blocks until the forkserver has finished its preload, so this returns
    only once the next child can start quickly.
    """
    p = _MP_CONTEXT.Process(target=_warm_up_process)
    p.start()
    p.join()


_ReaderQueue: TypeAlias = "Queue[dict[object, object] | BaseException]"


def reader_process(
    reader_queue: _ReaderQueue,
    runtime_directory: str,
    writer_schema: ProtobufSchema,
    reader_schema: ProtobufSchema,
    bio: BytesIO,
) -> None:
    try:
        reader_queue.put(protobuf_to_dict(read_data(runtime_directory, writer_schema, reader_schema, bio), True))
    # Reading happens in the forked process, catch is broad so exception will get communicated
    # back to calling process.
    except BaseException as base_exception:
        reader_queue.put(base_exception)


def read_in_forked_multiprocess_process(
    config: Config,
    writer_schema: ProtobufSchema,
    reader_schema: ProtobufSchema,
    bio: BytesIO,
) -> dict[object, object]:
    # Note Protobuf enum values use C++ scoping rules,
    # meaning that enum values are siblings of their type, not children of it.
    # Therefore, if we have two proto files with Enums which elements have the same name we will have error.
    # There we use simple way of Serialization/Deserialization (SerDe) which use python Protobuf library and
    # protoc compiler.
    # To avoid problem with enum values for basic SerDe support we
    # will isolate work with call protobuf libraries in child process.
    if __name__ == "karapace.core.protobuf.io":
        reader_queue: _ReaderQueue = _MP_CONTEXT.Queue()
        p = _MP_CONTEXT.Process(
            target=reader_process,
            args=(reader_queue, config.protobuf_runtime_directory, writer_schema, reader_schema, bio),
        )
        p.start()
        TEN_SECONDS_WAIT = 10
        try:
            result = reader_queue.get(True, TEN_SECONDS_WAIT)
        finally:
            p.join()
            reader_queue.close()
        if isinstance(result, dict):
            return result
        if isinstance(result, BaseException):
            raise result
        raise IllegalArgumentException()
    return {"Error": "This never must be returned"}


class ProtobufDatumReader:
    """Deserialize Protobuf-encoded data into a Python data structure."""

    def __init__(
        self,
        config: Config,
        writer_schema: ProtobufSchema,
        reader_schema: ProtobufSchema | None = None,
    ) -> None:
        """As defined in the Protobuf specification, we call the schema encoded
        in the data the "writer's schema", and the schema expected by the
        reader the "reader's schema".
        """
        self.config: Final = config
        self._writer_schema: Final = writer_schema
        self._reader_schema = reader_schema

    def read(self, bio: BytesIO) -> dict:
        if self._reader_schema is None:
            self._reader_schema = self._writer_schema
        return read_in_forked_multiprocess_process(self.config, self._writer_schema, self._reader_schema, bio)


_WriterQueue: TypeAlias = "Queue[bytes | str | BaseException]"


def writer_process(
    writer_queue: _WriterQueue,
    runtime_directory: str,
    writer_schema: ProtobufSchema,
    message_name: str,
    datum: dict,
) -> None:
    try:
        class_instance = get_protobuf_class_instance(writer_schema, message_name, runtime_directory)
        dict_to_protobuf(class_instance, datum)
        result = class_instance.SerializeToString()
        writer_queue.put(result)
    # Writing happens in the forked process, catch is broad so exception will get communicated
    # back to calling process.
    except Exception as bare_exception:
        try:
            raise ProtobufTypeException(writer_schema, datum) from bare_exception
        except ProtobufTypeException as protobuf_exception:
            writer_queue.put(protobuf_exception)
            raise protobuf_exception
    except BaseException as base_exception:
        writer_queue.put(base_exception)


def write_in_forked_multiprocess_process(
    config: Config,
    writer_schema: ProtobufSchema,
    message_name: str,
    datum: dict[object, object],
) -> bytes:
    # Note Protobuf enum values use C++ scoping rules,
    # meaning that enum values are siblings of their type, not children of it.
    # Therefore, if we have two proto files with Enums which elements have the same name we will have error.
    # There we use simple way of Serialization/Deserialization (SerDe) which use python Protobuf library and
    # protoc compiler.
    # To avoid problem with enum values for basic SerDe support we
    # will isolate work with call protobuf libraries in child process.
    if __name__ == "karapace.core.protobuf.io":
        writer_queue: _WriterQueue = _MP_CONTEXT.Queue(1)
        p = _MP_CONTEXT.Process(
            target=writer_process,
            args=(writer_queue, config.protobuf_runtime_directory, writer_schema, message_name, datum),
        )
        p.start()
        TEN_SECONDS_WAIT = 10
        try:
            result = writer_queue.get(True, TEN_SECONDS_WAIT)  # Block for ten seconds
        finally:
            p.join()
            writer_queue.close()
        if isinstance(result, bytes):
            return result
        if isinstance(result, BaseException):
            raise result
        raise IllegalArgumentException()
    raise NotImplementedError("Error: Reached unreachable code")


class ProtobufDatumWriter:
    """ProtobufDatumWriter for generic python objects."""

    def __init__(self, config: Config, writer_schema: ProtobufSchema) -> None:
        self.config = config
        self._writer_schema = writer_schema
        a: ProtobufSchema = writer_schema
        el: TypeElement
        self._message_name = ""
        for idx, el in enumerate(a.proto_file_element.types):
            if isinstance(el, MessageElement):
                self._message_name = el.name
                self._message_index = idx
                break

        if self._message_name == "":
            raise ProtobufTypeException("No message in protobuf schema")

    def write_index(self, writer: BytesIO) -> None:
        write_indexes(writer, [self._message_index])

    def write(self, datum: dict[object, object], writer: BytesIO) -> None:
        writer.write(write_in_forked_multiprocess_process(self.config, self._writer_schema, self._message_name, datum))
