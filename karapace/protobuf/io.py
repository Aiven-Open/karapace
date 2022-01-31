from io import BytesIO
from karapace import config
from karapace.protobuf.encoding_variants import read_indexes, write_indexes
from karapace.protobuf.exception import IllegalArgumentException, ProtobufSchemaResolutionException, ProtobufTypeException
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.protobuf_to_dict import dict_to_protobuf, protobuf_to_dict
from karapace.protobuf.schema import ProtobufSchema
from karapace.protobuf.type_element import TypeElement
from typing import Any, Dict, List

import hashlib
import importlib
import importlib.util
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def calculate_class_name(name: str) -> str:
    return "c_" + hashlib.md5(name.encode('utf-8')).hexdigest()


def match_schemas(writer_schema: ProtobufSchema, reader_schema: ProtobufSchema) -> bool:
    # TODO (serge): is schema comparison by fields required?

    return str(writer_schema) == str(reader_schema)


def find_message_name(schema: ProtobufSchema, indexes: List[int]) -> str:
    result: List[str] = []
    types = schema.proto_file_element.types
    for index in indexes:
        try:
            message = types[index]
        except IndexError:
            # pylint: disable=raise-missing-from
            raise IllegalArgumentException(f"Invalid message indexes: {indexes}")

        if message and isinstance(message, MessageElement):
            result.append(message.name)
            types = message.nested_types
        else:
            raise IllegalArgumentException(f"Invalid message indexes: {indexes}")

    # for java we also need package name. But in case we will use protoc
    # for compiling to python we can ignore it at all
    return ".".join(result)


def get_protobuf_class_instance(schema: ProtobufSchema, class_name: str, cfg: Dict) -> Any:
    directory = cfg["protobuf_runtime_directory"]
    proto_name = calculate_class_name(str(schema))
    proto_path = f"{directory}/{proto_name}.proto"
    class_path = f"{directory}/{proto_name}_pb2.py"
    if not os.path.isfile(proto_path):
        with open(f"{directory}/{proto_name}.proto", "w") as proto_text:
            proto_text.write(str(schema))

    if not os.path.isfile(class_path):
        subprocess.run([
            "protoc",
            "--python_out=./",
            proto_path,
        ], check=True)

    spec = importlib.util.spec_from_file_location(f"{proto_name}_pb2", class_path)
    tmp_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tmp_module)
    class_to_call = getattr(tmp_module, class_name)
    return class_to_call()


def read_data(writer_schema: ProtobufSchema, reader_schema: ProtobufSchema, bio: BytesIO) -> Any:
    if not match_schemas(writer_schema, reader_schema):
        fail_msg = 'Schemas do not match.'
        raise ProtobufSchemaResolutionException(fail_msg, writer_schema, reader_schema)

    indexes = read_indexes(bio)
    name = find_message_name(writer_schema, indexes)

    class_instance = get_protobuf_class_instance(writer_schema, name, config.DEFAULTS)
    class_instance.ParseFromString(bio.read())

    return class_instance


class ProtobufDatumReader:
    """Deserialize Protobuf-encoded data into a Python data structure."""

    def __init__(self, writer_schema: ProtobufSchema = None, reader_schema: ProtobufSchema = None) -> None:
        """ As defined in the Protobuf specification, we call the schema encoded
        in the data the "writer's schema", and the schema expected by the
        reader the "reader's schema".
        """
        self._writer_schema = writer_schema
        self._reader_schema = reader_schema

    def read(self, bio: BytesIO) -> None:
        if self._reader_schema is None:
            self._reader_schema = self._writer_schema
        return protobuf_to_dict(read_data(self._writer_schema, self._reader_schema, bio), True)


class ProtobufDatumWriter:
    """ProtobufDatumWriter for generic python objects."""

    def __init__(self, writer_schema: ProtobufSchema = None):
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

    def write(self, datum: dict, writer: BytesIO) -> None:

        class_instance = get_protobuf_class_instance(self._writer_schema, self._message_name, config.DEFAULTS)

        try:
            dict_to_protobuf(class_instance, datum)
        except Exception:
            # pylint: disable=raise-missing-from
            raise ProtobufTypeException(self._writer_schema, datum)

        writer.write(class_instance.SerializeToString())
