#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from io import BytesIO
from karapace.protobuf.exception import IllegalArgumentException, ProtobufSchemaResolutionException, \
    ProtobufTypeException
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.protobuf_to_dict import dict_to_protobuf, protobuf_to_dict
from karapace.protobuf.schema import ProtobufSchema
from karapace.protobuf.type_element import TypeElement

import hashlib
import importlib
import importlib.util
import logging
import os

ZERO_BYTE = b'\x00'

logger = logging.getLogger(__name__)


def calculate_class_name(name: str) -> str:
    return "c_" + hashlib.md5(name.encode('utf-8')).hexdigest()


class ProtobufDatumReader:
    """Deserialize Protobuf-encoded data into a Python data structure."""

    @staticmethod
    def check_props(schema_one, schema_two, prop_list):
        try:
            return all(getattr(schema_one, prop) == getattr(schema_two, prop) for prop in prop_list)
        except AttributeError:
            return False

    @staticmethod
    def match_schemas(writer_schema: ProtobufSchema, reader_schema: ProtobufSchema) -> bool:
        # TODO (serge): schema comparison by fields required

        return str(writer_schema) == str(reader_schema)

    def __init__(self, writer_schema=None, reader_schema=None):
        """ As defined in the Protobuf specification, we call the schema encoded
        in the data the "writer's schema", and the schema expected by the
        reader the "reader's schema".
        """
        self._writer_schema = writer_schema
        self._reader_schema = reader_schema

    @staticmethod
    def read_varint(bio: BytesIO) -> int:
        """Read a variable-length integer.

        :returns: Integer
        """
        varint = 0
        read_bytes = 0

        while True:
            char = bio.read(1)
            if len(char) == 0:
                if read_bytes == 0:
                    return 0
                raise EOFError(f"EOF while reading varint, value is {varint} so far")

            byte = ord(char)
            varint += (byte & 0x7F) << (7 * read_bytes)

            read_bytes += 1

            if not byte & 0x80:
                return varint

    def read_indexes(self, bio: BytesIO):
        try:
            size: int = self.read_varint(bio)
        except EOFError:
            # TODO: change exception
            raise IllegalArgumentException("problem with reading binary data")
        if size == 0:
            return [0]
        return [
            self.read_varint(bio)
            for _ in range(size)
        ]

    def read(self, bio: BytesIO):
        if self._reader_schema is None:
            self._reader_schema = self._writer_schema
        return protobuf_to_dict(self.read_data(self._writer_schema, self._reader_schema, bio), True)

    @staticmethod
    def find_message_name(schema: ProtobufSchema, indexes: List[int]) -> str:
        result: list = []
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

    def read_data(self, writer_schema, reader_schema, bio: BytesIO):
        # TODO (serge): check and polish it
        if not ProtobufDatumReader.match_schemas(writer_schema, reader_schema):
            fail_msg = 'Schemas do not match.'
            raise ProtobufSchemaResolutionException(fail_msg, writer_schema, reader_schema)

        indexes = self.read_indexes(bio)
        name = self.find_message_name(writer_schema, indexes)
        proto_name = calculate_class_name(str(writer_schema))
        with open(f"{proto_name}.proto", "w") as proto_text:
            proto_text.write(str(writer_schema))
            proto_text.close()

        os.system(f"protoc --python_out=./ {proto_name}.proto")

        spec = importlib.util.spec_from_file_location(f"{proto_name}_pb2", f"./{proto_name}_pb2.py")
        tmp_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tmp_module)
        class_to_call = getattr(tmp_module, name)
        class_instance = class_to_call()
        class_instance.ParseFromString(bio.read())

        return class_instance


class ProtobufDatumWriter:
    """ProtobufDatumWriter for generic python objects."""

    def __init__(self, writer_schema=None):
        self._writer_schema = writer_schema
        a: ProtobufSchema = writer_schema
        el: TypeElement
        self._message_name = ''
        for idx, el in enumerate(a.proto_file_element.types):
            if isinstance(el, MessageElement):
                self._message_name = el.name
                self._message_index = idx
                break

        if self._message_name == '':
            raise ProtobufTypeException("No message in protobuf schema")

    # read/write properties
    def set_writer_schema(self, writer_schema):
        self._writer_schema = writer_schema

    writer_schema = property(lambda self: self._writer_schema, set_writer_schema)

    @staticmethod
    def write_varint(bio: BytesIO, value):

        if value == 0:
            bio.write(ZERO_BYTE)
            return 1

        written_bytes = 0
        while value > 0:
            to_write = value & 0x7f
            value = value >> 7

            if value > 0:
                to_write |= 0x80

            bio.write(bytearray(to_write)[0])
            written_bytes += 1

        return written_bytes

    def write_indexes(self, bio: BytesIO, value):
        self.write_varint(bio, value)

    def write_index(self, writer: BytesIO):
        self.write_indexes(writer, self._message_index)

    def write(self, datum: dict, writer: BytesIO):
        # validate datum

        proto_name = calculate_class_name(str(self._writer_schema))
        with open(f"{proto_name}.proto", "w") as proto_text:
            proto_text.write(str(self._writer_schema))
            proto_text.close()

        os.system(f"protoc --python_out=./ {proto_name}.proto")
        name = self._message_name
        spec = importlib.util.spec_from_file_location(f"{proto_name}_pb2", f"./{proto_name}_pb2.py")
        tmp_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tmp_module)
        class_to_call = getattr(tmp_module, name)
        class_instance = class_to_call()

        try:
            dict_to_protobuf(class_instance, datum)
        except Exception:
            raise ProtobufTypeException(self._writer_schema, datum)

        writer.write(class_instance.SerializeToString())


if __name__ == '__main__':
    raise Exception('Not a standalone module')
