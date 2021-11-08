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
from karapace.protobuf.exception import IllegalArgumentException, ProtobufSchemaResolutionException
from karapace.protobuf.message_element import MessageElement
from karapace.protobuf.schema import ProtobufSchema

import importlib.util
import logging
import os

logger = logging.getLogger(__name__)


class ProtobufDatumReader():
    """Deserialize Avro-encoded data into a Python data structure."""

    @staticmethod
    def check_props(schema_one, schema_two, prop_list):
        for prop in prop_list:
            if getattr(schema_one, prop) != getattr(schema_two, prop):
                return False
        return True

    @staticmethod
    def match_schemas(writer_schema: ProtobufSchema, reader_schema: ProtobufSchema) -> bool:
        # TODO (serge): schema comparison by fields required

        if str(writer_schema) == str(reader_schema):
            return True
        return False

    def __init__(self, writer_schema=None, reader_schema=None):
        """
    As defined in the Avro specification, we call the schema encoded
    in the data the "writer's schema", and the schema expected by the
    reader the "reader's schema".
    """
        self._writer_schema = writer_schema
        self._reader_schema = reader_schema

    # read/write properties
    def set_writer_schema(self, writer_schema):
        self._writer_schema = writer_schema

    writer_schema = property(lambda self: self._writer_schema, set_writer_schema)

    def set_reader_schema(self, reader_schema):
        self._reader_schema = reader_schema

    reader_schema = property(lambda self: self._reader_schema, set_reader_schema)

    @staticmethod
    def read_varint(bio: BytesIO):
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
                # raise EOFError('EOF while reading varint, value is %i so far' %
                #               varint)

            byte = ord(char)
            varint += (byte & 0x7F) << (7 * read_bytes)

            read_bytes += 1

            if not byte & 0x80:
                return varint

    def read_indexes(self, bio: BytesIO):
        size: int = self.read_varint(bio)
        result = []
        if size == 0:
            result.append(0)
            return result
        i = 0
        while i < size:
            result.append(self.read_varint(bio))
            i += 1

    def read(self, bio: BytesIO):
        if self.reader_schema is None:
            self.reader_schema = self.writer_schema
        return self.read_data(self.writer_schema, self.reader_schema, bio)

    @staticmethod
    def find_message_name(schema: ProtobufSchema, indexes: list) -> str:
        result: list = []
        dot: bool = False
        types = schema.schema.types
        for index in indexes:
            if dot:
                result.append(".")
            else:
                dot = True

            try:
                message = types[index]
            except Exception:
                raise IllegalArgumentException(f"Invalid message indexes: {indexes}")

            if message and isinstance(message, MessageElement):
                result.append(message.name)
                types = message.nested_types
            else:
                raise IllegalArgumentException(f"Invalid message indexes: {indexes}")

        # for java we also need package name. But in case we will use protoc
        # for compiling to python we can ignore it at all

        return "".join(result)

    def read_data(self, writer_schema, reader_schema, bio: BytesIO):
        # TODO (serge): check and polish it
        if not ProtobufDatumReader.match_schemas(writer_schema, reader_schema):
            fail_msg = 'Schemas do not match.'
            raise ProtobufSchemaResolutionException(fail_msg, writer_schema, reader_schema)

        indexes = self.read_indexes(bio)
        name = self.find_message_name(writer_schema, indexes)

        with open("tmp.proto", "w") as proto_text:
            proto_text.write(str(writer_schema))
            proto_text.close()

        os.system("protoc --python_out=./ tmp.proto")

        spec = importlib.util.spec_from_file_location("tmp_pb2", "./tmp_pb2.py")
        tmp_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tmp_module)
        class_to_call = getattr(tmp_module, name)
        class_instance = class_to_call()
        class_instance.ParseFromString(bio.read())

        #
        return class_instance
