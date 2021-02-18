"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from enum import Enum, unique
from karapace.avro_compatibility import (
    ReaderWriterCompatibilityChecker as AvroChecker, SchemaCompatibilityType, SchemaIncompatibilityType
)
from karapace.schema_reader import SchemaType, TypedSchema

import logging

LOG = logging.getLogger(__name__)


@unique
class CompatibilityModes(Enum):
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"

    def is_transitive(self) -> bool:
        TRANSITIVE_MODES = {
            "BACKWARD_TRANSITIVE",
            "FORWARD_TRANSITIVE",
            "FULL_TRANSITIVE",
        }
        return self.value in TRANSITIVE_MODES


class IncompatibleSchema(Exception):
    pass


def check_avro_compatibility(reader_schema, writer_schema) -> None:
    result = AvroChecker().get_compatibility(reader=reader_schema, writer=writer_schema)
    if (
        result.compatibility is SchemaCompatibilityType.incompatible
        and [SchemaIncompatibilityType.missing_enum_symbols] != result.incompatibilities
    ):
        raise IncompatibleSchema(str(result.compatibility))


def check_compatibility(source: TypedSchema, target: TypedSchema, compatibility_mode: CompatibilityModes) -> None:
    if source.schema_type is not target.schema_type:
        raise IncompatibleSchema(f"Comparing different schema types: {source.schema_type} with {target.schema_type}")

    if compatibility_mode is CompatibilityModes.NONE:
        LOG.info("Compatibility level set to NONE, no schema compatibility checks performed")
        return

    if source.schema_type is SchemaType.AVRO:
        if compatibility_mode in {CompatibilityModes.BACKWARD, CompatibilityModes.BACKWARD_TRANSITIVE}:
            check_avro_compatibility(writer_schema=source.schema, reader_schema=target.schema)

        elif compatibility_mode in {CompatibilityModes.FORWARD, CompatibilityModes.FORWARD_TRANSITIVE}:
            check_avro_compatibility(writer_schema=target.schema, reader_schema=source.schema)

        elif compatibility_mode in {CompatibilityModes.FULL, CompatibilityModes.FULL_TRANSITIVE}:
            check_avro_compatibility(writer_schema=source.schema, reader_schema=target.schema)
            check_avro_compatibility(writer_schema=target.schema, reader_schema=source.schema)

    LOG.info("Unknow schema_type %r", source.schema_type)
