"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.avro_compatibility import (
    ReaderWriterCompatibilityChecker as AvroChecker, SchemaCompatibilityType, SchemaIncompatibilityType
)
from karapace.schema_reader import SchemaType, TypedSchema

import logging

LOG = logging.getLogger(__name__)


class IncompatibleSchema(Exception):
    pass


def check_compatibility(source: TypedSchema, target: TypedSchema, compatibility: str) -> bool:
    if source.schema_type is not target.schema_type:
        raise IncompatibleSchema(f"Comparing different schema types: {source.schema_type} with {target.schema_type}")

    # Compatibility only checks between two versions, so we can drop the possible _TRANSITIVE
    checking_for = compatibility.split("_")[0]
    if checking_for == "NONE":
        LOG.info("Compatibility level set to NONE, no schema compatibility checks performed")
        return True

    if source.schema_type is SchemaType.AVRO:
        if checking_for in {"BACKWARD", "FULL"}:
            writer, reader = source.schema, target.schema
            result = AvroChecker().get_compatibility(reader=reader, writer=writer)
            if (
                result.compatibility is SchemaCompatibilityType.incompatible
                and [SchemaIncompatibilityType.missing_enum_symbols] != result.incompatibilities
            ):
                raise IncompatibleSchema(str(result.compatibility))
        if checking_for in {"FORWARD", "FULL"}:
            writer, reader = target.schema, source.schema
            result = AvroChecker().get_compatibility(reader=reader, writer=writer)
            if (
                result.compatibility is SchemaCompatibilityType.incompatible
                and [SchemaIncompatibilityType.missing_enum_symbols] != result.incompatibilities
            ):
                raise IncompatibleSchema(str(result.compatibility))
