"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from enum import Enum, unique
from jsonschema import Draft7Validator
from karapace.avro_compatibility import (
    ReaderWriterCompatibilityChecker as AvroChecker, SchemaCompatibilityResult, SchemaCompatibilityType,
    SchemaIncompatibilityType
)
from karapace.compatibility.jsonschema.checks import compatibility as jsonschema_compatibility
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


def check_avro_compatibility(reader_schema, writer_schema) -> SchemaCompatibilityResult:
    result = AvroChecker().get_compatibility(reader=reader_schema, writer=writer_schema)
    if (
        result.compatibility is SchemaCompatibilityType.incompatible
        and [SchemaIncompatibilityType.missing_enum_symbols] != result.incompatibilities
    ):
        return result

    return SchemaCompatibilityResult.compatible()


def check_jsonschema_compatibility(reader: Draft7Validator, writer: Draft7Validator) -> SchemaCompatibilityResult:
    return jsonschema_compatibility(reader, writer)


def check_compatibility(
    source: TypedSchema, target: TypedSchema, compatibility_mode: CompatibilityModes
) -> SchemaCompatibilityResult:
    if source.schema_type is not target.schema_type:
        return SchemaCompatibilityResult.incompatible(
            incompat_type=SchemaIncompatibilityType.type_mismatch,
            message=f"Comparing different schema types: {source.schema_type} with {target.schema_type}",
            location=[],
        )

    if compatibility_mode is CompatibilityModes.NONE:
        LOG.info("Compatibility level set to NONE, no schema compatibility checks performed")
        return SchemaCompatibilityResult.compatible()

    if source.schema_type is SchemaType.AVRO:
        if compatibility_mode in {CompatibilityModes.BACKWARD, CompatibilityModes.BACKWARD_TRANSITIVE}:
            result = check_avro_compatibility(reader_schema=target.schema, writer_schema=source.schema)

        elif compatibility_mode in {CompatibilityModes.FORWARD, CompatibilityModes.FORWARD_TRANSITIVE}:
            result = check_avro_compatibility(reader_schema=source.schema, writer_schema=target.schema)

        elif compatibility_mode in {CompatibilityModes.FULL, CompatibilityModes.FULL_TRANSITIVE}:
            result = check_avro_compatibility(reader_schema=target.schema, writer_schema=source.schema)
            result = result.merged_with(check_avro_compatibility(reader_schema=source.schema, writer_schema=target.schema))

    elif source.schema_type is SchemaType.JSONSCHEMA:
        if compatibility_mode in {CompatibilityModes.BACKWARD, CompatibilityModes.BACKWARD_TRANSITIVE}:
            result = check_jsonschema_compatibility(reader=target.schema, writer=source.schema)

        elif compatibility_mode in {CompatibilityModes.FORWARD, CompatibilityModes.FORWARD_TRANSITIVE}:
            result = check_jsonschema_compatibility(reader=source.schema, writer=target.schema)

        elif compatibility_mode in {CompatibilityModes.FULL, CompatibilityModes.FULL_TRANSITIVE}:
            result = check_jsonschema_compatibility(reader=target.schema, writer=source.schema)
            result = result.merged_with(check_jsonschema_compatibility(reader=source.schema, writer=target.schema))

    else:
        result = SchemaCompatibilityResult.incompatible(
            incompat_type=SchemaIncompatibilityType.type_mismatch,
            message=f"Unknow schema_type {source.schema_type}",
            location=[],
        )

    return result
