"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from avro.compatibility import (
    merge,
    ReaderWriterCompatibilityChecker as AvroChecker,
    SchemaCompatibilityResult,
    SchemaCompatibilityType,
    SchemaIncompatibilityType,
)
from avro.schema import Schema as AvroSchema
from jsonschema import Draft7Validator
from karapace.compatibility import CompatibilityModes
from karapace.compatibility.jsonschema.checks import compatibility as jsonschema_compatibility, incompatible_schema
from karapace.compatibility.protobuf.checks import check_protobuf_schema_compatibility
from karapace.protobuf.schema import ProtobufSchema
from karapace.schema_models import ParsedTypedSchema, ValidatedTypedSchema
from karapace.schema_type import SchemaType
from karapace.utils import assert_never

import logging

LOG = logging.getLogger(__name__)


class SchemaCompatibility:
    @staticmethod
    def check_compatibility(
        old_schema: ParsedTypedSchema,
        new_schema: ValidatedTypedSchema,
        compatibility_mode: CompatibilityModes,
    ) -> SchemaCompatibilityResult:
        """Check that `old_schema` and `new_schema` are compatible under `compatibility_mode`."""

        if compatibility_mode is CompatibilityModes.NONE:
            LOG.info("Compatibility level set to NONE, no schema compatibility checks performed")
            return SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

        if old_schema.schema_type is not new_schema.schema_type:
            return incompatible_schema(
                incompat_type=SchemaIncompatibilityType.type_mismatch,
                message=f"Comparing different schema types: {old_schema.schema_type} with {new_schema.schema_type}",
                location=[],
            )

        if old_schema.schema_type is SchemaType.AVRO:
            assert isinstance(old_schema.schema, AvroSchema)
            assert isinstance(new_schema.schema, AvroSchema)
            if compatibility_mode in {CompatibilityModes.BACKWARD, CompatibilityModes.BACKWARD_TRANSITIVE}:
                result = SchemaCompatibility.check_avro_compatibility(
                    reader_schema=new_schema.schema,
                    writer_schema=old_schema.schema,
                )
            elif compatibility_mode in {CompatibilityModes.FORWARD, CompatibilityModes.FORWARD_TRANSITIVE}:
                result = SchemaCompatibility.check_avro_compatibility(
                    reader_schema=old_schema.schema,
                    writer_schema=new_schema.schema,
                )
            elif compatibility_mode in {CompatibilityModes.FULL, CompatibilityModes.FULL_TRANSITIVE}:
                result = SchemaCompatibility.check_avro_compatibility(
                    reader_schema=new_schema.schema,
                    writer_schema=old_schema.schema,
                )
                result = merge(
                    result,
                    SchemaCompatibility.check_avro_compatibility(
                        reader_schema=old_schema.schema,
                        writer_schema=new_schema.schema,
                    ),
                )
        elif old_schema.schema_type is SchemaType.JSONSCHEMA:
            assert isinstance(old_schema.schema, Draft7Validator)
            assert isinstance(new_schema.schema, Draft7Validator)
            if compatibility_mode in {CompatibilityModes.BACKWARD, CompatibilityModes.BACKWARD_TRANSITIVE}:
                result = SchemaCompatibility.check_jsonschema_compatibility(
                    reader=new_schema.schema,
                    writer=old_schema.schema,
                )
            elif compatibility_mode in {CompatibilityModes.FORWARD, CompatibilityModes.FORWARD_TRANSITIVE}:
                result = SchemaCompatibility.check_jsonschema_compatibility(
                    reader=old_schema.schema,
                    writer=new_schema.schema,
                )
            elif compatibility_mode in {CompatibilityModes.FULL, CompatibilityModes.FULL_TRANSITIVE}:
                result = SchemaCompatibility.check_jsonschema_compatibility(
                    reader=new_schema.schema,
                    writer=old_schema.schema,
                )
                result = merge(
                    result,
                    SchemaCompatibility.check_jsonschema_compatibility(
                        reader=old_schema.schema,
                        writer=new_schema.schema,
                    ),
                )
        elif old_schema.schema_type is SchemaType.PROTOBUF:
            assert isinstance(old_schema.schema, ProtobufSchema)
            assert isinstance(new_schema.schema, ProtobufSchema)
            if compatibility_mode in {CompatibilityModes.BACKWARD, CompatibilityModes.BACKWARD_TRANSITIVE}:
                result = SchemaCompatibility.check_protobuf_compatibility(
                    reader=new_schema.schema,
                    writer=old_schema.schema,
                )
            elif compatibility_mode in {CompatibilityModes.FORWARD, CompatibilityModes.FORWARD_TRANSITIVE}:
                result = SchemaCompatibility.check_protobuf_compatibility(
                    reader=old_schema.schema,
                    writer=new_schema.schema,
                )

            elif compatibility_mode in {CompatibilityModes.FULL, CompatibilityModes.FULL_TRANSITIVE}:
                result = SchemaCompatibility.check_protobuf_compatibility(
                    reader=new_schema.schema,
                    writer=old_schema.schema,
                )
                result = merge(
                    result,
                    SchemaCompatibility.check_protobuf_compatibility(
                        reader=old_schema.schema,
                        writer=new_schema.schema,
                    ),
                )
        else:
            assert_never(f"Unknown schema_type {old_schema.schema_type}")

        return result

    @staticmethod
    def check_avro_compatibility(reader_schema: AvroSchema, writer_schema: AvroSchema) -> SchemaCompatibilityResult:
        return AvroChecker().get_compatibility(reader=reader_schema, writer=writer_schema)

    @staticmethod
    def check_jsonschema_compatibility(reader: Draft7Validator, writer: Draft7Validator) -> SchemaCompatibilityResult:
        return jsonschema_compatibility(reader, writer)

    @staticmethod
    def check_protobuf_compatibility(reader: ProtobufSchema, writer: ProtobufSchema) -> SchemaCompatibilityResult:
        return check_protobuf_schema_compatibility(reader, writer)
