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
from karapace.core.compatibility import CompatibilityModes
from karapace.core.compatibility.jsonschema.checks import compatibility as jsonschema_compatibility, incompatible_schema
from karapace.core.compatibility.jsonschema.utils import find_unsupported_compat_keywords
from karapace.core.compatibility.protobuf.checks import check_protobuf_schema_compatibility
from karapace.core.protobuf.schema import ProtobufSchema
from karapace.core.schema_models import ParsedTypedSchema, ValidatedTypedSchema
from karapace.core.schema_type import SchemaType
from karapace.core.typing import JsonSchemaValidator
from karapace.core.utils import assert_never

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
            # The parsed schema may be any JSON Schema draft validator (Draft-7 default,
            # or 2019-09 / 2020-12 when selected via $schema). Renamed/relocated keywords are
            # canonicalized to Draft-7 shapes during normalization so the engine compares them
            # correctly across drafts. Keywords the engine cannot yet evaluate are rejected below
            # rather than mis-judged.
            assert isinstance(old_schema.schema, JsonSchemaValidator)
            assert isinstance(new_schema.schema, JsonSchemaValidator)

            # Fail closed: reject rather than return a possibly-wrong verdict for keywords whose
            # compatibility semantics are not implemented. NONE mode already returned above, so this
            # only affects compatibility-checked modes.
            unsupported = find_unsupported_compat_keywords(old_schema.schema.schema) | find_unsupported_compat_keywords(
                new_schema.schema.schema
            )
            if unsupported:
                keywords = ", ".join(sorted(unsupported))
                return incompatible_schema(
                    incompat_type=SchemaIncompatibilityType.type_mismatch,
                    message=(
                        f"Compatibility checking is not supported for JSON Schema keyword(s): {keywords}. "
                        "Register the schema under a subject with compatibility NONE, or remove these keywords."
                    ),
                    location=[],
                )

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
    def check_jsonschema_compatibility(
        reader: JsonSchemaValidator, writer: JsonSchemaValidator
    ) -> SchemaCompatibilityResult:
        return jsonschema_compatibility(reader, writer)

    @staticmethod
    def check_protobuf_compatibility(reader: ProtobufSchema, writer: ProtobufSchema) -> SchemaCompatibilityResult:
        return check_protobuf_schema_compatibility(reader, writer)
