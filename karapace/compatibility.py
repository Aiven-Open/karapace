"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.avro_compatibility import ReaderWriterCompatibilityChecker as AvroChecker, SchemaCompatibilityType
from karapace.schema_reader import SchemaType, TypedSchema

import logging


class IncompatibleSchema(Exception):
    pass


class UnknownSchemaType(Exception):
    pass


class Compatibility:
    def __init__(self, source, target, compatibility):
        self.source = source
        self.target = target
        self.log = logging.getLogger("Compatibility")
        self.compatibility = compatibility
        self.log.info("Compatibility initialized with level: %r", self.compatibility)
        # Compatibility only checks between two versions, so we can drop the possible _TRANSITIONAL
        self._checking_for = compatibility.split("_")[0]

    def check(self) -> bool:  # pylint: disable=inconsistent-return-statements
        if self.source.schema_type is not self.target.schema_type:
            raise IncompatibleSchema(
                f"Comparing different schema types: {self.source.schema_type} with {self.target.schema_type}"
            )
        if self._checking_for == "NONE":
            self.log.info("Compatibility level set to NONE, no schema compatibility checks performed")
            return True
        if self.source.schema_type is SchemaType.AVRO:
            if self._checking_for in {"BACKWARD", "FULL"}:
                writer, reader = self.source.schema, self.target.schema
                compat = AvroChecker().get_compatibility(reader=reader, writer=writer).compatibility
                if compat is SchemaCompatibilityType.incompatible:
                    raise IncompatibleSchema(str(compat))
            if self._checking_for in {"FORWARD", "FULL"}:
                writer, reader = self.target.schema, self.source.schema
                compat = AvroChecker().get_compatibility(reader=reader, writer=writer).compatibility
                if compat is SchemaCompatibilityType.incompatible:
                    raise IncompatibleSchema(str(compat))
        if self.source.schema_type is SchemaType.JSONSCHEMA:
            return JsonSchemaCompatibility(self.source.schema, self.target.schema, self.compatibility).check()


class JsonSchemaCompatibility(Compatibility):
    def check_compatibility(self, source: TypedSchema, target: TypedSchema) -> bool:
        raise NotImplementedError("write me")

    def check(self) -> bool:
        return self.check_compatibility(self.source, self.target)
