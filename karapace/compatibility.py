"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.schema_reader import SchemaType, TypedSchema

import avro.schema
import logging


class IncompatibleSchema(Exception):
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
            return AvroCompatibility(self.source.schema, self.target.schema, self.compatibility).check()
        if self.source.schema_type is SchemaType.JSONSCHEMA:
            return JsonSchemaCompatibility(self.source.schema, self.target.schema, self.compatibility).check()


class JsonSchemaCompatibility(Compatibility):
    def check_compatibility(self, source: TypedSchema, target: TypedSchema) -> bool:
        raise NotImplementedError("write me")

    def check(self) -> bool:
        return self.check_compatibility(self.source, self.target)


class AvroCompatibility(Compatibility):
    _TYPE_PROMOTION_RULES = {
        # Follow promotion rules in schema resolution section of:
        # https://avro.apache.org/docs/current/spec.html#schemas
        "BACKWARD": {
            "bytes": {
                "bytes": True,
                "string": True
            },
            "double": {
                "int": False
            },
            "float": {
                "int": False
            },
            "int": {
                "double": True,
                "float": True,
                "int": True,
                "long": True
            },
            "long": {
                "int": False
            },
            "string": {
                "bytes": True
            }
        },
        "FORWARD": {
            "bytes": {
                "bytes": True,
                "string": True
            },
            "double": {
                "int": True
            },
            "float": {
                "int": True
            },
            "int": {
                "double": False,
                "float": False,
                "int": True,
                "long": False
            },
            "long": {
                "int": True
            },
            "string": {
                "bytes": True
            }
        },
        "FULL": {
            "bytes": {
                "bytes": True,
                "string": True
            },
            "double": {
                "int": False
            },
            "float": {
                "int": False
            },
            "int": {
                "double": False,
                "float": False,
                "int": True,
                "long": False
            },
            "long": {
                "int": False
            },
            "string": {
                "bytes": True
            }
        }
    }

    _NUMBER_TYPES = {"int", "long", "float", "double"}
    _STRING_TYPES = {"string", "bytes"}

    def check(self) -> bool:
        if self._checking_for == "NONE":
            self.log.info("Compatibility level set to NONE, no schema compatibility checks performed")
            return True
        return self.check_compatibility(self.source, self.target)

    def contains(self, field, target):  # pylint: disable=no-self-use
        return bool(field in target.fields)

    def check_same_name(self, source, target):  # pylint: disable=no-self-use
        return source.name == target.name

    def check_same_type(self, source, target):  # pylint: disable=no-self-use, too-many-return-statements
        """Returns info on if the types are the same and whether it's a basetype or not"""
        self.log.error("source: %s, target: %s", type(source.type), type(target.type))
        self.log.error("source: %s, target: %s", source, target)

        if isinstance(source.type, str):
            return source.type == target.type, True
        if isinstance(source.type, avro.schema.PrimitiveSchema):
            if isinstance(target.type, avro.schema.PrimitiveSchema):
                # Check if the types are compatible, actual promotion rules are checked separately
                # via check_type_promotion()
                if source.type.type in self._NUMBER_TYPES and target.type.type in self._NUMBER_TYPES:
                    return True, True
                if source.type.type in self._STRING_TYPES and target.type.type in self._STRING_TYPES:
                    return True, True
                return source.type.type == target.type.type, True
            return False, True
        if isinstance(source.type, avro.schema.RecordSchema):
            return isinstance(target.type, avro.schema.RecordSchema), False
        if isinstance(source.type, avro.schema.EnumSchema):
            return isinstance(target.type, avro.schema.EnumSchema), True
        raise IncompatibleSchema("Unhandled schema type: {}".format(type(source.type)))

    def check_type_promotion(self, source_type, target_type):
        if source_type.type == target_type.type:
            return True
        try:
            return self._TYPE_PROMOTION_RULES[self._checking_for][source_type.type][target_type.type]
        except KeyError:
            return False

    def check_source_field(self, source, target):
        for source_field in source.fields:
            if self.contains(source_field, target):  # this is an optimization to check for identical fields
                self.log.info("source_field: identical %s in both source and target: %s", source_field.name, target)
                continue
            # The fields aren't identical in both but could be similar enough (i.e. changed default)
            found = False
            for target_field in target.fields:
                if self.check_same_name(source_field, target_field):
                    # Ok we found the same named fields
                    same_type, base_type = self.check_same_type(source_field, target_field)
                    if not same_type:  # different types
                        raise IncompatibleSchema(
                            "source_field.type: {} != target_field.type: {}".format(source_field.type, target_field.type)
                        )
                    if not base_type:  # same type but nested structure
                        self.log.info("Recursing source with: source: %s target: %s", source_field, target_field)
                        self.check_compatibility(source_field.type, target_field.type)
                    else:
                        if not self.check_type_promotion(source_field.type, target_field.type):
                            raise IncompatibleSchema(
                                "Incompatible type promotion {} {}".format(source_field.type.type, target_field.type.type)
                            )
                        found = True
                        break
            if not found:
                self.log.info("source_field: %s removed from: %s", source_field.name, target)
                if not found and self._checking_for in {"FORWARD", "FULL"} and not source_field.has_default:
                    raise IncompatibleSchema("Source field: {} removed".format(source_field.name))

    def check_target_field(self, source, target):
        for target_field in target.fields:
            if self.contains(target_field, source):
                self.log.info("target_field: %r in both source and target: %r", target_field.name, source)
                continue
            # The fields aren't identical in both but could be similar enough (i.e. changed default)
            found = False
            for source_field in source.fields:
                if self.check_same_name(source_field, target_field):
                    same_type, base_type = self.check_same_type(source_field, target_field)
                    if not same_type:
                        raise IncompatibleSchema(
                            "source_field.type: {} != target_field.type: {}".format(source_field.type, target_field.type)
                        )
                    if not base_type:
                        self.log.info("Recursing target with: source: %s target: %s", source_field, target_field)
                        self.check_compatibility(source_field.type, target_field.type)
                    else:
                        found = True
                        self.log.info("source_field is: %s, target_field: %s added", source_field, target_field)
                        break

            if not found and self._checking_for in {"BACKWARD", "FULL"} and not target_field.has_default:
                raise IncompatibleSchema("Target field: {} added".format(target_field.name))

    def check_compatibility(self, source, target):
        same_type, _ = self.check_same_type(source, target)
        if not same_type:
            raise IncompatibleSchema("source {} and target {} different types".format(source, target))

        if source.type == "record":
            self.check_source_field(source, target)
            self.check_target_field(source, target)
