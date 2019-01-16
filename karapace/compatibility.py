"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
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

    def check(self):
        if self.compatibility == "NONE":
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
                # Follow promotion rules in schema resolution section of:
                # https://avro.apache.org/docs/current/spec.html#schemas
                if source.type.type == "int" and target.type.type in {"int", "long", "float", "double"}:
                    return True, True
                if source.type.type == "long" and target.type.type in {"long", "float", "double"}:
                    return True, True
                if source.type.type == "float" and target.type.type in {"float", "double"}:
                    return True, True
                if source.type.type == "string" and target.type.type in {"string", "bytes"}:
                    return True, True
                return source.type.type == target.type.type, True
            return False, True
        if isinstance(source.type, avro.schema.RecordSchema):
            return isinstance(target.type, avro.schema.RecordSchema), False
        if isinstance(source.type, avro.schema.EnumSchema):
            return isinstance(target.type, avro.schema.EnumSchema), True
        raise IncompatibleSchema("Unhandled schema type: {}".format(type(source.type)))

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
                        found = True
                        break
            if not found:
                self.log.info("source_field: %s removed from: %s", source_field.name, target)
                if not found and self.compatibility in {"FORWARD", "FULL"} and not source_field.has_default:
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

            if not found and self.compatibility in {"BACKWARD", "FULL"} and not target_field.has_default:
                raise IncompatibleSchema("Target field: {} added".format(target_field.name))

    def check_compatibility(self, source, target):
        same_type, _ = self.check_same_type(source, target)
        if not same_type:
            raise IncompatibleSchema("source {} and target {} different types".format(source, target))

        if source.type == "record":
            self.check_source_field(source, target)
            self.check_target_field(source, target)
