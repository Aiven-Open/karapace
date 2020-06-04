"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import avro.schema
import logging


class IncompatibleSchema(Exception):
    pass


class UnknownSchemaType(Exception):
    pass


class Compatibility:
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

    def __init__(self, source, target, compatibility):
        self.source = source
        self.target = target
        self.log = logging.getLogger("Compatibility")
        self.compatibility = compatibility
        self.log.info("Compatibility initialized with level: %r", self.compatibility)
        # Compatibility only checks between two versions, so we can drop the possible _TRANSITIONAL
        self._checking_for = compatibility.split("_")[0]

    def check(self):
        if self._checking_for == "NONE":
            self.log.info("Compatibility level set to NONE, no schema compatibility checks performed")
            return True
        return self.check_compatibility(self.source, self.target)

    def contains(self, field, target):  # pylint: disable=no-self-use
        return bool(field in self.get_schema_field(target))

    def check_same_name(self, source, target):  # pylint: disable=no-self-use
        return source.name == target.name

    @staticmethod
    def is_named_type(schema):
        if isinstance(schema, avro.schema.ArraySchema):
            return True
        if isinstance(schema, avro.schema.UnionSchema):
            return True
        if isinstance(schema, avro.schema.FixedSchema):
            return True
        if isinstance(schema, avro.schema.RecordSchema):
            return True
        if isinstance(schema, avro.schema.Field):
            return True
        return False

    def check_same_type(self, source, target):  # pylint: disable=no-self-use, too-many-return-statements
        """Returns info on if the types are the same and whether it's a basetype or not"""
        self.log.info("source: %s, target: %s", source, target)

        # Simple form presentation of values e.g. "int"
        if isinstance(source.type, str):
            if source.type in self._NUMBER_TYPES and target.type in self._NUMBER_TYPES:
                return True, True
            if source.type in self._STRING_TYPES and target.type in self._STRING_TYPES:
                return True, True
            return source.type == target.type, True

        self.log.info("source.type: %s, target.type: %s", type(source.type), type(target.type))

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
        if isinstance(source.type, avro.schema.ArraySchema):
            return isinstance(target.type, avro.schema.ArraySchema), False
        if isinstance(source.type, avro.schema.UnionSchema):
            return isinstance(target.type, avro.schema.UnionSchema), False
        if isinstance(source.type, avro.schema.MapSchema):
            return isinstance(target.type, avro.schema.MapSchema), False
        if isinstance(source.type, avro.schema.FixedSchema):
            return isinstance(target.type, avro.schema.FixedSchema), True
        raise IncompatibleSchema("Unhandled schema type: {}".format(type(source.type)))

    def check_type_promotion(self, source_type, target_type):
        if isinstance(source_type, str):  # Simple form presentation e.g. "int" so leave it as it is
            source_type_value = source_type
            target_type_value = target_type
        else:
            source_type_value = source_type.type
            target_type_value = target_type.type

        if source_type_value == target_type_value:
            # Complex type specific handling
            if isinstance(source_type, avro.schema.FixedSchema):
                return source_type.size == target_type.size and source_type.name == target_type.name
            if isinstance(source_type, avro.schema.EnumSchema):
                return source_type.name == target_type.name

            return True
        try:
            return self._TYPE_PROMOTION_RULES[self._checking_for][source_type_value][target_type_value]
        except KeyError:
            return False

    @staticmethod
    def get_schema_field(schema):  # pylint: disable=too-many-return-statements
        if isinstance(schema, tuple):  # Simple form of a Union.
            return schema
        if schema.type == "record":
            return schema.fields
        if schema.type == "array":
            return schema.items
        if schema.type == "map":
            return schema.values
        if schema.type == "union":
            return schema.schemas
        if schema.type == "enum":
            return schema.symbols
        return schema

    def check_simple_value(self, source, target):
        source_values = self.get_schema_field(source)
        target_values = self.get_schema_field(target)
        if not self.check_type_promotion(source_values, target_values):
            raise IncompatibleSchema("Incompatible type promotion {} {}".format(source_values.type, target_values.type))

    def extract_schema_if_union(self, source, target):
        source_union = isinstance(source, (avro.schema.UnionSchema, tuple))
        target_union = isinstance(target, (avro.schema.UnionSchema, tuple))
        found = False
        # Nothing to do here as neither is an union value
        if not source_union and not target_union:
            yield source, target

        # Unions and union compatibility with non-union types requires special handling so go through them here.
        elif source_union and target_union:
            target_idx_found = set()
            source_idx_found = set()
            source_schema_fields = self.get_schema_field(source)
            target_schema_fields = self.get_schema_field(target)
            for i, source_schema in enumerate(source_schema_fields):
                for j, target_schema in enumerate(target_schema_fields):
                    # some types are unhashable
                    if source_schema.type == target_schema.type and j not in target_idx_found and i not in source_idx_found:
                        target_idx_found.add(j)
                        source_idx_found.add(i)
                        yield source_schema, target_schema
            if len(target_idx_found) < len(target_schema_fields) and len(source_idx_found) < len(source_schema_fields):
                # sets overlap only
                raise IncompatibleSchema("Union types are incompatible")
            if len(target_idx_found) < len(target_schema_fields) and self._checking_for in {"FORWARD", "FULL"}:
                raise IncompatibleSchema("Previous union contains more types")
            if len(source_idx_found) < len(source_schema_fields) and self._checking_for in {"BACKWARD", "FULL"}:
                raise IncompatibleSchema("Previous union contains less types")

        elif source_union and not target_union:
            for schema in self.get_schema_field(source):
                if schema.type == target.type:
                    if self._checking_for in {"BACKWARD", "FULL"}:
                        raise IncompatibleSchema("Incompatible union for source: {} and target: {}".format(source, target))
                    yield schema, target
                    found = True
                    break
            if not found:
                raise IncompatibleSchema("Matching schema in union not found")

        elif not source_union and target_union:
            for schema in self.get_schema_field(target):
                if schema.type == source.type:
                    if self._checking_for in {"FORWARD", "FULL"}:
                        raise IncompatibleSchema("Incompatible union for source: {} and target: {}".format(source, target))
                    yield source, schema
                    found = True
                    break
            if not found:
                raise IncompatibleSchema("Matching schema in union not found")
        else:
            yield None, None

    def iterate_over_record_source_fields(self, source, target):
        for source_field in source.fields:
            if self.contains(source_field, target):  # this is an optimization to check for identical fields
                self.log.info("source_field: identical %s in both source and target: %s", source_field.name, target)
                continue
            # The fields aren't identical in both but could be similar enough (i.e. changed default)
            found = False
            for target_field in target.fields:
                if not self.is_named_type(target_field):
                    continue

                if not self.check_same_name(source_field, target_field):
                    continue

                same_type, base_type = self.check_same_type(source_field, target_field)
                if not same_type:  # different types
                    raise IncompatibleSchema(
                        "source_field.type: {} != target_field.type: {}".format(source_field.type, target_field.type)
                    )
                if not base_type:  # same type but a complex type
                    found = True
                    source_field_value = self.get_schema_field(source_field.type)
                    target_field_value = self.get_schema_field(target_field.type)
                    if isinstance(source_field_value, avro.schema.PrimitiveSchema):
                        self.check_simple_value(source_field_value, target_field_value)
                        break

                    # Simple presentation form for Union fields. Extract the correct schemas already here.
                    for source_field_value, target_field_value in self.extract_schema_if_union(
                        source_field_value, target_field_value
                    ):
                        self.log.info("Recursing source with: source: %s target: %s", source_field_value, target_field_value)
                        self.check_compatibility(source_field_value, target_field_value)
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

    def iterate_over_record_target_fields(self, source, target):
        for target_field in target.fields:
            if self.contains(target_field, source):
                self.log.info("target_field: %r in both source and target: %r", target_field.name, source)
                continue
            # The fields aren't identical in both but could be similar enough (i.e. changed default)
            found = False
            for source_field in source.fields:
                if not self.is_named_type(source_field):
                    continue

                if not self.check_same_name(source_field, target_field):
                    continue

                same_type, base_type = self.check_same_type(source_field, target_field)
                if not same_type:
                    raise IncompatibleSchema(
                        "source_field.type: {} != target_field.type: {}".format(source_field.type, target_field.type)
                    )
                if not base_type:
                    found = True
                    source_field_value = self.get_schema_field(source_field.type)
                    target_field_value = self.get_schema_field(target_field.type)
                    if isinstance(source_field_value, avro.schema.PrimitiveSchema):
                        self.check_simple_value(source_field_value, target_field_value)
                        break

                    for source_field_value, target_field_value in self.extract_schema_if_union(
                        source_field_value, target_field_value
                    ):
                        self.log.info("Recursing target with: source: %s target: %s", source_field_value, target_field_value)
                        self.check_compatibility(source_field_value, target_field_value)
                else:
                    found = True
                    self.log.info("source_field is: %s, target_field: %s added", source_field, target_field)
                    break

            if not found and self._checking_for in {"BACKWARD", "FULL"} and not target_field.has_default:
                raise IncompatibleSchema("Target field: {} added".format(target_field.name))

    def check_fields(self, source, target):
        if source.type == "record":
            self.iterate_over_record_source_fields(source, target)
            self.iterate_over_record_target_fields(source, target)
        elif source.type in {"array", "map", "union"}:
            source_field = self.get_schema_field(source)
            target_field = self.get_schema_field(target)
            if isinstance(source_field, avro.schema.PrimitiveSchema):
                self.check_simple_value(source, target)

            # Contains a complex type
            self.check_compatibility(source_field, target_field)
        elif source.type in {"fixed"}:
            self.check_simple_value(self.get_schema_field(source), self.get_schema_field(target))
        elif source.type in {"enum"}:
            # For enums the only qualification is that the name must match
            if source.name != target.name:
                raise IncompatibleSchema("Source name: {} must match target name: {}".format(source.name, target.name))
        elif isinstance(source, avro.schema.PrimitiveSchema):
            self.check_simple_value(self.get_schema_field(source), self.get_schema_field(target))
        else:
            raise UnknownSchemaType("Unknown schema type: {}".format(source.type))

    def check_compatibility(self, source, target):
        source_union = isinstance(source, (avro.schema.UnionSchema, tuple))
        target_union = isinstance(target, (avro.schema.UnionSchema, tuple))

        # Unions are the only exception where two different types are allowed
        same_type, _ = self.check_same_type(source, target)
        if not same_type and not (source_union or target_union):
            raise IncompatibleSchema("source {} and target {} different types".format(source, target))

        for source_f, target_f in self.extract_schema_if_union(source, target):
            self.check_fields(source_f, target_f)
