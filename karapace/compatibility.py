"""
karapace - schema compatibility checking

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from avro.schema import Schema as AvroSchema
from karapace.schema_reader import SchemaType, TypedSchema
from math import inf
from typing import Union

import avro.schema
import logging

min_val, max_val = -1 * inf, inf


class IncompatibleSchema(Exception):
    pass


class UnknownSchemaType(Exception):
    pass


class Compatibility:
    def __init__(
        self, source: Union[TypedSchema, AvroSchema, dict], target: Union[TypedSchema, AvroSchema, dict], compatibility: str
    ):
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
            return JsonSchemaCompatibility(
                target=self.source.to_json(), source=self.target.to_json(), compatibility=self.compatibility
            ).check()


class JsonSchemaCompatibility(Compatibility):
    @staticmethod
    def are_same_type(source: Union[dict, str], target: Union[dict, str]) -> bool:
        return JsonSchemaCompatibility.get_type_info(source) == JsonSchemaCompatibility.get_type_info(target)

    @staticmethod
    def get_type_info(source: Union[dict, str]) -> (bool, str):
        """
        :param source: a type object, either a string for primitives or a dict for all
        :return: tuple representing weather the type is complex or base on the first element and the type name on the second
        """
        # objects and unions are considered complex types
        if isinstance(source, str):
            source = {"type": source}
        is_base_type = source["type"] not in ["object", "array"] and not isinstance(source["type"], list)
        type_name = source["type"] if not isinstance(source["type"], list) else "union"
        # we'll pretend both number types are the same , and do the actual in the base type check method
        return is_base_type, type_name if type_name != "integer" else "number"

    def check_compatibility(self, source, target, checking_for=None) -> bool:
        checking_for = checking_for or self._checking_for
        self.log.debug("Checking %r against %r", source, target)
        if not self.are_same_type(source, target):
            self.log.debug("%r and %r are not the same", source, target)
            raise IncompatibleSchema(f"Source {source} and target {target} have different types")

        return self.check_same_type(source, target, checking_for)

    @staticmethod
    def can_read_base(source, target, field_type):
        if field_type in {"number", "integer"}:
            return JsonSchemaCompatibility.can_read_number(source, target)
        if field_type == "string":
            return JsonSchemaCompatibility.can_read_string(source, target)
        if field_type in {"boolean", "null"}:
            return True
        raise UnknownSchemaType(f"Unknown field type {field_type}")

    @staticmethod
    def can_read_number(source: dict, target: dict) -> bool:
        if isinstance(source, str):
            source = {"type": source}
        if isinstance(target, str):
            target = {"type": target}
        if source["type"] == "integer" and target["type"] != "integer":
            raise IncompatibleSchema(f"{source} cannot read {target}")
        source_multiple, target_multiple = source.get("multipleOf"), target.get("multipleOf")
        if source_multiple and (
            not target_multiple or target_multiple / source_multiple != int(target_multiple / source_multiple)
        ):
            raise IncompatibleSchema(
                f"Source multiple key {source_multiple} is not an exact multiple of target multiple {target_multiple}"
            )
        for min_lim_key in ["minimum", "exclusiveMinimum"]:
            source_lim, target_lim = source.get(min_lim_key, min_val), target.get(min_lim_key, min_val)
            if source_lim > target_lim:
                raise IncompatibleSchema(
                    f"Source {min_lim_key} {source_lim} is larger that target {min_lim_key} {target_lim}"
                )

        for max_lim_key in ["maximum", "exclusiveMaximum"]:
            source_lim, target_lim = source.get(max_lim_key, max_val), target.get(max_lim_key, max_val)
            if source_lim < target_lim:
                raise IncompatibleSchema(
                    f"Source {max_lim_key} {source_lim} is smaller that target {max_lim_key} {target_lim}"
                )
        return True

    @staticmethod
    def can_read_string(source: Union[dict, str], target: Union[dict, str]) -> bool:
        if isinstance(source, str):
            source = {"type": source}
        if isinstance(target, str):
            target = {"type": target}
        source_enum, target_enum = set(source.get("enum", [])), set(target.get("enum", []))
        if len(target_enum.difference(source_enum)) > 0:
            raise IncompatibleSchema(
                f"Source enum values do {source_enum} do not fully include target enum values {target_enum}"
            )
        for format_key in ["pattern", "format"]:
            source_pattern, target_pattern = source.get(format_key), target.get(format_key)
            if source_pattern != target_pattern:
                raise IncompatibleSchema(
                    f"Source {format_key} {source_pattern} is different from target {format_key} {target_pattern}"
                )
        source_min_len, target_min_len = source.get("minLength", 0), target.get("minLength", 0)
        if source_min_len > target_min_len:
            raise IncompatibleSchema(f"Source min len {source_min_len} is not greater than target min len {target_min_len}")
        source_max_len, target_max_len = source.get("maxLength", 0), target.get("maxLength", 0)
        if source_max_len < target_max_len:
            raise IncompatibleSchema(f"Source max len {source_max_len} is not greater than target max len {target_max_len}")
        return True

    def can_read_objects(self, source: dict, target: dict, checking_for: str) -> bool:
        if source.get("propertyNames") != target.get("propertyNames"):
            raise IncompatibleSchema(f"{source} and {target} have different values for propertyNames")
        if source.get("minProperties", 0) > target.get("minProperties", 0):
            raise IncompatibleSchema(f"{source} does not cover {target} on minProperties")
        if source.get("maxProperties", 0) < target.get("maxProperties", 0):
            raise IncompatibleSchema(f"{source} does not cover {target} on maxProperties")
        source_additional_props = source.get("additionalProperties")
        target_additional_props = target.get("additionalProperties")
        if source_additional_props is False and target_additional_props is not False:
            raise IncompatibleSchema(f"{source} does not cover {target} on additionalProperties")
        if target_additional_props is True and source_additional_props is not True:
            raise IncompatibleSchema(f"{source} does not cover {target} on additionalProperties")
        if isinstance(source_additional_props, dict) and isinstance(target_additional_props, dict):
            self.log.debug("Recursing for additional properties on %r and %r", source, target)
            self.check_compatibility(source_additional_props, target_additional_props, checking_for)
        return True
        # no patternProperties, no dependencies

    @staticmethod
    def can_read_unions(source, target, checking_for: str):  # pylint: disable=unused-argument
        return True

    @staticmethod
    def can_read_array(source: dict, target: dict, checking_for: str) -> bool:  # pylint: disable=unused-argument
        if source.get("minItems", 0) > target.get("minItems", 0):
            raise IncompatibleSchema(f"{source} does not cover {target} on minItems")
        if source.get("maxItems", 0) < target.get("maxItems", 0):
            raise IncompatibleSchema(f"{source} does not cover {target} on maxItems")
        if source.get("uniqueItems") and not target.get("uniqueItems"):
            raise IncompatibleSchema(f"{source} has unique items enabled while {target} does not")
        return True

    def can_read_complex(self, source, target, field_type, checking_for: str) -> bool:
        validators = {"array": self.can_read_array, "object": self.can_read_objects, "union": self.can_read_unions}
        can_read, can_be_read = True, True
        # we change the order of the source and target and reverse the check_for param
        # in case we encounter deeper properties
        if checking_for in {"FULL", "BACKWARD"}:
            recurse_check_for = "FULL" if checking_for == "FULL" else "FORWARD"
            can_read = validators[field_type](source, target, recurse_check_for)
        if checking_for in {"FULL", "FORWARD"}:
            recurse_check_for = "FULL" if checking_for == "FULL" else "BACKWARD"
            can_be_read = validators[field_type](target, source, recurse_check_for)
        if not can_read and can_be_read:
            raise IncompatibleSchema(f"Fields {source} and {target} contain incompatible properties")
        return True

    @staticmethod
    def get_fields(source):
        if source["type"] == "object":
            props = source.get("properties", {})
            return [(field, props[field]) for field in props]
        if source["type"] == "array":
            # how do we handle contains fields??
            fields = source["items"] if isinstance(source["items"], list) else [source["items"]]
            return [(None, item) for item in fields]
        if isinstance(source["type"], list):
            return [(None, field) for field in source["type"]]
        raise UnknownSchemaType(f"Unknown complex type: {source}")

    def check_same_type(self, source, target, checking_for: str) -> bool:
        self.log.debug("Checking same type %r against %r", source, target)
        is_base_type, type_name = self.get_type_info(source)
        if is_base_type:
            can_read, can_be_read = True, True
            if checking_for in {"BACKWARD", "FULL"}:
                can_read = self.can_read_base(source=source, target=target, field_type=type_name)
            if checking_for in {"FORWARD", "FULL"}:
                can_be_read = self.can_read_base(source=target, target=source, field_type=type_name)
            return can_read and can_be_read
        if not self.can_read_complex(source, target, type_name, checking_for):
            raise IncompatibleSchema(f"{source} and {target} are not compatible")

        if type_name == "union":
            unhandled_source_indexes, unhandled_target_indexes = self.handle_union(checking_for, source, target)
        else:
            unhandled_source_indexes, unhandled_target_indexes = self.handle_array_or_object(checking_for, source, target)

        if (checking_for in {"FULL", "FORWARD"} or type_name == "object") and len(unhandled_source_indexes) > 0:
            self.log.debug("Fields %r are missing from %r  for %s compatibility", source, target, self._checking_for)
            raise IncompatibleSchema(f"{source} incompatible with {target} with the compatibility {self._checking_for}")
        if (checking_for in {"FULL", "BACKWARD"} or type_name == "object") and len(unhandled_target_indexes) > 0:
            self.log.debug("Fields %r are missing from %r  for %s compatibility", target, source, self._checking_for)
            raise IncompatibleSchema(f"{target} incompatible with {source} with the compatibility {self._checking_for}")
        return True

    def handle_union(self, checking_for, source, target):
        source_fields = self.get_fields(source)
        target_fields = self.get_fields(target)
        unhandled_target_indexes = set(range(len(target_fields)))
        unhandled_source_indexes = set(range(len(source_fields)))
        for i, ((_, source_field), (_, target_field)) in enumerate(zip(source_fields, target_fields)):
            if not self.are_same_type(source_field, target_field):
                continue
            self.check_compatibility(source_field, target_field, checking_for)
            unhandled_source_indexes.remove(i)
            unhandled_target_indexes.remove(i)
        return unhandled_source_indexes, unhandled_target_indexes

    def handle_array_or_object(self, checking_for, source, target):
        source_fields = self.get_fields(source)
        target_fields = self.get_fields(target)
        unhandled_target_indexes = set(range(len(target_fields)))
        unhandled_source_indexes = set(range(len(source_fields)))
        for i, (source_name, source_field) in enumerate(source_fields):
            if i not in unhandled_source_indexes:
                continue
            for j, (target_name, target_field) in enumerate(target_fields):
                if j not in unhandled_target_indexes:
                    continue
                if source_name != target_name:
                    # only applies to objects, other complex types have the name set to None
                    continue
                # unhandled fields for now
                try:
                    if self.check_compatibility(source_field, target_field, checking_for):
                        unhandled_source_indexes.remove(i)
                        unhandled_target_indexes.remove(j)
                except (IncompatibleSchema, UnknownSchemaType):
                    self.log.debug("Not equal: %r and %r", source_field, target_field, exc_info=True)
        return unhandled_source_indexes, unhandled_target_indexes

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
