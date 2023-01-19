"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass
from enum import Enum, unique
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


@unique
class Keyword(Enum):
    REF = "$ref"
    TYPE = "type"
    MAX_LENGTH = "maxLength"
    MIN_LENGTH = "minLength"
    PATTERN = "pattern"
    MAXIMUM = "maximum"
    MINIMUM = "minimum"
    EXCLUSIVE_MAXIMUM = "exclusiveMaximum"
    EXCLUSIVE_MINIMUM = "exclusiveMinimum"
    MULTIPLE = "multiple"
    MAX_PROPERTIES = "maxProperties"
    MIN_PROPERTIES = "minProperties"
    ADDITIONAL_PROPERTIES = "additionalProperties"
    DEPENDENCIES = "dependencies"
    DEPENDENT_SCHEMAS = "dependentSchemas"
    PROPERTY_NAMES = "propertyNames"
    PATTERN_PROPERTIES = "patternProperties"
    PROPERTIES = "properties"
    REQUIRED = "required"
    DEFAULT = "default"
    ITEMS = "items"
    MAX_ITEMS = "maxItems"
    MIN_ITEMS = "minItems"
    UNIQUE_ITEMS = "uniqueItems"
    ADDITIONAL_ITEMS = "additionalItems"
    ENUM = "enum"


@unique
class Instance(Enum):
    # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.4.2.1
    NULL = "null"
    BOOLEAN = "boolean"
    NUMBER = "number"
    STRING = "string"
    INTEGER = "integer"
    OBJECT = "object"
    ARRAY = "array"


class BooleanSchema:
    """Type for a boolean schema.

    A boolean schema is not the same as the boolean instance. The first either
    accepts or rejects any instance. The later describes the values `true` and
    `false`.

    Boolean schema are the following _schemas_::

        `true`, `{}`, `false`

    Boolean instances are the values `true` and `false` describe by the following schema::

        {"type": "boolean"}
    """


@unique
class Subschema(Enum):
    ANY_OF = "anyOf"
    ALL_OF = "allOf"
    ONE_OF = "oneOf"
    NOT = "not"


@unique
class Incompatibility(Enum):
    type_changed = "type_changed"
    type_narrowed = "type_narrowed"
    max_length_added = "max_length_added"
    max_length_decreased = "max_length_decreased"
    min_length_added = "min_length_added"
    min_length_increased = "min_length_increased"
    pattern_added = "pattern_added"
    pattern_changed = "pattern_changed"
    maximum_added = "maximum_added"
    maximum_decreased = "maximum_decreased"
    minimum_added = "minimum_added"
    minimum_increased = "minimum_increased"
    maximum_exclusive_added = "maximum_exclusive_added"
    maximum_exclusive_decreased = "maximum_exclusive_decreased"
    minimum_exclusive_added = "minimum_exclusive_added"
    minimum_exclusive_increased = "minimum_exclusive_increased"
    multiple_added = "multiple_added"
    multiple_expanded = "multiple_expanded"
    multiple_changed = "multiple_changed"
    enum_array_narrowed = "enum_array_narrowed"
    combined_type_changed = "combined_type_changed"
    product_type_extended = "product_type_extended"
    sum_type_narrowed = "sum_type_narrowed"
    combined_type_subschemas_changed = "combined_type_subschemas_changed"
    max_properties_added = "max_properties_added"
    min_properties_added = "min_properties_added"
    max_properties_decreased = "max_properties_decreased"
    min_properties_increased = "min_properties_increased"
    additional_properties_narrowed = "additional_properties_narrowed"
    dependency_array_added = "dependency_array_added"
    dependency_array_extended = "dependency_array_extended"
    dependency_schema_added = "dependency_schema_added"
    property_removed_not_covered_by_partially_open_content_model = (
        "property_removed_not_covered_by_partially_open_content_model"
    )
    property_removed_from_closed_content_model = "property_removed_from_closed_content_model"
    property_added_to_open_content_model = "property_added_to_open_content_model"
    property_added_not_covered_by_partially_open_content_model = "property_added_not_covered_by_partially_open_content_model"
    required_property_added_to_unopen_content_model = "required_property_added_to_unopen_content_model"
    required_attribute_added = "required_attribute_added"
    max_items_added = "max_items_added"
    max_items_decreased = "max_items_decreased"
    min_items_added = "min_items_added"
    min_items_increased = "min_items_increased"
    unique_items_added = "unique_items_added"
    additional_items_removed = "additional_items_removed"
    additional_items_narrowed = "additional_items_narrowed"
    item_removed_not_covered_by_partially_open_content_model = "item_removed_not_covered_by_partially_open_content_model"
    item_removed_from_closed_content_model = "item_removed_from_closed_content_model"
    item_added_to_open_content_model = "item_added_to_open_content_model"
    item_added_not_covered_by_partially_open_content_model = "item_added_not_covered_by_partially_open_content_model"
    schema_added = "schema_added"
    schema_removed = "schema_removed"
    not_type_narrowed = "not_type_narrowed"


@dataclass
class AssertionCheck(Generic[T]):
    """Datatype to declare regular compatibility checks."""

    keyword: Keyword
    error_when_introducing: "Incompatibility"
    error_when_restricting: "Incompatibility"
    error_msg_when_introducing: str
    error_msg_when_restricting: str
    comparison: Callable[[T, T], bool]
