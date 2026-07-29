"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from copy import copy
from karapace.core.compatibility.jsonschema.types import BooleanSchema, Instance, Keyword, Subschema
from karapace.core.typing import JsonSchemaValidator
from typing import Any, TypeVar, Union

import re
import warnings

T = TypeVar("T")
JSONSCHEMA_TYPES = Union[Instance, Subschema, Keyword, type[BooleanSchema]]

# Newer-draft (2019-09 / 2020-12) keyword names that are not part of the Draft-7 `Keyword` enum.
PREFIX_ITEMS = "prefixItems"
DEFS = "$defs"
DEFINITIONS = "definitions"
DEPENDENT_REQUIRED = "dependentRequired"

# Keywords the compatibility engine cannot yet evaluate. Their presence makes a Draft-7-shaped
# comparison unreliable, so compatibility checking fails closed rather than returning a possibly
# wrong verdict.
UNSUPPORTED_COMPAT_KEYWORDS = frozenset(
    {
        "$dynamicRef",
        "$dynamicAnchor",
        "$recursiveRef",
        "$recursiveAnchor",
        "unevaluatedProperties",
        "unevaluatedItems",
        "$vocabulary",
    }
)

# Keywords whose *values are maps keyed by names chosen by the schema author* (property names,
# definition names, patterns). Those keys are not schema keywords, so the scanner must recurse into
# the values only — never treat the keys as keywords.
_NAME_KEYED_MAP_KEYWORDS = frozenset(
    {
        "properties",
        "patternProperties",
        "$defs",
        "definitions",
        "dependentSchemas",
        "dependentRequired",
    }
)

# Keywords whose values are arbitrary instance data, not subschemas. The scanner must not descend
# into them, otherwise a user value that happens to be an object with a keyword-like key would be
# misread as a schema keyword.
_DATA_VALUE_KEYWORDS = frozenset({"enum", "const", "default", "examples"})


def normalize_schema(validator: JsonSchemaValidator) -> Any:
    original_schema = validator.schema
    return normalize_schema_rec(validator, original_schema)


def _resolver_of(validator: JsonSchemaValidator) -> Any:
    """Return the validator's ref resolver.

    ``Validator.resolver`` is deprecated as of jsonschema 4.18 in favour of the ``referencing``
    library, but ``normalize_schema`` still relies on its ``push_scope``/``pop_scope``/``resolve``
    API. The pin ``jsonschema>=4.18,<5`` guarantees the attribute stays available (deprecations are
    not removed within a major series), so we access it in one place and silence the warning here.

    TODO: migrate reference resolution to the ``referencing`` library and drop this shim.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return validator.resolver


def normalize_schema_rec(validator: JsonSchemaValidator, original_schema: Any) -> Any:
    if isinstance(original_schema, (bool, str, float, int)) or original_schema is None:
        return original_schema

    normalized: Any
    if isinstance(original_schema, dict):
        scope = validator.ID_OF(original_schema)
        resolver = _resolver_of(validator)
        ref = original_schema.get(Keyword.REF.value)

        if scope:
            resolver.push_scope(scope)

        normalized = {}
        if ref is not None:
            resolved_scope, resolved_schema = resolver.resolve(ref)
            resolver.push_scope(resolved_scope)
            normalized.update(normalize_schema_rec(validator, resolved_schema))
            resolver.pop_scope()
        else:
            normalized.update(
                (keyword, normalize_schema_rec(validator, original_schema[keyword])) for keyword in original_schema
            )

        if scope:
            resolver.pop_scope()

        normalized = canonicalize_draft_keywords(normalized)

    elif isinstance(original_schema, list):
        normalized = [normalize_schema_rec(validator, item) for item in original_schema]
    else:
        raise ValueError(f"Cannot handle object of type {type(original_schema)}")

    return normalized


def canonicalize_draft_keywords(schema: dict) -> dict:
    """Rewrite Draft 2019-09 / 2020-12 keywords into their Draft-7 equivalents.

    The compatibility engine is expressed in terms of Draft-7 keyword shapes. Rewriting the
    renamed/relocated keywords here means every downstream check works unchanged, and comparisons
    between schemas written in different drafts become well-defined (a single canonical form).
    Keywords that cannot be canonicalized safely are handled by a fail-closed guard before
    compatibility checking, not here.
    """
    # 2020-12 array model: `prefixItems` holds the positional (tuple) schemas that Draft-7 spells as
    # the list form of `items`; when `prefixItems` is present, `items` holds the "additional items"
    # schema that Draft-7 spells as `additionalItems`.
    if PREFIX_ITEMS in schema:
        prefix_items = schema.pop(PREFIX_ITEMS)
        additional_items = schema.pop(Keyword.ITEMS.value, None)
        schema[Keyword.ITEMS.value] = prefix_items
        if additional_items is not None and Keyword.ADDITIONAL_ITEMS.value not in schema:
            schema[Keyword.ADDITIONAL_ITEMS.value] = additional_items

    # 2019-09 rename: `$defs` is the new name for `definitions`. References are already resolved by
    # normalize_schema_rec, so this only keeps the canonical key name for any surviving subschema.
    if DEFS in schema and DEFINITIONS not in schema:
        schema[DEFINITIONS] = schema.pop(DEFS)

    # 2019-09 split: `dependentRequired` is the string-array form of Draft-7 `dependencies`.
    if DEPENDENT_REQUIRED in schema:
        dependent_required = schema.pop(DEPENDENT_REQUIRED)
        existing = schema.get(Keyword.DEPENDENCIES.value, {})
        if isinstance(existing, dict) and isinstance(dependent_required, dict):
            # Existing `dependencies` entries win on key conflict.
            schema[Keyword.DEPENDENCIES.value] = {**dependent_required, **existing}

    return schema


def find_unsupported_compat_keywords(schema: Any) -> set[str]:
    """Recursively collect keywords (in keyword position) the compatibility engine cannot evaluate.

    Returns the subset of ``UNSUPPORTED_COMPAT_KEYWORDS`` used as *schema keywords* anywhere in the
    schema. Used by the fail-closed guard before normalization, so it runs on the original schema.

    The walk is position-aware to avoid false positives: a keyword-like string is only a match when
    it is a schema keyword, not when it is an author-chosen name (e.g. a property named
    ``unevaluatedItems``) or a value inside instance-data keywords (``enum``, ``const``, ``default``).
    """
    found: set[str] = set()
    if isinstance(schema, dict):
        for key, value in schema.items():
            if key in UNSUPPORTED_COMPAT_KEYWORDS:
                found.add(key)

            if key in _DATA_VALUE_KEYWORDS:
                # Value is arbitrary instance data, never a subschema — do not descend.
                continue
            if key in _NAME_KEYED_MAP_KEYWORDS and isinstance(value, dict):
                # Keys here are author-chosen names; only the values are subschemas.
                for subschema in value.values():
                    found |= find_unsupported_compat_keywords(subschema)
            else:
                found |= find_unsupported_compat_keywords(value)
    elif isinstance(schema, list):
        for item in schema:
            found |= find_unsupported_compat_keywords(item)
    return found


def maybe_get_subschemas_and_type(schema: Any) -> tuple[list[Any], Subschema] | None:
    """If schema contains `anyOf`, `allOf`, or `oneOf`, return it.

    This will also normalized schemas with a list of types to a `anyOf`, e..g:

    >>> maybe_get_subschemas_and_type({"type": ["number", "string"], "pattern": "[0-9]{1,2}", "maximum": 100})
    (
        [
            {"type": "number", "pattern": "[0-9]{1,2}", "maximum": 100},
            {"type": "string", "pattern": "[0-9]{1,2}", "maximum": 100}
        ],
        Subschema.ANY_OF,
    )

    Reference:
    - https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.7.6.1
    """
    if not isinstance(schema, dict):
        return None

    type_value = schema.get(Keyword.TYPE.value)

    subschema: Any
    if isinstance(type_value, list):
        normalized_schemas = []
        for subtype in type_value:
            subschema = copy(schema)
            subschema[Keyword.TYPE.value] = subtype
            normalized_schemas.append(subschema)

        return (normalized_schemas, Subschema.ANY_OF)

    for type_ in (Subschema.ALL_OF, Subschema.ANY_OF, Subschema.ONE_OF):
        subschema = schema.get(type_.value)
        if subschema is not None:
            # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.10.2.1
            assert isinstance(subschema, list), "allOf/anyOf/oneOf must be an array"
            return (subschema, type_)

    subschema = schema.get(Subschema.NOT.value)
    if subschema is not None:
        # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.10.2.1
        return ([subschema], Subschema.NOT)

    return None


def is_tuple(schema: Any) -> bool:
    if not isinstance(schema, dict):
        return False

    # if the value of items is `list` then it describes a tuple
    return isinstance(schema.get(Keyword.ITEMS.value), list)


def is_string_and_constrained(schema: Any) -> bool:
    """True if the schema is for a string and it limits the valid values.

    >>> is_string_and_constrained(True)
    False
    >>> is_string_and_constrained({})
    False
    >>> is_string_and_constrained({"type": "array"})
    False
    >>> is_string_and_constrained({"type": "string"})
    False
    >>> is_string_and_constrained({"type": "string", "minLength": 0})
    False
    >>> is_string_and_constrained({"type": "string", "minLength": 1})
    True
    """
    if not isinstance(schema, dict):
        return False

    if schema.get(Keyword.TYPE.value) != Instance.STRING.value:
        return False

    has_max_length = schema.get(Keyword.MAX_LENGTH.value, float("inf")) != float("inf")
    has_min_length = schema.get(Keyword.MIN_LENGTH.value, 0) != 0
    has_pattern = schema.get(Keyword.PATTERN.value) is not None

    return has_max_length or has_min_length or has_pattern


def is_object_content_model_open(schema: Any) -> bool:
    """True if the object schema only validates the explicitely declared
    properties.

    Properties can be validated without explicitly declaring it with:

    - patternProperties: uses a regex to determine the attribute to assert
    - additionalProperties: by definition asserts on all other attributes
    """
    if not isinstance(schema, dict):
        return False

    does_not_restrict_properties_by_pattern = len(schema.get(Keyword.PATTERN_PROPERTIES.value, [])) == 0
    does_not_restrict_additional_properties = is_true_schema(schema.get(Keyword.ADDITIONAL_PROPERTIES.value, True))

    return does_not_restrict_properties_by_pattern and does_not_restrict_additional_properties


def is_true_schema(schema: Any) -> bool:
    """True if the value of `schema` is equal to the explicit accept schema `{}`."""
    # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.4.3.2
    is_true = schema is True
    return is_true


def is_false_schema(schema: Any) -> bool:
    """True if the value of `schema` is the always reject schema.

    The `false` schema forbids a given value. For writers this means the value
    is never produced, for readers it means the value is always rejected.

    >>> is_false_schema(parse_jsonschema_definition("false"))
    True
    >>> is_false_schema(parse_jsonschema_definition("{}"))
    False
    >>> is_false_schema(parse_jsonschema_definition("true"))
    False

    Note:
        Negated schemas are not the same as the false schema:

        >>> is_false_schema(parse_jsonschema_definition('{"not":{}}'))
        False
        >>> is_false_schema(parse_jsonschema_definition('{"not":{"type":"number"}}'))
        False
    """
    # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.4.3.2
    is_false = schema is False
    return is_false


def is_array_content_model_open(schema: Any) -> bool:
    """True if the array schema represents a tuple, and the additional elements
    are not validated.

    It is possible to validate the other tuple elements with:

    - additionalItems: by definition asserts on every other item
    """
    if not isinstance(schema, dict):
        return False

    additional_items = schema.get(Keyword.ADDITIONAL_ITEMS.value)
    return is_tuple(schema) and additional_items in (True, None)


def is_tuple_without_additional_items(schema: Any) -> bool:
    """True if the schema describes a tuple and additional items are forbidden."""
    if not isinstance(schema, dict):
        return False

    # by default additional items are allowed
    additional_items_default = True
    additional_items = schema.get(Keyword.ADDITIONAL_ITEMS.value, additional_items_default)

    # can not rely on additional_items being falsy. It is possible for it to be
    # defined as the empty schema `{}` which is the same as the `true` schema,
    # but evaluates to `False` in python.
    return is_tuple(schema) and is_false_schema(additional_items)


def gt(left: int | None, right: int | None) -> bool:
    """Predicate greater-than that checks for nullables.

    When `left` is writer and `right` is reader, this can be used to check for
    stricter lower bound constraints, which implies an incompatibility. On the
    example below the values [5,10) are not valid anymore:

    >>> minimum_writer = 10
    >>> minimum_reader = 5
    >>> gt(minimum_writer, minimum_reader)
    True

    When `left` is reader and `right` is writer, this can be used to check for
    stricter upper bound constraints, which implies an incompatibility. On the
    example below the values (20,30] are not valid anymore:

    >>> maximum_reader = 30
    >>> maximum_writer = 20
    >>> gt(maximum_reader, maximum_writer)
    True

    Note:

        The values must be seperatly checked with::

            introduced_constraint(reader, writer)

        This is necessary because this predicate does not know which side is the
        reader, and ignores if either left or right do not have the value.

        >>> gt(1, None)
        False
        >>> gt(None, 1)
        False
    """
    return bool(left is not None and right is not None and left > right)


def lt(left: int | None, right: int | None) -> bool:
    return gt(right, left)


def ne(writer: T | None, reader: T | None) -> bool:
    """Predicate not-equals that checks for nullables.

    Predicate used to check for incompatibility in constraints that accept
    specific values. E.g. regular expression, the example below introduces an
    incompatibility because the empty string "" is not a valid value anymore.

    >>> ne("a*", "aa*")
    True

    Note:
        The values must be seperatly checked with::

            introduced_constraint(reader, writer)

        This is necessary because this predicate does not know which side is the
        reader, and ignores if either left or right do not have the value.

        >>> ne(None, 1)
        False
        >>> None != 1  # in contrast to
        True
        >>> ne(1, None)
        False
    """
    return bool(reader is not None and writer is not None and reader != writer)


def introduced_constraint(reader: T | None, writer: T | None) -> bool:
    """True if `writer` did *not* have the constraint but `reader` introduced it.

    A constraint limits the value domain, because of that objects that were
    valid become invalid introducing an incompatibility. On the example below
    the values [10) are not valid anymore:

    >>> reader_max_length = None
    >>> writer_max_length = 10
    >>> introduced_constraint(reader_max_length, writer_max_length)
    True
    """
    return writer is None and reader is not None


def schema_from_partially_open_content_model(schema: dict, target_property_name: str) -> Any:
    """Returns the schema from patternProperties or additionalProperties that
    validates `target_property_name`, if any.
    """
    for pattern, pattern_schema in schema.get(Keyword.PATTERN_PROPERTIES.value, {}).items():
        if re.match(pattern, target_property_name):
            return pattern_schema

    # additionalProperties is used when
    # - the property does not have a schema
    # - none of the patternProperties matches the property_name
    # https://json-schema.org/draft/2020-12/json-schema-core.html#additionalProperties
    return schema.get(Keyword.ADDITIONAL_PROPERTIES.value)


def get_type_of(schema: Any) -> JSONSCHEMA_TYPES:
    # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.4.2.1

    # The difference is due to the convertion of the JSON value null to the Python value None
    if schema is None:
        return Instance.NULL

    if isinstance(schema, str):
        # Strings should be described using type=string
        raise RuntimeError("Provided schema is just a string")

    if is_true_schema(schema) or is_false_schema(schema):
        return BooleanSchema

    if isinstance(schema, list):
        # The meaning of a list depends on the contexts, e.g. additionalItems
        # is a subschema that applies in the context of the parent, anyOf
        # applies in the current context.
        raise RuntimeError("Provided schema is just a list")

    if isinstance(schema, dict):
        subschema_type = maybe_get_subschemas_and_type(schema)
        if subschema_type is not None:
            return subschema_type[1]

        type_value = schema.get(Keyword.TYPE.value)
        if type_value:
            return Instance(type_value)

        if Keyword.ENUM.value in schema:
            return Keyword.ENUM

        return Instance.OBJECT

    raise ValueError("Couldnt determine type of schema")


def get_name_of(schema_type: JSONSCHEMA_TYPES) -> str:
    if isinstance(schema_type, (Instance, Subschema, Keyword)):
        return schema_type.value

    return ""


def is_simple_subschema(schema: Any) -> bool:
    if schema is None:
        return False

    subschemas = maybe_get_subschemas_and_type(schema)
    if subschemas is not None and len(subschemas[0]) == 1:
        return True

    return False
