from copy import copy
from jsonschema import Draft7Validator
from karapace.compatibility.jsonschema.types import BooleanSchema, Instance, Keyword, Subschema
from typing import Any, List, Optional, Tuple, Type, TypeVar, Union

import re

T = TypeVar('T')
JSONSCHEMA_TYPES = Union[Instance, Subschema, Keyword, Type[BooleanSchema]]


def normalize_schema(validator: Draft7Validator) -> Any:
    original_schema = validator.schema
    return normalize_schema_rec(validator, original_schema)


def normalize_schema_rec(validator, original_schema) -> Any:
    if isinstance(original_schema, (bool, str, float, int)) or original_schema is None:
        return original_schema

    normalized: Any
    if isinstance(original_schema, dict):
        scope = validator.ID_OF(original_schema)
        resolver = validator.resolver
        ref = original_schema.get(Keyword.REF.value)

        if scope:
            resolver.push_scope(scope)

        normalized = dict()
        if ref is not None:
            resolved_scope, resolved_schema = resolver.resolve(ref)
            resolver.push_scope(resolved_scope)
            normalized.update(normalize_schema_rec(validator, resolved_schema))
            resolver.pop_scope()
        else:
            normalized.update((keyword, normalize_schema_rec(validator, original_schema[keyword]))
                              for keyword in original_schema)

        if scope:
            resolver.pop_scope()

    elif isinstance(original_schema, list):
        normalized = [normalize_schema_rec(validator, item) for item in original_schema]
    else:
        raise ValueError(f"Cannot handle object of type {type(original_schema)}")

    return normalized


def maybe_get_subschemas_and_type(schema: Any) -> Optional[Tuple[List[Any], Subschema]]:
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

    has_max_length = schema.get(Keyword.MAX_LENGTH.value, float('inf')) != float('inf')
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

    does_not_restrict_properties_by_pattern = len(schema.get(Keyword.PATTERN_PROPERTIES.value, list())) == 0
    does_not_restrict_additional_properties = is_true_schema(schema.get(Keyword.ADDITIONAL_PROPERTIES.value, True))

    # For propertyNames the type is implictly "string":
    #
    #   https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.10.3.2.4
    #
    property_names = schema.get(Keyword.PROPERTY_NAMES.value, {})
    assert property_names.setdefault(Keyword.TYPE.value, Instance.STRING.value) == Instance.STRING.value
    does_restrict_properties_names = is_string_and_constrained(property_names)

    return (
        does_not_restrict_properties_by_pattern and does_not_restrict_additional_properties
        and not does_restrict_properties_names
    )


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


def gt(left: Optional[int], right: Optional[int]) -> bool:
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


def lt(left: Optional[int], right: Optional[int]) -> bool:
    return gt(right, left)  # pylint: disable=arguments-out-of-order


def ne(writer: Optional[T], reader: Optional[T]) -> bool:
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


def introduced_constraint(reader: Optional[T], writer: Optional[T]) -> bool:
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
    valdiates `target_property_name`, if any.
    """
    for pattern, pattern_schema in schema.get(Keyword.PATTERN_PROPERTIES.value, dict()).items():
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
