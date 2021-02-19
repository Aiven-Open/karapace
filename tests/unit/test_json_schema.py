from karapace.avro_compatibility import SchemaCompatibilityResult
from karapace.compatibility.jsonschema.checks import compatibility
from karapace.compatibility.jsonschema.parse import parse_schema_definition

COMPATIBLE = SchemaCompatibilityResult.compatible()

# boolean schemas
NOT_OF_EMPTY_SCHEMA = parse_schema_definition('{"not":{}}')
NOT_OF_TRUE_SCHEMA = parse_schema_definition('{"not":true}')
FALSE_SCHEMA = parse_schema_definition('false')
TRUE_SCHEMA = parse_schema_definition('true')
EMPTY_SCHEMA = parse_schema_definition('{}')

# simple instance schemas
BOOLEAN_SCHEMA = parse_schema_definition('{"type":"boolean"}')
INT_SCHEMA = parse_schema_definition('{"type":"integer"}')
NUMBER_SCHEMA = parse_schema_definition('{"type":"number"}')
STRING_SCHEMA = parse_schema_definition('{"type":"string"}')
OBJECT_SCHEMA = parse_schema_definition('{"type":"object"}')
ARRAY_SCHEMA = parse_schema_definition('{"type":"array"}')

# negation of simple schemas
NOT_BOOLEAN_SCHEMA = parse_schema_definition('{"not":{"type":"boolean"}}')
NOT_INT_SCHEMA = parse_schema_definition('{"not":{"type":"integer"}}')
NOT_NUMBER_SCHEMA = parse_schema_definition('{"not":{"type":"number"}}')
NOT_STRING_SCHEMA = parse_schema_definition('{"not":{"type":"string"}}')
NOT_OBJECT_SCHEMA = parse_schema_definition('{"not":{"type":"object"}}')
NOT_ARRAY_SCHEMA = parse_schema_definition('{"not":{"type":"array"}}')

# structural validation
MAX_LENGTH_SCHEMA = parse_schema_definition('{"type":"string","maxLength":3}')
MAX_LENGTH_DECREASED_SCHEMA = parse_schema_definition('{"type":"string","maxLength":2}')
MIN_LENGTH_SCHEMA = parse_schema_definition('{"type":"string","minLength":5}')
MIN_LENGTH_INCREASED_SCHEMA = parse_schema_definition('{"type":"string","minLength":7}')
MIN_PATTERN_SCHEMA = parse_schema_definition('{"type":"string","pattern":"a*"}')
MIN_PATTERN_STRICT_SCHEMA = parse_schema_definition('{"type":"string","pattern":"a+"}')
MAXIMUM_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","maximum":13}')
MAXIMUM_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","maximum":13}')
MAXIMUM_DECREASED_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","maximum":11}')
MAXIMUM_DECREASED_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","maximum":11}')
MINIMUM_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","minimum":17}')
MINIMUM_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","minimum":17}')
MINIMUM_INCREASED_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","minimum":19}')
MINIMUM_INCREASED_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","minimum":19}')
EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","exclusiveMaximum":29}')
EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","exclusiveMaximum":29}')
EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","exclusiveMaximum":23}')
EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","exclusiveMaximum":23}')
EXCLUSIVE_MINIMUM_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","exclusiveMinimum":31}')
EXCLUSIVE_MINIMUM_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","exclusiveMinimum":31}')
EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA = parse_schema_definition('{"type":"integer","exclusiveMinimum":37}')
EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA = parse_schema_definition('{"type":"number","exclusiveMinimum":37}')
MAX_PROPERTIES_SCHEMA = parse_schema_definition('{"type":"object","maxProperties":43}')
MAX_PROPERTIES_DECREASED_SCHEMA = parse_schema_definition('{"type":"object","maxProperties":41}')
MIN_PROPERTIES_SCHEMA = parse_schema_definition('{"type":"object","minProperties":47}')
MIN_PROPERTIES_INCREASED_SCHEMA = parse_schema_definition('{"type":"object","minProperties":53}')
MAX_ITEMS_SCHEMA = parse_schema_definition('{"type":"array","maxItems":61}')
MAX_ITEMS_DECREASED_SCHEMA = parse_schema_definition('{"type":"array","maxItems":59}')
MIN_ITEMS_SCHEMA = parse_schema_definition('{"type":"array","minItems":67}')
MIN_ITEMS_INCREASED_SCHEMA = parse_schema_definition('{"type":"array","minItems":71}')

TUPLE_OF_INT_INT_SCHEMA = parse_schema_definition(
    '{"type":"array","items":[{"type":"integer"},{"type":"integer"}],"additionalItems":false}'
)
TUPLE_OF_INT_SCHEMA = parse_schema_definition('{"type":"array","items":[{"type":"integer"}],"additionalItems":false}')
TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA = parse_schema_definition(
    '{"type":"array","items":[{"type":"integer"}],"additionalItems":{"type":"integer"}}'
)
TUPLE_OF_INT_INT_OPEN_SCHEMA = parse_schema_definition('{"type":"array","items":[{"type":"integer"},{"type":"integer"}]}')
TUPLE_OF_INT_OPEN_SCHEMA = parse_schema_definition('{"type":"array","items":[{"type":"integer"}]}')
ARRAY_OF_INT_SCHEMA = parse_schema_definition('{"type":"array","items":{"type":"integer"}}')
ARRAY_OF_NUMBER_SCHEMA = parse_schema_definition('{"type":"array","items":{"type":"number"}}')
ARRAY_OF_STRING_SCHEMA = parse_schema_definition('{"type":"array","items":{"type":"string"}}')
ENUM_AB_SCHEMA = parse_schema_definition('{"enum":["A","B"]}')
ENUM_ABC_SCHEMA = parse_schema_definition('{"enum":["A","B","C"]}')
ENUM_BC_SCHEMA = parse_schema_definition('{"enum":["B","C"]}')
ONEOF_STRING_SCHEMA = parse_schema_definition('{"oneOf":[{"type":"string"}]}')
ONEOF_STRING_INT_SCHEMA = parse_schema_definition('{"oneOf":[{"type":"string"},{"type":"integer"}]}')
ONEOF_INT_SCHEMA = parse_schema_definition('{"oneOf":[{"type":"integer"}]}')
ONEOF_NUMBER_SCHEMA = parse_schema_definition('{"oneOf":[{"type":"number"}]}')
TYPES_STRING_INT_SCHEMA = parse_schema_definition('{"type":["string","integer"]}')
TYPES_STRING_SCHEMA = parse_schema_definition('{"type":["string"]}')
EMPTY_OBJECT_SCHEMA = parse_schema_definition('{"type":"object","additionalProperties":false}')
A_OBJECT_SCHEMA = parse_schema_definition('{"type":"object","properties":{"a":{}}}')
B_OBJECT_SCHEMA = parse_schema_definition('{"type":"object","properties":{"b":{}}}')
A_INT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}'
)
A_DINT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer","default":0}}}'
)
B_INT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"b":{"type":"integer"}}}'
)
A_INT_OPEN_OBJECT_SCHEMA = parse_schema_definition('{"type":"object","properties":{"a":{"type":"integer"}}}')
B_INT_OPEN_OBJECT_SCHEMA = parse_schema_definition('{"type":"object","properties":{"b":{"type":"integer"}}}')
B_DINT_OPEN_OBJECT_SCHEMA = parse_schema_definition('{"type":"object","properties":{"b":{"type":"integer","default":0}}}')
A_INT_B_INT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"integer"}}}'
)
A_DINT_B_INT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer","default":0},"b":{"type":"integer"}}}'
)
A_INT_B_INT_REQUIRED_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"required":["b"],'
    '"properties":{"a":{"type":"integer"},"b":{"type":"integer"}}}'
)
A_INT_B_DINT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"integer","default":0}}}'
)
A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"required":["b"],'
    '"properties":{"a":{"type":"integer"},"b":{"type":"integer","default":0}}}'
)
A_DINT_B_DINT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,'
    '"properties":{"a":{"type":"integer","default":0},"b":{"type":"integer","default":0}}}'
)
A_DINT_B_NUM_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer","default":1},"b":{"type":"number"}}}'
)
A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,'
    '"properties":{"a":{"type":"integer","default":1},"b":{"type":"number"},"c":{"type":"integer","default":0}}}'
)
B_NUM_C_DINT_OPEN_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","properties":{"b":{"type":"number"},"c":{"type":"integer","default":0}}}'
)
B_NUM_C_INT_OPEN_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","properties":{"b":{"type":"number"},"c":{"type":"integer"}}}'
)
B_NUM_C_INT_OBJECT_SCHEMA = parse_schema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"b":{"type":"number"},"c":{"type":"integer"}}}'
)
PROPERTY_ASTAR_OBJECT_SCHEMA = parse_schema_definition('{"type":"object","propertyNames":{"pattern":"a*"}}')
ARRAY_OF_POSITIVE_INTEGER = parse_schema_definition(
    '''
    {
        "type": "array",
        "items": {"type": "integer", "exclusiveMinimum": 0}
    }
    '''
)
ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF = parse_schema_definition(
    '''
    {
        "type": "array",
        "items": {"$ref": "#/$defs/positiveInteger"},
        "$defs": {
            "positiveInteger": {
                "type": "integer",
                "exclusiveMinimum": 0
            }
        }
    }
    '''
)


def test_extra_optional_field_with_open_model_is_compatible() -> None:
    # - the reader is an open model, the extra field produced by the writer is
    # automatically accepted
    assert compatibility(
        reader=OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An unknown field to an open model is compatible"
    assert compatibility(
        reader=TRUE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An unknown field to an open model is compatible"
    assert compatibility(
        reader=EMPTY_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An unknown field to an open model is compatible"

    # - the writer is a closed model, so the field `b` was never produced, which
    # means that the writer never produced an invalid value.
    # - the reader's `b` field is optional, so the absenced of the field is not
    # a problem, and `a` is ignored because of the open model
    assert compatibility(
        reader=B_INT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An unknown field on a open model is compatible"

    # - if the model is closed, then `a` must also be accepted
    assert compatibility(
        reader=A_INT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "A new optional field is compatible"

    # Examples a bit more complex
    assert compatibility(
        reader=B_NUM_C_INT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An extra optional field is compatible"
    assert compatibility(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
    ) == COMPATIBLE


def test_extra_field_with_closed_model_is_incompatible() -> None:
    # The field here is not required but forbidden, because of this the reader
    # will reject the writer data
    assert compatibility(
        reader=FALSE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=NOT_OF_TRUE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=NOT_OF_EMPTY_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
    ) != COMPATIBLE


def test_missing_required_field_is_incompatible() -> None:
    assert compatibility(
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) != COMPATIBLE


def test_giving_a_default_value_for_a_non_required_field_is_compatible() -> None:
    assert compatibility(
        reader=OBJECT_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An unknown field to an open model is compatible"
    assert compatibility(
        reader=TRUE_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An unknown field to an open model is compatible"
    assert compatibility(
        reader=EMPTY_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
    ) == COMPATIBLE, "An unknown field to an open model is compatible"
    assert compatibility(
        reader=B_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=A_INT_B_DINT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=A_DINT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=A_DINT_B_DINT_OBJECT_SCHEMA,
        writer=EMPTY_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=A_DINT_B_DINT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE


def test_from_closed_to_open_is_incompatible() -> None:
    assert compatibility(
        reader=FALSE_SCHEMA,
        writer=TRUE_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
    ) != COMPATIBLE


def test_union_with_incompatible_elements() -> None:
    union1 = parse_schema_definition(
        '{"oneOf":[{"type":"array","items":{"type":"object",'
        '"additionalProperties":false,"properties":{"a":{"type":"integer","default":1},"b":{"type":"number"}}}}]}'
    )
    union2 = parse_schema_definition(
        '{"oneOf":[{"type":"array","items":{"type":"object",'
        '"additionalProperties":false,"properties":{"b":{"type":"number"},"c":{"type":"integer"}}}}]}'
    )
    assert compatibility(reader=union2, writer=union1) != COMPATIBLE


def test_union_with_compatible_elements() -> None:
    union1 = parse_schema_definition(
        '{"oneOf":[{"type":"array","items":{"type":"object",'
        '"additionalProperties":false,"properties":{"a":{"type":"integer","default":1},"b":{"type":"number"}}}}]}'
    )
    union2 = parse_schema_definition(
        '{"oneOf":[{"type":"array","items":{"type":"object",'
        '"properties":{"b":{"type":"number"},"c":{"type":"integer","default":0}}}}]}'
    )
    assert compatibility(reader=union2, writer=union1) == COMPATIBLE


def test_reflexivity() -> None:
    schema = parse_schema_definition(
        '{"type":"object","required":["boolF","intF","numberF","stringF","enumF","arrayF","recordF"],'
        '"properties":{"recordF":{"type":"object","properties":{"f":{"type":"number"}}},"stringF":{"type":"string"},'
        '"boolF":{"type":"boolean"},"intF":{"type":"integer"},"enumF":{"enum":["S"]},'
        '"arrayF":{"type":"array","items":{"type":"string"}},"numberF":{"type":"number"},"bool0":{"type":"boolean"}}}'
    )
    assert compatibility(reader=schema, writer=schema) == COMPATIBLE

    assert compatibility(NOT_OF_EMPTY_SCHEMA, NOT_OF_EMPTY_SCHEMA) == COMPATIBLE
    assert compatibility(NOT_OF_TRUE_SCHEMA, NOT_OF_TRUE_SCHEMA) == COMPATIBLE
    assert compatibility(FALSE_SCHEMA, FALSE_SCHEMA) == COMPATIBLE
    assert compatibility(TRUE_SCHEMA, TRUE_SCHEMA) == COMPATIBLE
    assert compatibility(EMPTY_SCHEMA, EMPTY_SCHEMA) == COMPATIBLE
    assert compatibility(BOOLEAN_SCHEMA, BOOLEAN_SCHEMA) == COMPATIBLE
    assert compatibility(INT_SCHEMA, INT_SCHEMA) == COMPATIBLE
    assert compatibility(NUMBER_SCHEMA, NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(STRING_SCHEMA, STRING_SCHEMA) == COMPATIBLE
    assert compatibility(OBJECT_SCHEMA, OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(ARRAY_SCHEMA, ARRAY_SCHEMA) == COMPATIBLE
    assert compatibility(NOT_BOOLEAN_SCHEMA, NOT_BOOLEAN_SCHEMA) == COMPATIBLE
    assert compatibility(NOT_INT_SCHEMA, NOT_INT_SCHEMA) == COMPATIBLE
    assert compatibility(NOT_NUMBER_SCHEMA, NOT_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(NOT_STRING_SCHEMA, NOT_STRING_SCHEMA) == COMPATIBLE
    assert compatibility(NOT_OBJECT_SCHEMA, NOT_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(NOT_ARRAY_SCHEMA, NOT_ARRAY_SCHEMA) == COMPATIBLE
    assert compatibility(MAX_LENGTH_SCHEMA, MAX_LENGTH_SCHEMA) == COMPATIBLE
    assert compatibility(MAX_LENGTH_DECREASED_SCHEMA, MAX_LENGTH_DECREASED_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_LENGTH_SCHEMA, MIN_LENGTH_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_LENGTH_INCREASED_SCHEMA, MIN_LENGTH_INCREASED_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_PATTERN_SCHEMA, MIN_PATTERN_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_PATTERN_STRICT_SCHEMA, MIN_PATTERN_STRICT_SCHEMA) == COMPATIBLE
    assert compatibility(MAXIMUM_INTEGER_SCHEMA, MAXIMUM_INTEGER_SCHEMA) == COMPATIBLE
    assert compatibility(MAXIMUM_NUMBER_SCHEMA, MAXIMUM_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(MAXIMUM_DECREASED_INTEGER_SCHEMA, MAXIMUM_DECREASED_INTEGER_SCHEMA) == COMPATIBLE
    assert compatibility(MAXIMUM_DECREASED_NUMBER_SCHEMA, MAXIMUM_DECREASED_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(MINIMUM_INTEGER_SCHEMA, MINIMUM_INTEGER_SCHEMA) == COMPATIBLE
    assert compatibility(MINIMUM_NUMBER_SCHEMA, MINIMUM_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(MINIMUM_INCREASED_INTEGER_SCHEMA, MINIMUM_INCREASED_INTEGER_SCHEMA) == COMPATIBLE
    assert compatibility(MINIMUM_INCREASED_NUMBER_SCHEMA, MINIMUM_INCREASED_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA, EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA) == COMPATIBLE
    assert compatibility(EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA, EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(
        EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA,
        EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA, EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(EXCLUSIVE_MINIMUM_INTEGER_SCHEMA, EXCLUSIVE_MINIMUM_INTEGER_SCHEMA) == COMPATIBLE
    assert compatibility(EXCLUSIVE_MINIMUM_NUMBER_SCHEMA, EXCLUSIVE_MINIMUM_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(
        EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA,
        EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA, EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(MAX_PROPERTIES_SCHEMA, MAX_PROPERTIES_SCHEMA) == COMPATIBLE
    assert compatibility(MAX_PROPERTIES_DECREASED_SCHEMA, MAX_PROPERTIES_DECREASED_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_PROPERTIES_SCHEMA, MIN_PROPERTIES_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_PROPERTIES_INCREASED_SCHEMA, MIN_PROPERTIES_INCREASED_SCHEMA) == COMPATIBLE
    assert compatibility(MAX_ITEMS_SCHEMA, MAX_ITEMS_SCHEMA) == COMPATIBLE
    assert compatibility(MAX_ITEMS_DECREASED_SCHEMA, MAX_ITEMS_DECREASED_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_ITEMS_SCHEMA, MIN_ITEMS_SCHEMA) == COMPATIBLE
    assert compatibility(MIN_ITEMS_INCREASED_SCHEMA, MIN_ITEMS_INCREASED_SCHEMA) == COMPATIBLE
    assert compatibility(TUPLE_OF_INT_INT_SCHEMA, TUPLE_OF_INT_INT_SCHEMA) == COMPATIBLE
    assert compatibility(TUPLE_OF_INT_SCHEMA, TUPLE_OF_INT_SCHEMA) == COMPATIBLE
    assert compatibility(TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA, TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA) == COMPATIBLE
    assert compatibility(TUPLE_OF_INT_INT_OPEN_SCHEMA, TUPLE_OF_INT_INT_OPEN_SCHEMA) == COMPATIBLE
    assert compatibility(TUPLE_OF_INT_OPEN_SCHEMA, TUPLE_OF_INT_OPEN_SCHEMA) == COMPATIBLE
    assert compatibility(ARRAY_OF_INT_SCHEMA, ARRAY_OF_INT_SCHEMA) == COMPATIBLE
    assert compatibility(ARRAY_OF_NUMBER_SCHEMA, ARRAY_OF_NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(ARRAY_OF_STRING_SCHEMA, ARRAY_OF_STRING_SCHEMA) == COMPATIBLE
    assert compatibility(ENUM_AB_SCHEMA, ENUM_AB_SCHEMA) == COMPATIBLE
    assert compatibility(ENUM_ABC_SCHEMA, ENUM_ABC_SCHEMA) == COMPATIBLE
    assert compatibility(ENUM_BC_SCHEMA, ENUM_BC_SCHEMA) == COMPATIBLE
    assert compatibility(ONEOF_STRING_SCHEMA, ONEOF_STRING_SCHEMA) == COMPATIBLE
    assert compatibility(ONEOF_STRING_INT_SCHEMA, ONEOF_STRING_INT_SCHEMA) == COMPATIBLE
    assert compatibility(EMPTY_OBJECT_SCHEMA, EMPTY_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_INT_OBJECT_SCHEMA, A_INT_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_INT_OPEN_OBJECT_SCHEMA, A_INT_OPEN_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_INT_B_INT_OBJECT_SCHEMA, A_INT_B_INT_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_INT_B_INT_REQUIRED_OBJECT_SCHEMA, A_INT_B_INT_REQUIRED_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_INT_B_DINT_OBJECT_SCHEMA, A_INT_B_DINT_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA, A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_DINT_B_DINT_OBJECT_SCHEMA, A_DINT_B_DINT_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_DINT_B_NUM_OBJECT_SCHEMA, A_DINT_B_NUM_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA, A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(B_NUM_C_DINT_OPEN_OBJECT_SCHEMA, B_NUM_C_DINT_OPEN_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(B_NUM_C_INT_OBJECT_SCHEMA, B_NUM_C_INT_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(ARRAY_OF_POSITIVE_INTEGER, ARRAY_OF_POSITIVE_INTEGER) == COMPATIBLE
    assert compatibility(ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF, ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF) == COMPATIBLE


def test_schema_compatibility_successes() -> None:
    # allowing a broader set of values is compatible
    assert compatibility(reader=NUMBER_SCHEMA, writer=INT_SCHEMA) == COMPATIBLE
    assert compatibility(reader=ARRAY_OF_NUMBER_SCHEMA, writer=ARRAY_OF_INT_SCHEMA) == COMPATIBLE
    assert compatibility(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA,
        writer=TUPLE_OF_INT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(reader=ARRAY_OF_INT_SCHEMA, writer=TUPLE_OF_INT_OPEN_SCHEMA) == COMPATIBLE
    assert compatibility(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=TUPLE_OF_INT_INT_OPEN_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(reader=ENUM_ABC_SCHEMA, writer=ENUM_AB_SCHEMA) == COMPATIBLE
    assert compatibility(
        reader=ONEOF_STRING_INT_SCHEMA,
        writer=ONEOF_STRING_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(reader=ONEOF_STRING_INT_SCHEMA, writer=STRING_SCHEMA) == COMPATIBLE
    assert compatibility(
        reader=A_INT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_B_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE

    # requiring less values is compatible
    assert compatibility(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_INT_OPEN_SCHEMA,
    ) == COMPATIBLE

    # equivalences
    assert compatibility(reader=ONEOF_STRING_SCHEMA, writer=STRING_SCHEMA) == COMPATIBLE
    assert compatibility(reader=STRING_SCHEMA, writer=ONEOF_STRING_SCHEMA) == COMPATIBLE

    # new non-required fields is compatible
    assert compatibility(reader=A_INT_OBJECT_SCHEMA, writer=EMPTY_OBJECT_SCHEMA) == COMPATIBLE
    assert compatibility(
        reader=A_INT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
    ) == COMPATIBLE


def test_type_narrowing_incompabilities() -> None:
    assert compatibility(reader=INT_SCHEMA, writer=NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(reader=ARRAY_OF_INT_SCHEMA, writer=ARRAY_OF_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(reader=ENUM_AB_SCHEMA, writer=ENUM_ABC_SCHEMA) != COMPATIBLE
    assert compatibility(reader=ENUM_BC_SCHEMA, writer=ENUM_ABC_SCHEMA) != COMPATIBLE
    assert compatibility(
        reader=ONEOF_INT_SCHEMA,
        writer=ONEOF_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        reader=ONEOF_STRING_SCHEMA,
        writer=ONEOF_STRING_INT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(reader=INT_SCHEMA, writer=ONEOF_STRING_INT_SCHEMA) != COMPATIBLE


def test_type_mismatch_incompabilities() -> None:
    assert compatibility(reader=BOOLEAN_SCHEMA, writer=INT_SCHEMA) != COMPATIBLE
    assert compatibility(reader=INT_SCHEMA, writer=BOOLEAN_SCHEMA) != COMPATIBLE
    assert compatibility(reader=STRING_SCHEMA, writer=BOOLEAN_SCHEMA) != COMPATIBLE
    assert compatibility(reader=STRING_SCHEMA, writer=INT_SCHEMA) != COMPATIBLE
    assert compatibility(reader=ARRAY_OF_INT_SCHEMA, writer=ARRAY_OF_STRING_SCHEMA) != COMPATIBLE
    assert compatibility(
        reader=TUPLE_OF_INT_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_OPEN_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(reader=INT_SCHEMA, writer=ENUM_AB_SCHEMA) != COMPATIBLE
    assert compatibility(reader=ENUM_AB_SCHEMA, writer=INT_SCHEMA) != COMPATIBLE


def test_true_and_false_schemas() -> None:
    assert compatibility(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=NOT_OF_TRUE_SCHEMA,
        reader=FALSE_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=FALSE_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=TRUE_SCHEMA,
        reader=EMPTY_SCHEMA,
    ) == COMPATIBLE

    # the true schema accepts anything ... including nothing
    assert compatibility(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=TRUE_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=NOT_OF_TRUE_SCHEMA,
        reader=TRUE_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=TRUE_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=TRUE_SCHEMA,
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=FALSE_SCHEMA,
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
    ) != COMPATIBLE


def test_schema_strict_attributes() -> None:
    assert compatibility(
        writer=STRING_SCHEMA,
        reader=MAX_LENGTH_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MAX_LENGTH_SCHEMA,
        reader=MAX_LENGTH_DECREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=STRING_SCHEMA,
        reader=MIN_LENGTH_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_LENGTH_SCHEMA,
        reader=MIN_LENGTH_INCREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=STRING_SCHEMA,
        reader=MIN_PATTERN_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_PATTERN_SCHEMA,
        reader=MIN_PATTERN_STRICT_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(writer=INT_SCHEMA, reader=MAXIMUM_INTEGER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=INT_SCHEMA, reader=MAXIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=NUMBER_SCHEMA, reader=MAXIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=MAXIMUM_NUMBER_SCHEMA, reader=MAXIMUM_DECREASED_NUMBER_SCHEMA) != COMPATIBLE

    assert compatibility(writer=INT_SCHEMA, reader=MINIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=NUMBER_SCHEMA, reader=MINIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=MINIMUM_NUMBER_SCHEMA, reader=MINIMUM_INCREASED_NUMBER_SCHEMA) != COMPATIBLE

    assert compatibility(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=OBJECT_SCHEMA,
        reader=MAX_PROPERTIES_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MAX_PROPERTIES_SCHEMA,
        reader=MAX_PROPERTIES_DECREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=OBJECT_SCHEMA,
        reader=MIN_PROPERTIES_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_PROPERTIES_SCHEMA,
        reader=MIN_PROPERTIES_INCREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=ARRAY_SCHEMA,
        reader=MAX_ITEMS_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MAX_ITEMS_SCHEMA,
        reader=MAX_ITEMS_DECREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=ARRAY_SCHEMA,
        reader=MIN_ITEMS_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_ITEMS_SCHEMA,
        reader=MIN_ITEMS_INCREASED_SCHEMA,
    ) != COMPATIBLE


def test_schema_broadenning_attributes_is_compatible() -> None:
    assert compatibility(
        writer=MAX_LENGTH_SCHEMA,
        reader=STRING_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=MAX_LENGTH_DECREASED_SCHEMA,
        reader=MAX_LENGTH_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=MIN_LENGTH_SCHEMA,
        reader=STRING_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=MIN_LENGTH_INCREASED_SCHEMA,
        reader=MIN_LENGTH_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=MIN_PATTERN_SCHEMA,
        reader=STRING_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(writer=MAXIMUM_INTEGER_SCHEMA, reader=INT_SCHEMA) == COMPATIBLE
    assert compatibility(writer=MAXIMUM_NUMBER_SCHEMA, reader=NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(writer=MAXIMUM_DECREASED_NUMBER_SCHEMA, reader=MAXIMUM_NUMBER_SCHEMA) == COMPATIBLE

    assert compatibility(writer=MINIMUM_INTEGER_SCHEMA, reader=INT_SCHEMA) == COMPATIBLE
    assert compatibility(writer=MINIMUM_NUMBER_SCHEMA, reader=NUMBER_SCHEMA) == COMPATIBLE
    assert compatibility(writer=MINIMUM_INCREASED_NUMBER_SCHEMA, reader=MINIMUM_NUMBER_SCHEMA) == COMPATIBLE

    assert compatibility(
        writer=EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=MAX_PROPERTIES_SCHEMA,
        reader=OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=MAX_PROPERTIES_DECREASED_SCHEMA,
        reader=MAX_PROPERTIES_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=MIN_PROPERTIES_SCHEMA,
        reader=OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=MIN_PROPERTIES_INCREASED_SCHEMA,
        reader=MIN_PROPERTIES_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=MAX_ITEMS_SCHEMA,
        reader=ARRAY_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=MAX_ITEMS_DECREASED_SCHEMA,
        reader=MAX_ITEMS_SCHEMA,
    ) == COMPATIBLE

    assert compatibility(
        writer=MIN_ITEMS_SCHEMA,
        reader=ARRAY_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        writer=MIN_ITEMS_INCREASED_SCHEMA,
        reader=MIN_ITEMS_SCHEMA,
    ) == COMPATIBLE


def test_schema_restrict_attributes_is_incompatible() -> None:
    assert compatibility(
        writer=STRING_SCHEMA,
        reader=MAX_LENGTH_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MAX_LENGTH_SCHEMA,
        reader=MAX_LENGTH_DECREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=STRING_SCHEMA,
        reader=MIN_LENGTH_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_LENGTH_SCHEMA,
        reader=MIN_LENGTH_INCREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=STRING_SCHEMA,
        reader=MIN_PATTERN_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_PATTERN_SCHEMA,
        reader=MIN_PATTERN_STRICT_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(writer=INT_SCHEMA, reader=MAXIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=NUMBER_SCHEMA, reader=MAXIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=MAXIMUM_NUMBER_SCHEMA, reader=MAXIMUM_DECREASED_NUMBER_SCHEMA) != COMPATIBLE

    assert compatibility(writer=INT_SCHEMA, reader=MINIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=NUMBER_SCHEMA, reader=MINIMUM_NUMBER_SCHEMA) != COMPATIBLE
    assert compatibility(writer=MINIMUM_NUMBER_SCHEMA, reader=MINIMUM_INCREASED_NUMBER_SCHEMA) != COMPATIBLE

    assert compatibility(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=OBJECT_SCHEMA,
        reader=MAX_PROPERTIES_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MAX_PROPERTIES_SCHEMA,
        reader=MAX_PROPERTIES_DECREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=OBJECT_SCHEMA,
        reader=MIN_PROPERTIES_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_PROPERTIES_SCHEMA,
        reader=MIN_PROPERTIES_INCREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=ARRAY_SCHEMA,
        reader=MAX_ITEMS_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MAX_ITEMS_SCHEMA,
        reader=MAX_ITEMS_DECREASED_SCHEMA,
    ) != COMPATIBLE

    assert compatibility(
        writer=ARRAY_SCHEMA,
        reader=MIN_ITEMS_SCHEMA,
    ) != COMPATIBLE
    assert compatibility(
        writer=MIN_ITEMS_SCHEMA,
        reader=MIN_ITEMS_INCREASED_SCHEMA,
    ) != COMPATIBLE


def test_property_name():
    assert compatibility(
        reader=OBJECT_SCHEMA,
        writer=PROPERTY_ASTAR_OBJECT_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=A_OBJECT_SCHEMA,
        writer=PROPERTY_ASTAR_OBJECT_SCHEMA,
    ) == COMPATIBLE

    # - writer accept any value for `a`
    # - reader requires it to be an `int`, therefore the other values became
    # invalid
    assert compatibility(
        reader=A_INT_OBJECT_SCHEMA,
        writer=PROPERTY_ASTAR_OBJECT_SCHEMA,
    ) != COMPATIBLE

    # - writer has property `b`
    # - reader only accepts properties with match regex `a*`
    assert compatibility(
        reader=PROPERTY_ASTAR_OBJECT_SCHEMA,
        writer=B_INT_OBJECT_SCHEMA,
    ) != COMPATIBLE


def test_type_with_list():
    # "type": [] is treated as a shortcut for anyOf
    assert compatibility(
        reader=STRING_SCHEMA,
        writer=TYPES_STRING_SCHEMA,
    ) == COMPATIBLE
    assert compatibility(
        reader=TYPES_STRING_INT_SCHEMA,
        writer=TYPES_STRING_SCHEMA,
    ) == COMPATIBLE


def test_ref():
    assert compatibility(
        reader=ARRAY_OF_POSITIVE_INTEGER,
        writer=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
    ) == COMPATIBLE
    assert compatibility(
        reader=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
        writer=ARRAY_OF_POSITIVE_INTEGER,
    ) == COMPATIBLE
