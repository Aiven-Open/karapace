from karapace.schema_reader import parse_jsonschema_definition

# boolean schemas
NOT_OF_EMPTY_SCHEMA = parse_jsonschema_definition('{"not":{}}')
NOT_OF_TRUE_SCHEMA = parse_jsonschema_definition('{"not":true}')
FALSE_SCHEMA = parse_jsonschema_definition('false')
TRUE_SCHEMA = parse_jsonschema_definition('true')
EMPTY_SCHEMA = parse_jsonschema_definition('{}')

# simple instance schemas
BOOLEAN_SCHEMA = parse_jsonschema_definition('{"type":"boolean"}')
INT_SCHEMA = parse_jsonschema_definition('{"type":"integer"}')
NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number"}')
STRING_SCHEMA = parse_jsonschema_definition('{"type":"string"}')
OBJECT_SCHEMA = parse_jsonschema_definition('{"type":"object"}')
ARRAY_SCHEMA = parse_jsonschema_definition('{"type":"array"}')

# negation of simple schemas
NOT_BOOLEAN_SCHEMA = parse_jsonschema_definition('{"not":{"type":"boolean"}}')
NOT_INT_SCHEMA = parse_jsonschema_definition('{"not":{"type":"integer"}}')
NOT_NUMBER_SCHEMA = parse_jsonschema_definition('{"not":{"type":"number"}}')
NOT_STRING_SCHEMA = parse_jsonschema_definition('{"not":{"type":"string"}}')
NOT_OBJECT_SCHEMA = parse_jsonschema_definition('{"not":{"type":"object"}}')
NOT_ARRAY_SCHEMA = parse_jsonschema_definition('{"not":{"type":"array"}}')

# structural validation
MAX_LENGTH_SCHEMA = parse_jsonschema_definition('{"type":"string","maxLength":3}')
MAX_LENGTH_DECREASED_SCHEMA = parse_jsonschema_definition('{"type":"string","maxLength":2}')
MIN_LENGTH_SCHEMA = parse_jsonschema_definition('{"type":"string","minLength":5}')
MIN_LENGTH_INCREASED_SCHEMA = parse_jsonschema_definition('{"type":"string","minLength":7}')
MIN_PATTERN_SCHEMA = parse_jsonschema_definition('{"type":"string","pattern":"a*"}')
MIN_PATTERN_STRICT_SCHEMA = parse_jsonschema_definition('{"type":"string","pattern":"a+"}')
MAXIMUM_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","maximum":13}')
MAXIMUM_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","maximum":13}')
MAXIMUM_DECREASED_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","maximum":11}')
MAXIMUM_DECREASED_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","maximum":11}')
MINIMUM_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","minimum":17}')
MINIMUM_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","minimum":17}')
MINIMUM_INCREASED_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","minimum":19}')
MINIMUM_INCREASED_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","minimum":19}')
EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","exclusiveMaximum":29}')
EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","exclusiveMaximum":29}')
EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","exclusiveMaximum":23}')
EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","exclusiveMaximum":23}')
EXCLUSIVE_MINIMUM_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","exclusiveMinimum":31}')
EXCLUSIVE_MINIMUM_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","exclusiveMinimum":31}')
EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA = parse_jsonschema_definition('{"type":"integer","exclusiveMinimum":37}')
EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"number","exclusiveMinimum":37}')
MAX_PROPERTIES_SCHEMA = parse_jsonschema_definition('{"type":"object","maxProperties":43}')
MAX_PROPERTIES_DECREASED_SCHEMA = parse_jsonschema_definition('{"type":"object","maxProperties":41}')
MIN_PROPERTIES_SCHEMA = parse_jsonschema_definition('{"type":"object","minProperties":47}')
MIN_PROPERTIES_INCREASED_SCHEMA = parse_jsonschema_definition('{"type":"object","minProperties":53}')
MAX_ITEMS_SCHEMA = parse_jsonschema_definition('{"type":"array","maxItems":61}')
MAX_ITEMS_DECREASED_SCHEMA = parse_jsonschema_definition('{"type":"array","maxItems":59}')
MIN_ITEMS_SCHEMA = parse_jsonschema_definition('{"type":"array","minItems":67}')
MIN_ITEMS_INCREASED_SCHEMA = parse_jsonschema_definition('{"type":"array","minItems":71}')

TUPLE_OF_INT_INT_SCHEMA = parse_jsonschema_definition(
    '{"type":"array","items":[{"type":"integer"},{"type":"integer"}],"additionalItems":false}'
)
TUPLE_OF_INT_SCHEMA = parse_jsonschema_definition('{"type":"array","items":[{"type":"integer"}],"additionalItems":false}')
TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA = parse_jsonschema_definition(
    '{"type":"array","items":[{"type":"integer"}],"additionalItems":{"type":"integer"}}'
)
TUPLE_OF_INT_INT_OPEN_SCHEMA = parse_jsonschema_definition(
    '{"type":"array","items":[{"type":"integer"},{"type":"integer"}]}'
)
TUPLE_OF_INT_OPEN_SCHEMA = parse_jsonschema_definition('{"type":"array","items":[{"type":"integer"}]}')
ARRAY_OF_INT_SCHEMA = parse_jsonschema_definition('{"type":"array","items":{"type":"integer"}}')
ARRAY_OF_NUMBER_SCHEMA = parse_jsonschema_definition('{"type":"array","items":{"type":"number"}}')
ARRAY_OF_STRING_SCHEMA = parse_jsonschema_definition('{"type":"array","items":{"type":"string"}}')
ENUM_AB_SCHEMA = parse_jsonschema_definition('{"enum":["A","B"]}')
ENUM_ABC_SCHEMA = parse_jsonschema_definition('{"enum":["A","B","C"]}')
ENUM_BC_SCHEMA = parse_jsonschema_definition('{"enum":["B","C"]}')
ONEOF_STRING_SCHEMA = parse_jsonschema_definition('{"oneOf":[{"type":"string"}]}')
ONEOF_STRING_INT_SCHEMA = parse_jsonschema_definition('{"oneOf":[{"type":"string"},{"type":"integer"}]}')
ONEOF_INT_SCHEMA = parse_jsonschema_definition('{"oneOf":[{"type":"integer"}]}')
ONEOF_NUMBER_SCHEMA = parse_jsonschema_definition('{"oneOf":[{"type":"number"}]}')
TYPES_STRING_INT_SCHEMA = parse_jsonschema_definition('{"type":["string","integer"]}')
TYPES_STRING_SCHEMA = parse_jsonschema_definition('{"type":["string"]}')
EMPTY_OBJECT_SCHEMA = parse_jsonschema_definition('{"type":"object","additionalProperties":false}')
A_OBJECT_SCHEMA = parse_jsonschema_definition('{"type":"object","properties":{"a":{}}}')
A_INT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}'
)
A_DINT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer","default":0}}}'
)
B_INT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"b":{"type":"integer"}}}'
)
A_INT_OPEN_OBJECT_SCHEMA = parse_jsonschema_definition('{"type":"object","properties":{"a":{"type":"integer"}}}')
B_INT_OPEN_OBJECT_SCHEMA = parse_jsonschema_definition('{"type":"object","properties":{"b":{"type":"integer"}}}')
B_DINT_OPEN_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","properties":{"b":{"type":"integer","default":0}}}'
)
A_INT_B_INT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"integer"}}}'
)
A_DINT_B_INT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer","default":0},"b":{"type":"integer"}}}'
)
A_INT_B_INT_REQUIRED_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"required":["b"],'
    '"properties":{"a":{"type":"integer"},"b":{"type":"integer"}}}'
)
A_INT_B_DINT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"integer","default":0}}}'
)
A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"required":["b"],'
    '"properties":{"a":{"type":"integer"},"b":{"type":"integer","default":0}}}'
)
A_DINT_B_DINT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,'
    '"properties":{"a":{"type":"integer","default":0},"b":{"type":"integer","default":0}}}'
)
A_DINT_B_NUM_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer","default":1},"b":{"type":"number"}}}'
)
A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,'
    '"properties":{"a":{"type":"integer","default":1},"b":{"type":"number"},"c":{"type":"integer","default":0}}}'
)
B_NUM_C_DINT_OPEN_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","properties":{"b":{"type":"number"},"c":{"type":"integer","default":0}}}'
)
B_NUM_C_INT_OPEN_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","properties":{"b":{"type":"number"},"c":{"type":"integer"}}}'
)
B_NUM_C_INT_OBJECT_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","additionalProperties":false,"properties":{"b":{"type":"number"},"c":{"type":"integer"}}}'
)
PROPERTY_ASTAR_OBJECT_SCHEMA = parse_jsonschema_definition('{"type":"object","propertyNames":{"pattern":"a*"}}')
ARRAY_OF_POSITIVE_INTEGER = parse_jsonschema_definition(
    '''
    {
        "type": "array",
        "items": {"type": "integer", "exclusiveMinimum": 0}
    }
    '''
)
ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF = parse_jsonschema_definition(
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

ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA = parse_jsonschema_definition(
    '{"oneOf":[{"type":"array","items":{"type":"object",'
    '"additionalProperties":false,"properties":{"a":{"type":"integer","default":1},"b":{"type":"number"}}}}]}'
)
ONEOF_ARRAY_B_NUM_C_INT_SCHEMA = parse_jsonschema_definition(
    '{"oneOf":[{"type":"array","items":{"type":"object",'
    '"additionalProperties":false,"properties":{"b":{"type":"number"},"c":{"type":"integer"}}}}]}'
)
ONEOF_ARRAY_B_NUM_C_DINT_OPEN_SCHEMA = parse_jsonschema_definition(
    '{"oneOf":[{"type":"array","items":{"type":"object",'
    '"properties":{"b":{"type":"number"},"c":{"type":"integer","default":0}}}}]}'
)
EVERY_TYPE_SCHEMA = parse_jsonschema_definition(
    '{"type":"object","required":["boolF","intF","numberF","stringF","enumF","arrayF","recordF"],'
    '"properties":{"recordF":{"type":"object","properties":{"f":{"type":"number"}}},"stringF":{"type":"string"},'
    '"boolF":{"type":"boolean"},"intF":{"type":"integer"},"enumF":{"enum":["S"]},'
    '"arrayF":{"type":"array","items":{"type":"string"}},"numberF":{"type":"number"},"bool0":{"type":"boolean"}}}'
)
