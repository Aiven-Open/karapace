from jsonschema import Draft7Validator
from karapace.avro_compatibility import SchemaCompatibilityResult
from karapace.compatibility.jsonschema.checks import compatibility
from tests.schemas.json_schemas import (
    A_DINT_B_DINT_OBJECT_SCHEMA, A_DINT_B_INT_OBJECT_SCHEMA, A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA, A_DINT_B_NUM_OBJECT_SCHEMA,
    A_DINT_OBJECT_SCHEMA, A_INT_B_DINT_OBJECT_SCHEMA, A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA, A_INT_B_INT_OBJECT_SCHEMA,
    A_INT_B_INT_REQUIRED_OBJECT_SCHEMA, A_INT_OBJECT_SCHEMA, A_INT_OPEN_OBJECT_SCHEMA, A_OBJECT_SCHEMA, ARRAY_OF_INT_SCHEMA,
    ARRAY_OF_NUMBER_SCHEMA, ARRAY_OF_POSITIVE_INTEGER, ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF, ARRAY_OF_STRING_SCHEMA,
    ARRAY_SCHEMA, B_DINT_OPEN_OBJECT_SCHEMA, B_INT_OBJECT_SCHEMA, B_INT_OPEN_OBJECT_SCHEMA, B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
    B_NUM_C_INT_OBJECT_SCHEMA, B_NUM_C_INT_OPEN_OBJECT_SCHEMA, BOOLEAN_SCHEMA, EMPTY_OBJECT_SCHEMA, EMPTY_SCHEMA,
    ENUM_AB_SCHEMA, ENUM_ABC_SCHEMA, ENUM_BC_SCHEMA, EVERY_TYPE_SCHEMA, EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA,
    EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA, EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA, EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
    EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA, EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA, EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
    EXCLUSIVE_MINIMUM_NUMBER_SCHEMA, FALSE_SCHEMA, INT_SCHEMA, MAX_ITEMS_DECREASED_SCHEMA, MAX_ITEMS_SCHEMA,
    MAX_LENGTH_DECREASED_SCHEMA, MAX_LENGTH_SCHEMA, MAX_PROPERTIES_DECREASED_SCHEMA, MAX_PROPERTIES_SCHEMA,
    MAXIMUM_DECREASED_INTEGER_SCHEMA, MAXIMUM_DECREASED_NUMBER_SCHEMA, MAXIMUM_INTEGER_SCHEMA, MAXIMUM_NUMBER_SCHEMA,
    MIN_ITEMS_INCREASED_SCHEMA, MIN_ITEMS_SCHEMA, MIN_LENGTH_INCREASED_SCHEMA, MIN_LENGTH_SCHEMA, MIN_PATTERN_SCHEMA,
    MIN_PATTERN_STRICT_SCHEMA, MIN_PROPERTIES_INCREASED_SCHEMA, MIN_PROPERTIES_SCHEMA, MINIMUM_INCREASED_INTEGER_SCHEMA,
    MINIMUM_INCREASED_NUMBER_SCHEMA, MINIMUM_INTEGER_SCHEMA, MINIMUM_NUMBER_SCHEMA, NOT_ARRAY_SCHEMA, NOT_BOOLEAN_SCHEMA,
    NOT_INT_SCHEMA, NOT_NUMBER_SCHEMA, NOT_OBJECT_SCHEMA, NOT_OF_EMPTY_SCHEMA, NOT_OF_TRUE_SCHEMA, NOT_STRING_SCHEMA,
    NUMBER_SCHEMA, OBJECT_SCHEMA, ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA, ONEOF_ARRAY_B_NUM_C_DINT_OPEN_SCHEMA,
    ONEOF_ARRAY_B_NUM_C_INT_SCHEMA, ONEOF_INT_SCHEMA, ONEOF_NUMBER_SCHEMA, ONEOF_STRING_INT_SCHEMA, ONEOF_STRING_SCHEMA,
    PROPERTY_ASTAR_OBJECT_SCHEMA, STRING_SCHEMA, TRUE_SCHEMA, TUPLE_OF_INT_INT_SCHEMA, TUPLE_OF_INT_OPEN_SCHEMA,
    TUPLE_OF_INT_SCHEMA, TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA, TYPES_STRING_INT_SCHEMA, TYPES_STRING_SCHEMA
)

COMPATIBLE = SchemaCompatibilityResult.compatible()

COMPATIBLE_READER_IS_TRUE_SCHEMA = "The reader is a true schema which _accepts_ every value"
COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES = "The reader schema is an open schema and ignores unknown values"
COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED = "The new fields in the reader schema are not required"
COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET = "The reader schema changed a field type which accepts all writer values"
COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED = "Every value produced by the writer is accepted by the reader"

INCOMPATIBLE_READER_IS_FALSE_SCHEMA = "The reader is a false schema which _rejects_ every value"
INCOMPATIBLE_READER_IS_CLOSED_AND_REMOVED_FIELD = "The does not accepts all fields produced by the writer"
INCOMPATIBLE_READER_HAS_A_NEW_REQUIRED_FIELDg = "The reader has a new required field"
INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES = (
    "The reader changed restricted field and only accept a subset of the writer's values"
)
INCOMPATIBLE_READER_CHANGED_FIELD_TYPE = "The reader schema changed a field type, the previous values are no longer valid"


def schemas_are_compatible(
    reader: Draft7Validator,
    writer: Draft7Validator,
    msg: str,
) -> None:
    assert compatibility(reader=reader, writer=writer) == COMPATIBLE, msg


def not_schemas_are_compatible(
    reader: Draft7Validator,
    writer: Draft7Validator,
    msg: str,
) -> None:
    assert compatibility(reader=reader, writer=writer) != COMPATIBLE, msg


def test_reflexivity() -> None:
    reflexivity_msg = "every schema is compatible with itself"
    schemas_are_compatible(
        reader=EVERY_TYPE_SCHEMA,
        writer=EVERY_TYPE_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_OF_EMPTY_SCHEMA,
        writer=NOT_OF_EMPTY_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_OF_TRUE_SCHEMA,
        writer=NOT_OF_TRUE_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=FALSE_SCHEMA,
        writer=FALSE_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=TRUE_SCHEMA,
        writer=TRUE_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EMPTY_SCHEMA,
        writer=EMPTY_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=BOOLEAN_SCHEMA,
        writer=BOOLEAN_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=INT_SCHEMA,
        writer=INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NUMBER_SCHEMA,
        writer=NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=STRING_SCHEMA,
        writer=STRING_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=OBJECT_SCHEMA,
        writer=OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ARRAY_SCHEMA,
        writer=ARRAY_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_BOOLEAN_SCHEMA,
        writer=NOT_BOOLEAN_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_INT_SCHEMA,
        writer=NOT_INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_NUMBER_SCHEMA,
        writer=NOT_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_STRING_SCHEMA,
        writer=NOT_STRING_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_OBJECT_SCHEMA,
        writer=NOT_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=NOT_ARRAY_SCHEMA,
        writer=NOT_ARRAY_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAX_LENGTH_SCHEMA,
        writer=MAX_LENGTH_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAX_LENGTH_DECREASED_SCHEMA,
        writer=MAX_LENGTH_DECREASED_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_LENGTH_SCHEMA,
        writer=MIN_LENGTH_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_LENGTH_INCREASED_SCHEMA,
        writer=MIN_LENGTH_INCREASED_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_PATTERN_SCHEMA,
        writer=MIN_PATTERN_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_PATTERN_STRICT_SCHEMA,
        writer=MIN_PATTERN_STRICT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAXIMUM_INTEGER_SCHEMA,
        writer=MAXIMUM_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAXIMUM_NUMBER_SCHEMA,
        writer=MAXIMUM_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAXIMUM_DECREASED_INTEGER_SCHEMA,
        writer=MAXIMUM_DECREASED_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAXIMUM_DECREASED_NUMBER_SCHEMA,
        writer=MAXIMUM_DECREASED_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MINIMUM_INTEGER_SCHEMA,
        writer=MINIMUM_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MINIMUM_NUMBER_SCHEMA,
        writer=MINIMUM_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MINIMUM_INCREASED_INTEGER_SCHEMA,
        writer=MINIMUM_INCREASED_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MINIMUM_INCREASED_NUMBER_SCHEMA,
        writer=MINIMUM_INCREASED_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA,
        writer=EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA,
        writer=EXCLUSIVE_MAXIMUM_DECREASED_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
        writer=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
        writer=EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA,
        writer=EXCLUSIVE_MINIMUM_INCREASED_INTEGER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
        writer=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAX_PROPERTIES_SCHEMA,
        writer=MAX_PROPERTIES_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAX_PROPERTIES_DECREASED_SCHEMA,
        writer=MAX_PROPERTIES_DECREASED_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_PROPERTIES_SCHEMA,
        writer=MIN_PROPERTIES_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_PROPERTIES_INCREASED_SCHEMA,
        writer=MIN_PROPERTIES_INCREASED_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAX_ITEMS_SCHEMA,
        writer=MAX_ITEMS_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MAX_ITEMS_DECREASED_SCHEMA,
        writer=MAX_ITEMS_DECREASED_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_ITEMS_SCHEMA,
        writer=MIN_ITEMS_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=MIN_ITEMS_INCREASED_SCHEMA,
        writer=MIN_ITEMS_INCREASED_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=TUPLE_OF_INT_INT_SCHEMA,
        writer=TUPLE_OF_INT_INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=TUPLE_OF_INT_SCHEMA,
        writer=TUPLE_OF_INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA,
        writer=TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=TUPLE_OF_INT_INT_SCHEMA,
        writer=TUPLE_OF_INT_INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_OPEN_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=ARRAY_OF_INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_NUMBER_SCHEMA,
        writer=ARRAY_OF_NUMBER_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_STRING_SCHEMA,
        writer=ARRAY_OF_STRING_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ENUM_AB_SCHEMA,
        writer=ENUM_AB_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ENUM_ABC_SCHEMA,
        writer=ENUM_ABC_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ENUM_BC_SCHEMA,
        writer=ENUM_BC_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ONEOF_STRING_SCHEMA,
        writer=ONEOF_STRING_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ONEOF_STRING_INT_SCHEMA,
        writer=ONEOF_STRING_INT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=EMPTY_OBJECT_SCHEMA,
        writer=EMPTY_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_INT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_OPEN_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_INT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_B_INT_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        writer=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_INT_B_DINT_OBJECT_SCHEMA,
        writer=A_INT_B_DINT_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA,
        writer=A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_DINT_B_DINT_OBJECT_SCHEMA,
        writer=A_DINT_B_DINT_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_DINT_B_NUM_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=B_NUM_C_INT_OBJECT_SCHEMA,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_POSITIVE_INTEGER,
        writer=ARRAY_OF_POSITIVE_INTEGER,
        msg=reflexivity_msg,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
        writer=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
        msg=reflexivity_msg,
    )


def test_extra_optional_field_with_open_model_is_compatible() -> None:
    # - the reader is an open model, the extra field produced by the writer is
    # automatically accepted
    schemas_are_compatible(
        reader=OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_TRUE_SCHEMA,
    )
    schemas_are_compatible(
        reader=TRUE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_TRUE_SCHEMA,
    )
    schemas_are_compatible(
        reader=EMPTY_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_TRUE_SCHEMA,
    )

    # - the writer is a closed model, so the field `b` was never produced, which
    # means that the writer never produced an invalid value.
    # - the reader's `b` field is optional, so the absenced of the field is not
    # a problem, and `a` is ignored because of the open model
    schemas_are_compatible(
        reader=B_INT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES,
    )

    # - if the model is closed, then `a` must also be accepted
    schemas_are_compatible(
        reader=A_INT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED,
    )

    # Examples a bit more complex
    schemas_are_compatible(
        reader=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED,
    )
    schemas_are_compatible(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES,
    )
    schemas_are_compatible(
        reader=B_NUM_C_INT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        msg=f"{COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES} + {COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED}",
    )
    schemas_are_compatible(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        msg=f"{COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES} + {COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED}",
    )


def test_extra_field_with_closed_model_is_incompatible() -> None:
    # The field here is not required but forbidden, because of this the reader
    # will reject the writer data
    not_schemas_are_compatible(
        reader=FALSE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_FALSE_SCHEMA,
    )
    not_schemas_are_compatible(
        reader=NOT_OF_TRUE_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_FALSE_SCHEMA,
    )
    not_schemas_are_compatible(
        reader=NOT_OF_EMPTY_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_FALSE_SCHEMA,
    )
    not_schemas_are_compatible(
        reader=B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_CLOSED_AND_REMOVED_FIELD,
    )
    not_schemas_are_compatible(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_CLOSED_AND_REMOVED_FIELD,
    )
    not_schemas_are_compatible(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_C_DINT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_CLOSED_AND_REMOVED_FIELD,
    )


def test_missing_required_field_is_incompatible() -> None:
    not_schemas_are_compatible(
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_HAS_A_NEW_REQUIRED_FIELDg,
    )
    not_schemas_are_compatible(
        reader=A_INT_B_DINT_REQUIRED_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_HAS_A_NEW_REQUIRED_FIELDg,
    )


def test_giving_a_default_value_for_a_non_required_field_is_compatible() -> None:
    schemas_are_compatible(
        reader=OBJECT_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_TRUE_SCHEMA,
    )
    schemas_are_compatible(
        reader=TRUE_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_TRUE_SCHEMA,
    )
    schemas_are_compatible(
        reader=EMPTY_SCHEMA,
        writer=A_DINT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_TRUE_SCHEMA,
    )
    schemas_are_compatible(
        reader=B_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_TRUE_SCHEMA,
    )
    schemas_are_compatible(
        reader=A_INT_B_DINT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED,
    )
    schemas_are_compatible(
        reader=A_DINT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED,
    )
    schemas_are_compatible(
        reader=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        writer=A_DINT_B_NUM_OBJECT_SCHEMA,
        msg=f"{COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES} + {COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED}",
    )
    schemas_are_compatible(
        reader=A_DINT_B_DINT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,  # field B was anything, now it int
    )
    schemas_are_compatible(
        reader=A_DINT_B_DINT_OBJECT_SCHEMA,
        writer=EMPTY_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_NEW_FIELD_IS_NOT_REQUIRED,
    )


def test_from_closed_to_open_is_incompatible() -> None:
    not_schemas_are_compatible(
        reader=FALSE_SCHEMA,
        writer=TRUE_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_FALSE_SCHEMA,
    )
    not_schemas_are_compatible(
        reader=B_NUM_C_INT_OBJECT_SCHEMA,
        writer=B_NUM_C_DINT_OPEN_OBJECT_SCHEMA,
        msg="The reader is closed model and rejects the fields ignored by the writer",
    )


def test_union_with_incompatible_elements() -> None:
    not_schemas_are_compatible(
        reader=ONEOF_ARRAY_B_NUM_C_INT_SCHEMA,
        writer=ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_CLOSED_AND_REMOVED_FIELD,
    )


def test_union_with_compatible_elements() -> None:
    schemas_are_compatible(
        reader=ONEOF_ARRAY_B_NUM_C_DINT_OPEN_SCHEMA,
        writer=ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA,
        msg=COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES,
    )


def test_schema_compatibility_successes() -> None:
    # allowing a broader set of values is compatible
    schemas_are_compatible(
        reader=NUMBER_SCHEMA,
        writer=INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_NUMBER_SCHEMA,
        writer=ARRAY_OF_INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA,
        writer=TUPLE_OF_INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=TUPLE_OF_INT_OPEN_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=TUPLE_OF_INT_INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=ENUM_ABC_SCHEMA,
        writer=ENUM_AB_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=ONEOF_STRING_INT_SCHEMA,
        writer=ONEOF_STRING_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=ONEOF_STRING_INT_SCHEMA,
        writer=STRING_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=A_INT_OPEN_OBJECT_SCHEMA,
        writer=A_INT_B_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES,
    )

    # requiring less values is compatible
    schemas_are_compatible(
        reader=TUPLE_OF_INT_OPEN_SCHEMA,
        writer=TUPLE_OF_INT_INT_SCHEMA,
        msg=COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES,
    )

    # equivalences
    schemas_are_compatible(
        reader=ONEOF_STRING_SCHEMA,
        writer=STRING_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        reader=STRING_SCHEMA,
        writer=ONEOF_STRING_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )

    # new non-required fields is compatible
    schemas_are_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=EMPTY_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )
    schemas_are_compatible(
        reader=A_INT_B_INT_OBJECT_SCHEMA,
        writer=A_INT_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )


def test_type_narrowing_incompabilities() -> None:
    not_schemas_are_compatible(
        reader=INT_SCHEMA,
        writer=NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=ARRAY_OF_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=ENUM_AB_SCHEMA,
        writer=ENUM_ABC_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=ENUM_BC_SCHEMA,
        writer=ENUM_ABC_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=ONEOF_INT_SCHEMA,
        writer=ONEOF_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=ONEOF_STRING_SCHEMA,
        writer=ONEOF_STRING_INT_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=INT_SCHEMA,
        writer=ONEOF_STRING_INT_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )


def test_type_mismatch_incompabilities() -> None:
    not_schemas_are_compatible(
        reader=BOOLEAN_SCHEMA,
        writer=INT_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=INT_SCHEMA,
        writer=BOOLEAN_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=STRING_SCHEMA,
        writer=BOOLEAN_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=STRING_SCHEMA,
        writer=INT_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=ARRAY_OF_INT_SCHEMA,
        writer=ARRAY_OF_STRING_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=TUPLE_OF_INT_INT_SCHEMA,
        writer=TUPLE_OF_INT_OPEN_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=INT_SCHEMA,
        writer=ENUM_AB_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        reader=ENUM_AB_SCHEMA,
        writer=INT_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )


def test_true_and_false_schemas() -> None:
    schemas_are_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
        msg="both schemas reject every value",
    )
    schemas_are_compatible(
        writer=NOT_OF_TRUE_SCHEMA,
        reader=FALSE_SCHEMA,
        msg="both schemas reject every value",
    )
    schemas_are_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=FALSE_SCHEMA,
        msg="both schemas reject every value",
    )

    schemas_are_compatible(
        writer=TRUE_SCHEMA,
        reader=EMPTY_SCHEMA,
        msg="both schemas accept every value",
    )

    # the true schema accepts anything ... including nothing
    schemas_are_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=TRUE_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )
    schemas_are_compatible(
        writer=NOT_OF_TRUE_SCHEMA,
        reader=TRUE_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )
    schemas_are_compatible(
        writer=NOT_OF_EMPTY_SCHEMA,
        reader=TRUE_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )

    not_schemas_are_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_FALSE_SCHEMA,
    )
    not_schemas_are_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_TRUE_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_FALSE_SCHEMA,
    )
    not_schemas_are_compatible(
        writer=TRUE_SCHEMA,
        reader=NOT_OF_EMPTY_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_FALSE_SCHEMA,
    )

    not_schemas_are_compatible(
        writer=TRUE_SCHEMA,
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_CHANGED_FIELD_TYPE,
    )
    not_schemas_are_compatible(
        writer=FALSE_SCHEMA,
        reader=A_INT_B_INT_REQUIRED_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_HAS_A_NEW_REQUIRED_FIELDg,
    )


def test_schema_restrict_attributes_is_incompatible() -> None:
    not_schemas_are_compatible(
        writer=STRING_SCHEMA,
        reader=MAX_LENGTH_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MAX_LENGTH_SCHEMA,
        reader=MAX_LENGTH_DECREASED_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=STRING_SCHEMA,
        reader=MIN_LENGTH_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MIN_LENGTH_SCHEMA,
        reader=MIN_LENGTH_INCREASED_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=STRING_SCHEMA,
        reader=MIN_PATTERN_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MIN_PATTERN_SCHEMA,
        reader=MIN_PATTERN_STRICT_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=INT_SCHEMA,
        reader=MAXIMUM_INTEGER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=INT_SCHEMA,
        reader=MAXIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=NUMBER_SCHEMA,
        reader=MAXIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MAXIMUM_NUMBER_SCHEMA,
        reader=MAXIMUM_DECREASED_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=INT_SCHEMA,
        reader=MINIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=NUMBER_SCHEMA,
        reader=MINIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MINIMUM_NUMBER_SCHEMA,
        reader=MINIMUM_INCREASED_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=INT_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=OBJECT_SCHEMA,
        reader=MAX_PROPERTIES_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MAX_PROPERTIES_SCHEMA,
        reader=MAX_PROPERTIES_DECREASED_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=OBJECT_SCHEMA,
        reader=MIN_PROPERTIES_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MIN_PROPERTIES_SCHEMA,
        reader=MIN_PROPERTIES_INCREASED_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=ARRAY_SCHEMA,
        reader=MAX_ITEMS_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MAX_ITEMS_SCHEMA,
        reader=MAX_ITEMS_DECREASED_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    not_schemas_are_compatible(
        writer=ARRAY_SCHEMA,
        reader=MIN_ITEMS_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )
    not_schemas_are_compatible(
        writer=MIN_ITEMS_SCHEMA,
        reader=MIN_ITEMS_INCREASED_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )


def test_schema_broadenning_attributes_is_compatible() -> None:
    schemas_are_compatible(
        writer=MAX_LENGTH_SCHEMA,
        reader=STRING_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MAX_LENGTH_DECREASED_SCHEMA,
        reader=MAX_LENGTH_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MIN_LENGTH_SCHEMA,
        reader=STRING_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MIN_LENGTH_INCREASED_SCHEMA,
        reader=MIN_LENGTH_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MIN_PATTERN_SCHEMA,
        reader=STRING_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MAXIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MAXIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MAXIMUM_DECREASED_NUMBER_SCHEMA,
        reader=MAXIMUM_NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MINIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MINIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MINIMUM_INCREASED_NUMBER_SCHEMA,
        reader=MINIMUM_NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=EXCLUSIVE_MAXIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=EXCLUSIVE_MAXIMUM_DECREASED_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MAXIMUM_NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        reader=NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=EXCLUSIVE_MINIMUM_INTEGER_SCHEMA,
        reader=INT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=EXCLUSIVE_MINIMUM_INCREASED_NUMBER_SCHEMA,
        reader=EXCLUSIVE_MINIMUM_NUMBER_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MAX_PROPERTIES_SCHEMA,
        reader=OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MAX_PROPERTIES_DECREASED_SCHEMA,
        reader=MAX_PROPERTIES_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MIN_PROPERTIES_SCHEMA,
        reader=OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MIN_PROPERTIES_INCREASED_SCHEMA,
        reader=MIN_PROPERTIES_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MAX_ITEMS_SCHEMA,
        reader=ARRAY_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MAX_ITEMS_DECREASED_SCHEMA,
        reader=MAX_ITEMS_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )

    schemas_are_compatible(
        writer=MIN_ITEMS_SCHEMA,
        reader=ARRAY_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )
    schemas_are_compatible(
        writer=MIN_ITEMS_INCREASED_SCHEMA,
        reader=MIN_ITEMS_SCHEMA,
        msg=COMPATIBLE_READER_FIELD_TYPE_IS_A_SUPERSET,
    )


def test_property_name():
    schemas_are_compatible(
        reader=OBJECT_SCHEMA,
        writer=PROPERTY_ASTAR_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES,
    )
    schemas_are_compatible(
        reader=A_OBJECT_SCHEMA,
        writer=PROPERTY_ASTAR_OBJECT_SCHEMA,
        msg=COMPATIBLE_READER_IS_OPEN_AND_IGNORE_UNKNOWN_VALUES,
    )

    # - writer accept any value for `a`
    # - reader requires it to be an `int`, therefore the other values became
    # invalid
    not_schemas_are_compatible(
        reader=A_INT_OBJECT_SCHEMA,
        writer=PROPERTY_ASTAR_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_RESTRICTED_ACCEPTED_VALUES,
    )

    # - writer has property `b`
    # - reader only accepts properties with match regex `a*`
    not_schemas_are_compatible(
        reader=PROPERTY_ASTAR_OBJECT_SCHEMA,
        writer=B_INT_OBJECT_SCHEMA,
        msg=INCOMPATIBLE_READER_IS_CLOSED_AND_REMOVED_FIELD,
    )


def test_type_with_list():
    # "type": [] is treated as a shortcut for anyOf
    schemas_are_compatible(
        reader=STRING_SCHEMA,
        writer=TYPES_STRING_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )
    schemas_are_compatible(
        reader=TYPES_STRING_INT_SCHEMA,
        writer=TYPES_STRING_SCHEMA,
        msg=COMPATIBLE_READER_EVERY_VALUE_IS_ACCEPTED,
    )


def test_ref():
    schemas_are_compatible(
        reader=ARRAY_OF_POSITIVE_INTEGER,
        writer=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
        msg="the schemas are the same",
    )
    schemas_are_compatible(
        reader=ARRAY_OF_POSITIVE_INTEGER_THROUGH_REF,
        writer=ARRAY_OF_POSITIVE_INTEGER,
        msg="the schemas are the same",
    )
