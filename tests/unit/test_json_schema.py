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
    PROPERTY_ASTAR_OBJECT_SCHEMA, STRING_SCHEMA, TRUE_SCHEMA, TUPLE_OF_INT_INT_OPEN_SCHEMA, TUPLE_OF_INT_INT_SCHEMA,
    TUPLE_OF_INT_OPEN_SCHEMA, TUPLE_OF_INT_SCHEMA, TUPLE_OF_INT_WITH_ADDITIONAL_INT_SCHEMA, TYPES_STRING_INT_SCHEMA,
    TYPES_STRING_SCHEMA
)

COMPATIBLE = SchemaCompatibilityResult.compatible()


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
    assert compatibility(reader=ONEOF_ARRAY_B_NUM_C_INT_SCHEMA, writer=ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA) != COMPATIBLE


def test_union_with_compatible_elements() -> None:
    assert compatibility(reader=ONEOF_ARRAY_B_NUM_C_DINT_OPEN_SCHEMA, writer=ONEOF_ARRAY_A_DINT_B_NUM_SCHEMA) == COMPATIBLE


def test_reflexivity() -> None:
    assert compatibility(reader=EVERY_TYPE_SCHEMA, writer=EVERY_TYPE_SCHEMA) == COMPATIBLE
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
