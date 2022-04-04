from avro.compatibility import merge, SchemaCompatibilityResult, SchemaCompatibilityType, SchemaIncompatibilityType
from dataclasses import dataclass
from itertools import product
from jsonschema import Draft7Validator
from karapace.compatibility.jsonschema.types import (
    AssertionCheck,
    BooleanSchema,
    Incompatibility,
    Instance,
    Keyword,
    Subschema,
)
from karapace.compatibility.jsonschema.utils import (
    get_name_of,
    get_type_of,
    gt,
    introduced_constraint,
    is_false_schema,
    is_object_content_model_open,
    is_simple_subschema,
    is_true_schema,
    is_tuple,
    is_tuple_without_additional_items,
    JSONSCHEMA_TYPES,
    lt,
    maybe_get_subschemas_and_type,
    ne,
    normalize_schema,
    schema_from_partially_open_content_model,
)
from typing import Any, List, Optional

import networkx as nx

INTRODUCED_INCOMPATIBILITY_MSG_FMT = "Introduced incompatible assertion {assert_name} with value {introduced_value}"
RESTRICTED_INCOMPATIBILITY_MSG_FMT = "More restrictive assertion {assert_name} from {writer_value} to {reader_value}"
MODIFIED_INCOMPATIBILITY_MSG_FMT = "Assertion of {assert_name} changed from {writer_value} to {reader_value}"

MAX_LENGTH_CHECK = AssertionCheck(
    keyword=Keyword.MAX_LENGTH,
    error_when_introducing=Incompatibility.max_length_added,
    error_when_restricting=Incompatibility.max_length_decreased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=lt,
)
MIN_LENGTH_CHECK = AssertionCheck(
    keyword=Keyword.MIN_LENGTH,
    error_when_introducing=Incompatibility.min_length_added,
    error_when_restricting=Incompatibility.min_length_increased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=gt,
)
PATTERN_CHECK = AssertionCheck(
    keyword=Keyword.PATTERN,
    error_when_introducing=Incompatibility.pattern_added,
    error_when_restricting=Incompatibility.pattern_changed,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=MODIFIED_INCOMPATIBILITY_MSG_FMT,
    comparison=ne,
)
MAXIMUM_CHECK = AssertionCheck(
    keyword=Keyword.MAXIMUM,
    error_when_introducing=Incompatibility.maximum_added,
    error_when_restricting=Incompatibility.maximum_decreased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=lt,
)
MINIMUM_CHECK = AssertionCheck(
    keyword=Keyword.MINIMUM,
    error_when_introducing=Incompatibility.minimum_added,
    error_when_restricting=Incompatibility.minimum_increased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=gt,
)
EXCLUSIVE_MAXIMUM_CHECK = AssertionCheck(
    keyword=Keyword.EXCLUSIVE_MAXIMUM,
    error_when_introducing=Incompatibility.maximum_exclusive_added,
    error_when_restricting=Incompatibility.maximum_exclusive_decreased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=lt,
)
EXCLUSIVE_MINIMUM_CHECK = AssertionCheck(
    keyword=Keyword.EXCLUSIVE_MINIMUM,
    error_when_introducing=Incompatibility.minimum_exclusive_added,
    error_when_restricting=Incompatibility.minimum_exclusive_increased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=gt,
)
MAX_PROPERTIES_CHECK = AssertionCheck(
    keyword=Keyword.MAX_PROPERTIES,
    error_when_introducing=Incompatibility.max_properties_added,
    error_when_restricting=Incompatibility.max_properties_decreased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=lt,
)
MIN_PROPERTIES_CHECK = AssertionCheck(
    keyword=Keyword.MIN_PROPERTIES,
    error_when_introducing=Incompatibility.min_properties_added,
    error_when_restricting=Incompatibility.min_properties_increased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=gt,
)
MAX_ITEMS_CHECK = AssertionCheck(
    keyword=Keyword.MAX_ITEMS,
    error_when_introducing=Incompatibility.max_items_added,
    error_when_restricting=Incompatibility.max_items_decreased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=lt,
)
MIN_ITEMS_CHECK = AssertionCheck(
    keyword=Keyword.MIN_ITEMS,
    error_when_introducing=Incompatibility.min_items_added,
    error_when_restricting=Incompatibility.min_items_increased,
    error_msg_when_introducing=INTRODUCED_INCOMPATIBILITY_MSG_FMT,
    error_msg_when_restricting=RESTRICTED_INCOMPATIBILITY_MSG_FMT,
    comparison=gt,
)


def type_mismatch(
    reader_type: JSONSCHEMA_TYPES,
    writer_type: JSONSCHEMA_TYPES,
    location: List[str],
) -> SchemaCompatibilityResult:
    locations = "/".join(location)
    if len(location) > 1:  # Remove ROOT_REFERENCE_TOKEN
        locations = locations[1:]
    return SchemaCompatibilityResult(
        compatibility=SchemaCompatibilityType.incompatible,
        incompatibilities=[Incompatibility.type_changed],
        locations={locations},
        messages={f"type {reader_type} is not compatible with type {writer_type}"},
    )


def count_uniquely_compatible_schemas(reader_type: Instance, reader_schema, writer_schema, location: List[str]) -> int:
    # allOf/anyOf/oneOf subschemas do not enforce order, as a consequence the
    # new schema may change the order of the entries without breaking
    # compatibility.
    #
    #    Subschemas of these keywords evaluate the instance completely
    #    independently such that the results of one such subschema MUST NOT
    #    impact the results of sibling subschemas. Therefore subschemas may be
    #    applied in any order.
    #
    # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.10.2
    @dataclass(unsafe_hash=True, frozen=True)
    class Node:
        reader: str
        pos: int

    reader_node_schema = [(Node("reader", reader_pos), schema) for reader_pos, schema in enumerate(reader_schema)]
    writer_node_schema = [(Node("writer", writer_pos), schema) for writer_pos, schema in enumerate(writer_schema)]

    compatible_edges = []
    top_nodes = set()
    for (reader_node, reader_subschema), (writer_node, writer_subschema) in product(reader_node_schema, writer_node_schema):
        rec_result = compatibility_rec(
            reader_subschema, writer_subschema, location + [reader_type.value, str(writer_node.pos)]
        )

        if is_compatible(rec_result):
            top_nodes.add(reader_node)
            compatible_edges.append((reader_node, writer_node))

    compatibility_graph = nx.Graph(compatible_edges)
    matching = nx.algorithms.bipartite.maximum_matching(compatibility_graph, top_nodes)

    # Dividing by two because the result has the arrows for both directions
    assert len(matching) % 2 == 0, "the length of the matching must be even"
    return len(matching) // 2


def incompatible_schema(
    incompat_type: SchemaIncompatibilityType, message: str, location: List[str]
) -> SchemaCompatibilityResult:
    locations = "/".join(location)
    if len(location) > 1:  # Remove ROOT_REFERENCE_TOKEN
        locations = locations[1:]
    return SchemaCompatibilityResult(
        compatibility=SchemaCompatibilityType.incompatible,
        incompatibilities=[incompat_type],
        locations={locations},
        messages={message},
    )


def is_incompatible(result: "SchemaCompatibilityResult") -> bool:
    return result.compatibility is SchemaCompatibilityType.incompatible


def is_compatible(result: "SchemaCompatibilityResult") -> bool:
    return result.compatibility is SchemaCompatibilityType.compatible


def compatibility(reader: Draft7Validator, writer: Draft7Validator) -> SchemaCompatibilityResult:
    """Checks that `reader` can read values produced by `writer`."""
    assert reader is not None
    assert writer is not None

    # Normalize the schema before validating it. This will mostly resolve
    # schema references, which is done by jsonschema at runtime.
    reader_schema = normalize_schema(reader)
    writer_schema = normalize_schema(writer)

    return compatibility_rec(reader_schema, writer_schema, [])


def check_simple_subschema(
    simplified_reader_schema: Any,
    simplified_writer_schema: Any,
    original_reader_type: JSONSCHEMA_TYPES,
    original_writer_type: JSONSCHEMA_TYPES,
    location: List[str],
) -> SchemaCompatibilityResult:
    rec_result = compatibility_rec(simplified_reader_schema, simplified_writer_schema, location)
    if is_compatible(rec_result):
        return rec_result
    return type_mismatch(original_reader_type, original_writer_type, location)


def compatibility_rec(
    reader_schema: Optional[Any], writer_schema: Optional[Any], location: List[str]
) -> SchemaCompatibilityResult:
    if introduced_constraint(reader_schema, writer_schema):
        return incompatible_schema(
            incompat_type=Incompatibility.schema_added,
            message="schema added, previously used values may not be valid anymore",
            location=location,
        )

    # Note: This is not always an incompatible change, jsonschema accepts
    # values unless there is an explicit assertion to reject it, meaning the
    # reader_schema would have to be `false` instead of undefined. However, on
    # some code paths this is really a incompatible change, specially when the
    # reader has type `array` to represent a list, and the writer is either a
    # different type or it is also an `array` but now it representes a tuple.
    if reader_schema is None and writer_schema is not None:
        return incompatible_schema(
            incompat_type=Incompatibility.schema_removed,
            message="schema removed",
            location=location,
        )

    # The type of reader_schema and writer_schema may vary wildly. Example:
    #
    #   reader_schema = {"additionalProperties": {"type": "integer"}, ...}
    #   writer_schema = {"additionalProperties": false, ...}
    #
    # When recursing `reader_schema` will be Instance.INTEGER and
    # `writer_schema` will be BooleanSchema
    #
    reader_type = get_type_of(reader_schema)
    writer_type = get_type_of(writer_schema)

    reader_is_number = reader_type in (Instance.NUMBER, Instance.INTEGER)
    writer_is_number = writer_type in (Instance.NUMBER, Instance.INTEGER)
    both_are_numbers = reader_is_number and writer_is_number

    reader_has_subschema = reader_type in (Subschema.ALL_OF, Subschema.ANY_OF, Subschema.ONE_OF)
    writer_has_subschema = writer_type in (Subschema.ALL_OF, Subschema.ANY_OF, Subschema.ONE_OF)
    either_has_subschema = reader_has_subschema or writer_has_subschema

    reader_is_true_schema = is_true_schema(reader_schema)

    reader_is_object = reader_type == Instance.OBJECT
    reader_is_true_schema = is_true_schema(reader_schema)
    writer_is_object = writer_type == Instance.OBJECT
    writer_is_true_schema = is_true_schema(writer_schema)
    both_are_object = (reader_is_object or reader_is_true_schema) and (writer_is_object or writer_is_true_schema)

    # https://json-schema.org/draft/2020-12/json-schema-validation.html#rfc.section.6.1.1
    if not both_are_numbers and not either_has_subschema and not both_are_object and reader_type != writer_type:
        result = type_mismatch(reader_type, writer_type, location)
    elif both_are_numbers:
        result = compatibility_numerical(reader_schema, writer_schema, location)
    elif either_has_subschema:
        result = compatibility_subschemas(reader_schema, writer_schema, location)
    elif both_are_object:
        if reader_is_true_schema:
            reader_schema = {"type": Instance.OBJECT.value}
        if writer_is_true_schema:
            writer_schema = {"type": Instance.OBJECT.value}
        result = compatibility_object(reader_schema, writer_schema, location)
    elif reader_type is BooleanSchema:
        result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)
    elif reader_type is Subschema.NOT:
        assert reader_schema, "if just one schema is NOT the result should have been a type_mismatch"
        assert writer_schema, "if just one schema is NOT the result should have been a type_mismatch"

        location_not = location + [Subschema.NOT.value]
        return compatibility_rec(
            reader_schema[Subschema.NOT.value],
            writer_schema[Subschema.NOT.value],
            location_not,
        )
    elif reader_type == Instance.BOOLEAN:
        result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)
    elif reader_type == Instance.STRING:
        result = compatibility_string(reader_schema, writer_schema, location)
    elif reader_type == Instance.ARRAY:
        result = compatibility_array(reader_schema, writer_schema, location)
    elif reader_type == Keyword.ENUM:
        result = compatibility_enum(reader_schema, writer_schema, location)
    elif reader_type is Instance.NULL:
        result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)
    else:
        raise ValueError(f"unknown type {reader_type}")

    return result


def check_assertion_compatibility(
    reader_schema, writer_schema, assertion_check: AssertionCheck, location: List[str]
) -> SchemaCompatibilityResult:
    result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    reader_value = reader_schema.get(assertion_check.keyword.value)
    writer_value = writer_schema.get(assertion_check.keyword.value)

    if introduced_constraint(reader_value, writer_value):
        add_incompatibility(
            result,
            incompat_type=assertion_check.error_when_introducing,
            message=assertion_check.error_msg_when_introducing.format(
                assert_name=assertion_check.keyword.value, introduced_value=writer_value
            ),
            location=location,
        )

    # The type error below is due to a mypy bug for version 0.820 (issue #10131)
    if assertion_check.comparison(reader_value, writer_value):  # type: ignore[call-arg]
        add_incompatibility(
            result,
            incompat_type=assertion_check.error_when_restricting,
            message=assertion_check.error_msg_when_restricting.format(
                assert_name=assertion_check.keyword.value, reader_value=reader_value, writer_value=writer_value
            ),
            location=location,
        )

    return result


def compatibility_enum(reader_schema, writer_schema, location: List[str]) -> SchemaCompatibilityResult:
    # https://json-schema.org/draft/2020-12/json-schema-validation.html#rfc.section.6.1.2
    assert Keyword.ENUM.value in reader_schema, "types should have been previously checked"
    assert Keyword.ENUM.value in writer_schema, "types should have been previously checked"

    options_removed_by_reader = set(writer_schema[Keyword.ENUM.value]) - set(reader_schema[Keyword.ENUM.value])
    if options_removed_by_reader:
        options = ", ".join(options_removed_by_reader)
        return incompatible_schema(
            Incompatibility.enum_array_narrowed,
            message=f"some of enum options are no longer valid {options}",
            location=location + [Keyword.ENUM.value],
        )

    return SchemaCompatibilityResult(SchemaCompatibilityType.compatible)


def compatibility_numerical(reader_schema, writer_schema, location: List[str]) -> SchemaCompatibilityResult:
    # https://json-schema.org/draft/2020-12/json-schema-validation.html#rfc.section.6.2
    result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    reader_type = get_type_of(reader_schema)
    writer_type = get_type_of(writer_schema)

    reader_is_number = reader_type in (Instance.NUMBER, Instance.INTEGER)
    writer_is_number = writer_type in (Instance.NUMBER, Instance.INTEGER)
    assert reader_is_number, "types should have been previously checked"
    assert writer_is_number, "types should have been previously checked"

    checks: List[AssertionCheck] = [MAXIMUM_CHECK, MINIMUM_CHECK, EXCLUSIVE_MAXIMUM_CHECK, EXCLUSIVE_MINIMUM_CHECK]
    for assertion_check in checks:
        check_result = check_assertion_compatibility(
            reader_schema,
            writer_schema,
            assertion_check,
            location,
        )
        result = merge(result, check_result)

    reader_multiple = reader_schema.get(Keyword.MULTIPLE.value)
    writer_multiple = writer_schema.get(Keyword.MULTIPLE.value)

    if introduced_constraint(reader_multiple, writer_multiple):
        add_incompatibility(
            result,
            incompat_type=Incompatibility.multiple_added,
            message=INTRODUCED_INCOMPATIBILITY_MSG_FMT.format(
                assert_name=Keyword.MULTIPLE.value, introduced_value=writer_multiple
            ),
            location=location,
        )

    if ne(reader_multiple, writer_multiple):
        if reader_multiple > writer_multiple:
            message_expanded = f"Multiple must not increase ({reader_multiple} > {writer_multiple})"
            add_incompatibility(
                result, incompat_type=Incompatibility.multiple_expanded, message=message_expanded, location=location
            )

        elif writer_multiple % reader_multiple != 0:
            message_changed = (
                f"{reader_multiple} must be an integer multiple of "
                f"{writer_multiple} ({writer_multiple} % {reader_multiple} = 0)"
            )
            add_incompatibility(
                result, incompat_type=Incompatibility.multiple_changed, message=message_changed, location=location
            )

    if reader_type == Instance.INTEGER and writer_type == Instance.NUMBER:
        message_narrowed = "Writer produces numbers while reader only accepted integers"
        add_incompatibility(result, incompat_type=Incompatibility.type_narrowed, message=message_narrowed, location=location)

    return result


def compatibility_string(reader_schema, writer_schema, location: List[str]) -> SchemaCompatibilityResult:
    # https://json-schema.org/draft/2020-12/json-schema-validation.html#rfc.section.6.3
    result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    assert get_type_of(reader_schema) == Instance.STRING, "types should have been previously checked"
    assert get_type_of(writer_schema) == Instance.STRING, "types should have been previously checked"

    checks: List[AssertionCheck] = [MAX_LENGTH_CHECK, MIN_LENGTH_CHECK, PATTERN_CHECK]
    for assertion_check in checks:
        check_result = check_assertion_compatibility(
            reader_schema,
            writer_schema,
            assertion_check,
            location,
        )
        result = merge(result, check_result)
    return result


def compatibility_array(reader_schema, writer_schema, location: List[str]) -> SchemaCompatibilityResult:
    # https://json-schema.org/draft/2020-12/json-schema-validation.html#rfc.section.6.4
    reader_type = get_type_of(reader_schema)
    writer_type = get_type_of(writer_schema)
    assert reader_type == Instance.ARRAY, "types should have been previously checked"
    assert writer_type == Instance.ARRAY, "types should have been previously checked"

    reader_items = reader_schema.get(Keyword.ITEMS.value)
    writer_items = writer_schema.get(Keyword.ITEMS.value)

    result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    reader_is_tuple = is_tuple(reader_schema)
    writer_is_tuple = is_tuple(writer_schema)

    if reader_is_tuple != writer_is_tuple:
        return type_mismatch(reader_type, writer_type, location)

    # Extend the array iterator to match the tuple size
    if reader_is_tuple and writer_is_tuple:
        reader_items_iter = iter(reader_items)
        writer_items_iter = iter(writer_items)
        reader_requires_more_items = len(reader_items) > len(writer_items)
        writer_has_more_items = len(writer_items) > len(reader_items)
    else:
        reader_items_iter = iter([reader_items])
        writer_items_iter = iter([writer_items])
        reader_requires_more_items = False
        writer_has_more_items = False

    pos = 0
    for pos, (reader_item, writer_item) in enumerate(zip(reader_items_iter, writer_items_iter), start=pos):
        rec_result = compatibility_rec(reader_item, writer_item, location + ["items", f"{pos}"])
        if is_incompatible(rec_result):
            result = merge(result, rec_result)

    reader_additional_items = reader_schema.get(Keyword.ADDITIONAL_ITEMS.value, True)
    reader_restricts_additional_items = not is_true_schema(reader_additional_items)
    location_additional_items = location + [Keyword.ADDITIONAL_ITEMS.value]

    if writer_has_more_items and reader_restricts_additional_items:
        reader_rejects_additional_items = is_false_schema(reader_restricts_additional_items)
        if reader_rejects_additional_items:
            add_incompatibility(
                result,
                incompat_type=Incompatibility.item_removed_from_closed_content_model,
                message=f"Elements starting from index {pos} are not allowed",
                location=location + [Keyword.ADDITIONAL_ITEMS.value],
            )
        else:
            for pos, writer_item in enumerate(writer_items_iter, start=pos):
                rec_result = compatibility_rec(reader_restricts_additional_items, writer_item, location_additional_items)
                if is_incompatible(rec_result):
                    add_incompatibility(
                        result,
                        incompat_type=Incompatibility.item_removed_not_covered_by_partially_open_content_model,
                        message=f"Item in position {pos} is not compatible",
                        location=location_additional_items,
                    )

    writer_additional_items = writer_schema.get(Keyword.ADDITIONAL_ITEMS.value, True)
    writer_restricts_additional_items = not is_true_schema(writer_additional_items)

    if reader_requires_more_items:
        # This is just for more detailed diagnostics
        if writer_restricts_additional_items:
            for pos, reader_item in enumerate(reader_items_iter, start=pos):
                location_reader_item = location + ["items", f"{pos}"]
                rec_result = compatibility_rec(reader_item, writer_additional_items, location_reader_item)
                if is_incompatible(rec_result):
                    add_incompatibility(
                        result,
                        incompat_type=Incompatibility.item_added_not_covered_by_partially_open_content_model,
                        message="New element schema incompatible with the other version",
                        location=location_reader_item,
                    )

        add_incompatibility(
            result,
            incompat_type=Incompatibility.item_added_to_open_content_model,
            message=f"Elements starting from index {pos} are now required",
            location=location,
        )

    if is_tuple_without_additional_items(reader_schema) and not is_tuple_without_additional_items(writer_schema):
        add_incompatibility(
            result,
            incompat_type=Incompatibility.additional_items_removed,
            message="Additional items are not longer allowed",
            location=location_additional_items,
        )

    reader_additional_items = reader_schema.get(Keyword.ITEMS)
    writer_additional_items = writer_schema.get(Keyword.ITEMS)
    if introduced_constraint(reader_additional_items, writer_additional_items):
        add_incompatibility(
            result,
            incompat_type=Incompatibility.additional_items_removed,
            message="Items are now restricted, old values may not be valid anymore",
            location=location_additional_items,
        )

    rec_result = compatibility_rec(reader_additional_items, writer_additional_items, location_additional_items)
    result = merge(result, rec_result)

    checks: List[AssertionCheck] = [MAX_ITEMS_CHECK, MIN_ITEMS_CHECK]
    for assertion_check in checks:
        check_result = check_assertion_compatibility(
            reader_schema,
            writer_schema,
            assertion_check,
            location,
        )
        result = merge(result, check_result)

    reader_unique_items = reader_schema.get(Keyword.UNIQUE_ITEMS)
    writer_unique_items = reader_schema.get(Keyword.UNIQUE_ITEMS)

    if introduced_constraint(reader_unique_items, writer_unique_items):
        add_incompatibility(
            result,
            incompat_type=Incompatibility.unique_items_added,
            message=INTRODUCED_INCOMPATIBILITY_MSG_FMT.format(
                assert_name=Keyword.UNIQUE_ITEMS.value,
                introduced_value=writer_unique_items,
            ),
            location=location,
        )

    return result


def add_incompatibility(
    result: SchemaCompatibilityResult, incompat_type: SchemaIncompatibilityType, message: str, location: List[str]
) -> None:
    """Add an incompatibility, this will modify the object in-place."""
    formatted_location = "/".join(location[1:] if len(location) > 1 else location)

    result.compatibility = SchemaCompatibilityType.incompatible
    result.incompatibilities.append(incompat_type)
    result.messages.add(message)
    result.locations.add(formatted_location)


def compatibility_object(reader_schema, writer_schema, location: List[str]) -> SchemaCompatibilityResult:
    # https://json-schema.org/draft/2020-12/json-schema-validation.html#rfc.section.6.5
    result = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    assert get_type_of(reader_schema) == Instance.OBJECT, "types should have been previously checked"
    assert get_type_of(writer_schema) == Instance.OBJECT, "types should have been previously checked"

    properties_location = location + [Keyword.PROPERTIES.value]
    reader_properties = reader_schema.get(Keyword.PROPERTIES.value)
    writer_properties = writer_schema.get(Keyword.PROPERTIES.value)

    reader_property_set = set(reader_properties) if reader_properties else set()
    writer_property_set = set(writer_properties) if writer_properties else set()

    # These properties are unknown in the sense they don't have a direct
    # schema, however there may be an indirect schema (patternProperties or
    # additionalProperties)
    properties_unknown_to_writer = reader_property_set - writer_property_set
    properties_unknown_to_reader = writer_property_set - reader_property_set

    for common_property in reader_property_set & writer_property_set:
        this_property_location = properties_location + [common_property]

        reader_property = reader_properties[common_property]
        writer_property = writer_properties[common_property]

        is_required_by_reader = reader_property.get(Keyword.REQUIRED.value)
        is_required_by_writer = writer_property.get(Keyword.REQUIRED.value)
        if not is_required_by_writer and is_required_by_reader:
            add_incompatibility(
                result,
                incompat_type=Incompatibility.required_attribute_added,
                message=f"Property {common_property} became required",
                location=this_property_location,
            )

        rec_result = compatibility_rec(
            reader_schema=reader_property,
            writer_schema=writer_property,
            location=this_property_location,
        )
        if is_incompatible(rec_result):
            result = merge(result, rec_result)

    # With an open content model any property can be added without breaking
    # compatibility because those do not have assertions, so only check if the
    # reader is using a closed model
    if properties_unknown_to_reader and not is_object_content_model_open(reader_schema):
        for unknown_property_to_reader in properties_unknown_to_reader:
            schema_for_property = schema_from_partially_open_content_model(reader_schema, unknown_property_to_reader)

            if schema_for_property is None:
                add_incompatibility(
                    result,
                    incompat_type=Incompatibility.property_removed_from_closed_content_model,
                    message=f"The property {unknown_property_to_reader} is not accepted anymore",
                    location=properties_location,
                )
            else:
                rec_result = compatibility_rec(
                    reader_schema=schema_for_property,
                    writer_schema=writer_properties[unknown_property_to_reader],
                    location=properties_location,
                )
                if is_incompatible(rec_result):
                    result = merge(result, rec_result)
                    add_incompatibility(
                        result,
                        incompat_type=Incompatibility.property_removed_not_covered_by_partially_open_content_model,
                        message=f"property {unknown_property_to_reader} is not compatible",
                        location=properties_location,
                    )

    elif properties_unknown_to_writer:
        is_writer_open_model = is_object_content_model_open(writer_schema)

        if is_writer_open_model:
            properties = ", ".join(properties_unknown_to_writer)
            message_property_added_to_open_content_model = (
                f"Restricting acceptable values of properties is an incompatible "
                f"change. The following properties {properties} accepted any "
                f"value because of the lack of validation (the object schema had "
                f"neither patternProperties nor additionalProperties), now "
                f"these values are restricted."
            )
            add_incompatibility(
                result,
                incompat_type=Incompatibility.property_added_to_open_content_model,
                message=message_property_added_to_open_content_model,
                location=properties_location,
            )

        if not is_writer_open_model:
            for unknown_property_to_writer in properties_unknown_to_writer:
                schema_for_property = schema_from_partially_open_content_model(writer_schema, unknown_property_to_writer)

                schema_for_property_exists = schema_for_property is not None
                schema_allows_writes = not is_false_schema(schema_for_property)

                if schema_for_property_exists and schema_allows_writes:
                    rec_result = compatibility_rec(
                        reader_schema=reader_properties[unknown_property_to_writer],
                        writer_schema=schema_for_property,
                        location=properties_location,
                    )
                    if is_incompatible(rec_result):
                        add_incompatibility(
                            result,
                            incompat_type=Incompatibility.property_added_not_covered_by_partially_open_content_model,
                            message="incompatible schemas",
                            location=properties_location,
                        )

                new_property_is_required_without_default = unknown_property_to_writer in reader_schema.get(
                    Keyword.REQUIRED.value,
                    [],
                ) and Keyword.DEFAULT.value not in reader_properties.get(
                    Keyword.REQUIRED.value,
                    [],
                )
                if new_property_is_required_without_default:
                    add_incompatibility(
                        result,
                        incompat_type=Incompatibility.required_property_added_to_unopen_content_model,
                        message=f"Property {unknown_property_to_writer} added without a default",
                        location=properties_location,
                    )

    reader_attribute_dependencies_schema = reader_schema.get(Keyword.DEPENDENCIES.value, {})
    writer_attribute_dependencies_schema = writer_schema.get(Keyword.DEPENDENCIES.value, {})

    for writer_attribute_dependency_name, writer_attribute_dependencies in writer_attribute_dependencies_schema.items():
        reader_attribute_dependencies = reader_attribute_dependencies_schema.get(writer_attribute_dependency_name)

        if not reader_attribute_dependencies:
            add_incompatibility(
                result,
                incompat_type=Incompatibility.dependency_array_added,
                message="incompatible dependency array",
                location=location,
            )

        new_dependencies = set(writer_attribute_dependencies) - set(reader_attribute_dependencies)
        if new_dependencies:
            add_incompatibility(
                result,
                incompat_type=Incompatibility.dependency_array_extended,
                message=f"new dependencies {new_dependencies}",
                location=location,
            )

    reader_dependent_schemas = reader_schema.get(Keyword.DEPENDENT_SCHEMAS.value, {})
    writer_dependent_schemas = writer_schema.get(Keyword.DEPENDENT_SCHEMAS.value, {})

    for writer_dependent_schema_name, writer_dependent_schema in writer_dependent_schemas.items():
        reader_dependent_schema = reader_dependent_schemas.get(writer_dependent_schema_name)
        if introduced_constraint(reader_dependent_schema, writer_dependent_schemas):
            add_incompatibility(
                result,
                incompat_type=Incompatibility.dependency_schema_added,
                message=f"new dependency schema {writer_dependent_schema_name}",
                location=location,
            )

        rec_result = compatibility_rec(reader_dependent_schema, writer_dependent_schema, location)
        result = merge(result, rec_result)

    checks: List[AssertionCheck] = [MAX_PROPERTIES_CHECK, MIN_PROPERTIES_CHECK]
    for assertion_check in checks:
        check_result = check_assertion_compatibility(
            reader_schema,
            writer_schema,
            assertion_check,
            location,
        )
        result = merge(result, check_result)

    reader_additional_properties = reader_schema.get(Keyword.ADDITIONAL_PROPERTIES.value)
    writer_additional_properties = writer_schema.get(Keyword.ADDITIONAL_PROPERTIES.value)
    location_additional_properties = location + [Keyword.ADDITIONAL_PROPERTIES.value]

    if introduced_constraint(reader_additional_properties, writer_additional_properties):
        add_incompatibility(
            result,
            incompat_type=Incompatibility.additional_properties_narrowed,
            message="additionalProperties instroduced",
            location=location_additional_properties,
        )

    if reader_additional_properties and writer_additional_properties:
        rec_result = compatibility_rec(
            reader_additional_properties, writer_additional_properties, location_additional_properties
        )
        result = merge(result, rec_result)

    return result


def compatibility_subschemas(reader_schema, writer_schema, location: List[str]) -> SchemaCompatibilityResult:
    # https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.10
    # pylint: disable=too-many-return-statements
    reader_subschemas_and_type = maybe_get_subschemas_and_type(reader_schema)
    writer_subschemas_and_type = maybe_get_subschemas_and_type(writer_schema)

    reader_subschemas: Optional[List[Any]]
    reader_type: JSONSCHEMA_TYPES
    if reader_subschemas_and_type is not None:
        reader_subschemas = reader_subschemas_and_type[0]
        reader_type = reader_subschemas_and_type[1]
        reader_has_subschema = reader_type in (Subschema.ALL_OF, Subschema.ANY_OF, Subschema.ONE_OF)
    else:
        reader_subschemas = None
        reader_type = get_type_of(reader_schema)
        reader_has_subschema = False

    writer_subschemas: Optional[List[Any]]
    writer_type: JSONSCHEMA_TYPES
    if writer_subschemas_and_type is not None:
        writer_subschemas = writer_subschemas_and_type[0]
        writer_type = writer_subschemas_and_type[1]
        writer_has_subschema = writer_type in (Subschema.ALL_OF, Subschema.ANY_OF, Subschema.ONE_OF)
    else:
        writer_subschemas = None
        writer_type = get_type_of(writer_schema)
        writer_has_subschema = False

    is_reader_special_case = reader_has_subschema and not writer_has_subschema and is_simple_subschema(reader_schema)
    is_writer_special_case = not reader_has_subschema and writer_has_subschema and is_simple_subschema(writer_schema)

    subschema_location = location + [get_name_of(reader_type)]

    if is_reader_special_case:
        assert reader_subschemas
        return check_simple_subschema(reader_subschemas[0], writer_schema, reader_type, writer_type, subschema_location)

    if is_writer_special_case:
        assert writer_subschemas
        return check_simple_subschema(reader_schema, writer_subschemas[0], reader_type, writer_type, subschema_location)

    if reader_type in (Subschema.ANY_OF, Subschema.ONE_OF) and not writer_has_subschema:
        assert isinstance(reader_type, Subschema)
        for reader_subschema in reader_schema[reader_type.value]:
            rec_result = compatibility_rec(reader_subschema, writer_schema, subschema_location)
            if is_compatible(rec_result):
                return rec_result
        return type_mismatch(reader_type, writer_type, subschema_location)

    if reader_subschemas is not None and writer_subschemas is not None:
        if reader_type not in (Subschema.ANY_OF, writer_type):
            return incompatible_schema(
                Incompatibility.combined_type_changed,
                message=f"incompatible subschema change, from {reader_type} to {writer_type}",
                location=subschema_location,
            )

        len_reader_subschemas = len(reader_subschemas)
        len_writer_subschemas = len(writer_subschemas)

        if reader_type == Subschema.ALL_OF and len_writer_subschemas < len_reader_subschemas:
            msg = (
                f"Not all required schemas were provided, number of required "
                f"schemas increased from {len_writer_subschemas} to "
                f"{len_reader_subschemas}"
            )
            return incompatible_schema(
                Incompatibility.product_type_extended,
                message=msg,
                location=subschema_location,
            )

        # oneOf/anyOf differ on annotation collection not validation.
        if reader_type in (Subschema.ANY_OF, Subschema.ONE_OF) and len_writer_subschemas > len_reader_subschemas:
            msg = (
                f"Not all schemas are accepted, number of schemas "
                f"reduced from {len_writer_subschemas} to "
                f"{len_reader_subschemas}"
            )
            return incompatible_schema(
                Incompatibility.sum_type_narrowed,
                message=msg,
                location=subschema_location,
            )

        if reader_type == Subschema.ALL_OF:
            qty_of_required_compatible_subschemas = len_reader_subschemas
        else:
            qty_of_required_compatible_subschemas = len_writer_subschemas

        compatible_schemas_count = count_uniquely_compatible_schemas(
            reader_type,  # type: ignore
            reader_subschemas,
            writer_subschemas,
            subschema_location,
        )
        if compatible_schemas_count < qty_of_required_compatible_subschemas:
            return incompatible_schema(
                Incompatibility.combined_type_subschemas_changed,
                message="subschemas are incompatible",
                location=subschema_location,
            )
        return SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    return type_mismatch(reader_type, writer_type, subschema_location)
