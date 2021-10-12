from karapace.protobuf.compare_restult import CompareResult, Modification
from karapace.protobuf.compare_type_storage import CompareTypes
from karapace.protobuf.field import Field
from karapace.protobuf.field_element import FieldElement
from karapace.protobuf.location import Location
from karapace.protobuf.one_of_element import OneOfElement
from karapace.protobuf.option_element import OptionElement

location: Location = Location.get("some/folder", "file.proto")


def test_compare_oneof():
    self_one_of = OneOfElement(
        name="page_info",
        fields=[
            FieldElement(location=location.at(4, 5), element_type="int32", name="page_number", tag=2),
            FieldElement(location=location.at(5, 5), element_type="int32", name="result_per_page", tag=3)
        ],
    )

    other_one_of = OneOfElement(
        name="info",
        fields=[
            FieldElement(location=location.at(4, 5), element_type="int32", name="page_number", tag=2),
            FieldElement(location=location.at(5, 5), element_type="int32", name="result_per_page", tag=3),
            FieldElement(location=location.at(6, 5), element_type="int32", name="view", tag=4)
        ],
    )

    types = CompareTypes('', '')
    result = CompareResult()
    self_one_of.compare(other_one_of, result, types)
    assert not result.is_compatible()
    assert len(result.result) == 1
    result2: list = []
    for e in result.result:
        result2.append(e.modification)
    assert Modification.ONE_OF_FIELD_ADD in result2


def test_compare_field():
    self_field = FieldElement(
        location=location.at(4, 3),
        label=Field.Label.OPTIONAL,
        element_type="bool",
        name="test",
        tag=3,
        options=[
            OptionElement("old_default", OptionElement.Kind.BOOLEAN, "true"),
            OptionElement("delay", OptionElement.Kind.NUMBER, "200", True)
        ]
    )

    other_field = FieldElement(
        location=location.at(4, 3),
        label=Field.Label.OPTIONAL,
        element_type="bool",
        name="best",
        tag=3,
        options=[
            OptionElement("old_default", OptionElement.Kind.BOOLEAN, "true"),
            OptionElement("delay", OptionElement.Kind.NUMBER, "200", True)
        ]
    )

    types = CompareTypes('', '')
    result = CompareResult()
    self_field.compare(other_field, result, types)

    assert result.is_compatible()
    assert len(result.result) == 1
    result2: list = []
    for e in result.result:
        result2.append(e.modification)

    assert Modification.FIELD_NAME_ALTER in result2
