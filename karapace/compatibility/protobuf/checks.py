# TODO: PROTOBUF* this functionality must be implemented
from karapace.avro_compatibility import SchemaCompatibilityResult, SchemaCompatibilityType
from karapace.protobuf.compare_result import CompareResult, ModificationRecord
from karapace.schema_reader import SchemaType, TypedSchema


def check_protobuf_schema_compatibility(reader: str, writer: str) -> SchemaCompatibilityResult:
    reader_proto_file_element: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, reader).schema
    writer_proto_file_element: TypedSchema = TypedSchema.parse(SchemaType.PROTOBUF, writer).schema
    result: CompareResult = CompareResult()
    writer_proto_file_element.schema.schema.compare(reader_proto_file_element.schema.schema, result)
    if result.is_compatible():
        return SchemaCompatibilityResult.compatible()
    # TODO: maybe move incompatibility level raising to ProtoFileElement.compatible() ??

    incompatibilities = list()
    record: ModificationRecord
    locations: set = set()
    messages: set = set()
    for record in result.result:
        incompatibilities.append(record.modification.__str__())
        locations.add(record.path)
        messages.add(record.message)

    return SchemaCompatibilityResult(
        compatibility=SchemaCompatibilityType.incompatible,
        incompatibilities=list(incompatibilities),
        locations=set(locations),
        messages=set(messages),
    )
