# TODO: PROTOBUF* this functionality must be implemented
from karapace.avro_compatibility import SchemaCompatibilityResult, SchemaIncompatibilityType
from karapace.protobuf.proto_file_element import ProtoFileElement
from karapace.schema_reader import SchemaType, TypedSchema


def check_protobuf_schema_compatibility(reader: str, writer: str) -> SchemaCompatibilityResult:

    reader_proto_file_element: ProtoFileElement = TypedSchema.parse(SchemaType.PROTOBUF, reader)
    writer_proto_file_element: ProtoFileElement = TypedSchema.parse(SchemaType.PROTOBUF, writer)

    if writer_proto_file_element.compatible(reader_proto_file_element):
        return SchemaCompatibilityResult.compatible()
    #TODO: move incompatibility level raising to ProtoFileElement.compatible()
    return SchemaCompatibilityResult.incompatible(
        incompat_type=SchemaIncompatibilityType.name_mismatch, message=f" missed ", location=[]
    )
