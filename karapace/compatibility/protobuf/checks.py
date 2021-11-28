# TODO: PROTOBUF* this functionality must be implemented
from karapace.avro_compatibility import SchemaCompatibilityResult, SchemaCompatibilityType
from karapace.protobuf.compare_result import CompareResult, ModificationRecord
from karapace.protobuf.schema import ProtobufSchema

import logging

log = logging.getLogger(__name__)


def check_protobuf_schema_compatibility(reader: ProtobufSchema, writer: ProtobufSchema) -> SchemaCompatibilityResult:
    result: CompareResult = CompareResult()
    log.debug("READER: %s", reader.to_schema())
    log.debug("WRITER: %s", writer.to_schema())
    writer.compare(reader, result)
    log.debug("IS_COMPATIBLE %s", result.is_compatible())
    if result.is_compatible():
        return SchemaCompatibilityResult.compatible()
    # TODO: maybe move incompatibility level raising to ProtoFileElement.compatible() ??

    incompatibilities = list()
    record: ModificationRecord
    locations: set = set()
    messages: set = set()
    for record in result.result:
        if not record.modification.is_compatible():
            incompatibilities.append(record.modification.__str__())
            locations.add(record.path)
            messages.add(record.message)

    return SchemaCompatibilityResult(
        compatibility=SchemaCompatibilityType.incompatible,
        incompatibilities=list(incompatibilities),
        locations=set(locations),
        messages=set(messages),
    )
