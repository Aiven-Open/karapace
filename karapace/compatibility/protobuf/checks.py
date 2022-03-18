from avro.compatibility import SchemaCompatibilityResult, SchemaCompatibilityType
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.schema import ProtobufSchema

import logging

log = logging.getLogger(__name__)


def check_protobuf_schema_compatibility(reader: ProtobufSchema, writer: ProtobufSchema) -> SchemaCompatibilityResult:
    result = CompareResult()
    log.debug("READER: %s", reader.to_schema())
    log.debug("WRITER: %s", writer.to_schema())
    writer.compare(reader, result)
    log.debug("IS_COMPATIBLE %s", result.is_compatible())
    if result.is_compatible():
        return SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    incompatibilities = []
    locations = set()
    messages = set()
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
