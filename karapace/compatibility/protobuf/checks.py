"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from avro.compatibility import SchemaCompatibilityResult, SchemaCompatibilityType
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.schema import ProtobufSchema


def check_protobuf_schema_compatibility(reader: ProtobufSchema, writer: ProtobufSchema) -> SchemaCompatibilityResult:
    result = CompareResult()
    writer.compare(reader, result)
    if result.is_compatible():
        return SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

    incompatibilities = []
    locations = set()
    messages = set()
    for record in result.result:
        if not record.modification.is_compatible():
            incompatibilities.append(str(record.modification))
            locations.add(record.path)
            messages.add(record.message)

    return SchemaCompatibilityResult(
        compatibility=SchemaCompatibilityType.incompatible,
        incompatibilities=list(incompatibilities),
        locations=set(locations),
        messages=set(messages),
    )
