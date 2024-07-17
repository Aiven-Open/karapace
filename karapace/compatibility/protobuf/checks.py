"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from avro.compatibility import SchemaCompatibilityResult, SchemaCompatibilityType
from karapace.protobuf.compare_result import CompareResult
from karapace.protobuf.protopace import check_compatibility, IncompatibleError
from karapace.protobuf.schema import ProtobufSchema


def check_protobuf_schema_compatibility(
    reader: ProtobufSchema, writer: ProtobufSchema, use_protopace: bool = False
) -> SchemaCompatibilityResult:
    if use_protopace:
        old_proto = writer.to_proto()
        new_proto = reader.to_proto()
        try:
            check_compatibility(new_proto, old_proto)
        except IncompatibleError as err:
            return SchemaCompatibilityResult(
                compatibility=SchemaCompatibilityType.incompatible,
                messages={str(err)},
            )
        return SchemaCompatibilityResult(SchemaCompatibilityType.compatible)

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
        # TODO: https://github.com/aiven/karapace/issues/633
        incompatibilities=incompatibilities,  # type: ignore[arg-type]
        locations=set(locations),
        messages=set(messages),
    )
