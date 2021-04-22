# TODO: PROTOBUF* this functionality must be implemented
from karapace.avro_compatibility import SchemaCompatibilityResult


def check_protobuf_schema_compatibility(reader: str, writer: str) -> SchemaCompatibilityResult:
    # TODO: PROTOBUF* for investigation purposes yet

    if writer != reader:
        return SchemaCompatibilityResult.compatible()

    return SchemaCompatibilityResult.compatible()
