# TODO: PROTOBUF* this functionality must be implemented
from karapace.avro_compatibility import SchemaCompatibilityResult


def parse_protobuf_schema_definition(schema_definition: str) -> str:
    """ Parses and validates `schema_definition`.

    Raises:
        Nothing yet.

    """

    return schema_definition


def check_protobuf_schema_compatibility(reader: str, writer: str) -> SchemaCompatibilityResult:
    # TODO: PROTOBUF* for investigation purposes yet

    if writer != reader:
        return SchemaCompatibilityResult.compatible()

    return SchemaCompatibilityResult.compatible()
