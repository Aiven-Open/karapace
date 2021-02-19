from jsonschema import Draft7Validator

import json


def parse_schema_definition(schema_definition: str) -> Draft7Validator:
    """ Parses and validates `schema_definition`.

    Raises:
        SchemaError: If `schema_definition` is not a valid Draft7 schema.
    """
    schema = json.loads(schema_definition)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)
