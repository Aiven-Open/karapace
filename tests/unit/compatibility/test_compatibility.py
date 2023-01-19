"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from avro.compatibility import SchemaCompatibilityType
from karapace.compatibility import check_compatibility, CompatibilityModes
from karapace.schema_models import SchemaType, ValidatedTypedSchema

import json


def test_schema_type_can_change_when_mode_none():
    avro_str = json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "field1", "type": "int"}]})
    json_str = '{"type":"array"}'
    avro_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, avro_str)
    json_schema = ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, json_str)

    result = check_compatibility(old_schema=avro_schema, new_schema=json_schema, compatibility_mode=CompatibilityModes.NONE)
    assert result.compatibility is SchemaCompatibilityType.compatible
