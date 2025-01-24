"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from avro.compatibility import SchemaCompatibilityType
from karapace.core.compatibility import CompatibilityModes
from karapace.core.compatibility.schema_compatibility import SchemaCompatibility
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema

import json


def test_schema_type_can_change_when_mode_none() -> None:
    avro_str = json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "field1", "type": "int"}]})
    json_str = '{"type": "array"}'
    avro_schema = ValidatedTypedSchema.parse(SchemaType.AVRO, avro_str)
    json_schema = ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, json_str)

    result = SchemaCompatibility.check_compatibility(
        old_schema=avro_schema, new_schema=json_schema, compatibility_mode=CompatibilityModes.NONE
    )
    assert result.compatibility is SchemaCompatibilityType.compatible


def test_schema_compatible_in_transitive_mode() -> None:
    old_json = '{"type": "array", "name": "name_old"}'
    new_json = '{"type": "array", "name": "name_new"}'
    old_schema = ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, old_json)
    new_schema = ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, new_json)

    result = SchemaCompatibility.check_compatibility(
        old_schema=old_schema, new_schema=new_schema, compatibility_mode=CompatibilityModes.FULL_TRANSITIVE
    )
    assert result.compatibility is SchemaCompatibilityType.compatible


def test_schema_incompatible_in_transitive_mode() -> None:
    old_json = '{"type": "array"}'
    new_json = '{"type": "integer"}'
    old_schema = ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, old_json)
    new_schema = ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, new_json)

    result = SchemaCompatibility.check_compatibility(
        old_schema=old_schema, new_schema=new_schema, compatibility_mode=CompatibilityModes.FULL_TRANSITIVE
    )
    assert result.compatibility is SchemaCompatibilityType.incompatible
