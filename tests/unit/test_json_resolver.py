"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from jsonschema import RefResolver
from karapace.dependency import Dependency
from karapace.schema_models import json_resolver, parse_jsonschema_definition, ValidatedTypedSchema
from karapace.schema_type import SchemaType
from karapace.typing import Subject, Version


def test_json_registry_no_dependencies():
    """Test case when there are no dependencies."""
    schema_str = '{"$id": "http://example.com/schema.json"}'
    result = json_resolver(schema_str)
    assert result is None


def test_json_registry_with_single_dependency():
    """Test json_registry with a single dependency."""
    schema_str = '{"$id": "http://example.com/schema.json"}'
    dependency_schema = '{"$id": "http://example.com/dependency-schema.json"}'

    # Using the Dependency class from Karapace
    dependencies = {
        "dep1": Dependency(
            name="dep1",
            subject=Subject("subj"),
            version=Version(1),
            target_schema=ValidatedTypedSchema(
                schema_type=SchemaType.JSONSCHEMA,
                schema_str=dependency_schema,
                schema=parse_jsonschema_definition(dependency_schema),
            ),
        )
    }

    result = json_resolver(schema_str, dependencies)
    assert isinstance(result, RefResolver)
    assert result.store
    assert result.store.get("http://example.com/schema.json")
    assert result.store.get("http://example.com/dependency-schema.json")
