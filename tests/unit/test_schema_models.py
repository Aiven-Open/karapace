"""
karapace - Test schema models

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from avro.schema import Schema as AvroSchema
from karapace.errors import InvalidVersion, VersionNotFoundException
from karapace.schema_models import parse_avro_schema_definition, SchemaVersion, SchemaVersionManager, TypedSchema
from karapace.schema_type import SchemaType
from karapace.typing import ResolvedVersion, Version
from typing import Any, Callable, Dict, Optional

import pytest

# Schema versions factory fixture type
SVFCallable = Callable[[None], Callable[[ResolvedVersion, Dict[str, Any]], Dict[ResolvedVersion, SchemaVersion]]]


class TestSchemaVersionManager:
    @pytest.fixture
    def avro_schema(self) -> str:
        return '{"type":"record","name":"testRecord","fields":[{"type":"string","name":"test"}]}'

    @pytest.fixture
    def avro_schema_parsed(self, avro_schema: str) -> AvroSchema:
        return parse_avro_schema_definition(avro_schema)

    @pytest.fixture
    def schema_versions_factory(
        self,
        avro_schema: str,
        avro_schema_parsed: AvroSchema,
    ) -> Callable[[ResolvedVersion, Dict[str, Any]], Dict[ResolvedVersion, SchemaVersion]]:
        def schema_versions(resolved_version: int, schema_version_data: Optional[Dict[str, Any]] = None):
            schema_version_data = schema_version_data or dict()
            base_schema_version_data = dict(
                subject="test-topic",
                version=resolved_version,
                deleted=False,
                schema_id=1,
                schema=TypedSchema(
                    schema_type=SchemaType.AVRO,
                    schema_str=avro_schema,
                    schema=avro_schema_parsed,
                ),
                references=None,
            )
            return {ResolvedVersion(resolved_version): SchemaVersion(**{**base_schema_version_data, **schema_version_data})}

        return schema_versions

    def test_schema_version_manager_tags(self):
        assert SchemaVersionManager.LATEST_SCHEMA_VERSION_TAG == "latest"
        assert SchemaVersionManager.MINUS_1_SCHEMA_VERSION_TAG == "-1"

    @pytest.mark.parametrize(
        "version, is_latest",
        [("latest", True), ("-1", True), ("-20", False), (10, False)],
    )
    def test_schema_version_manager_latest_schema_tag_condition(
        self,
        version: Version,
        is_latest: bool,
    ):
        assert SchemaVersionManager.latest_schema_tag_condition(version) is is_latest

    @pytest.mark.parametrize("invalid_version", ["invalid_version", 0])
    def test_schema_version_manager_validate_version_invalid(self, invalid_version: Version):
        with pytest.raises(InvalidVersion):
            SchemaVersionManager.validate_version(invalid_version)

    @pytest.mark.parametrize(
        "version, validated_version",
        [("latest", "latest"), (-1, "latest"), ("-1", "latest"), (10, 10)],
    )
    def test_schema_version_manager_validate_version(
        self,
        version: Version,
        validated_version: Version,
    ):
        assert SchemaVersionManager.validate_version(version) == validated_version

    @pytest.mark.parametrize(
        "version, resolved_version",
        [("-1", 10), (-1, 10), (1, 1), (10, 10), ("latest", 10)],
    )
    def test_schema_version_manager_resolve_version(
        self,
        version: Version,
        resolved_version: ResolvedVersion,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(1))
        schema_versions.update(schema_versions_factory(2))
        schema_versions.update(schema_versions_factory(10))
        assert SchemaVersionManager.resolve_version(schema_versions, version) == resolved_version

    @pytest.mark.parametrize("invalid_version", ["invalid_version", 0, -20, "-10", "100", 2000])
    def test_schema_version_manager_resolve_version_invalid(
        self,
        invalid_version: Version,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(1))
        with pytest.raises(VersionNotFoundException):
            SchemaVersionManager.resolve_version(schema_versions, invalid_version)
