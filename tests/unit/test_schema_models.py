"""
karapace - Test schema models

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from avro.schema import Schema as AvroSchema
from karapace.errors import InvalidVersion, VersionNotFoundException
from karapace.schema_models import parse_avro_schema_definition, SchemaVersion, TypedSchema, Version
from karapace.schema_type import SchemaType
from typing import Any, Callable, Dict, Optional

import pytest

# Schema versions factory fixture type
SVFCallable = Callable[[None], Callable[[int, Dict[str, Any]], Dict[int, SchemaVersion]]]


class TestVersion:
    @pytest.fixture
    def version(self):
        return Version(1)

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
    ) -> Callable[[int, Dict[str, Any]], Dict[int, SchemaVersion]]:
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
            return {resolved_version: SchemaVersion(**{**base_schema_version_data, **schema_version_data})}

        return schema_versions

    def test_tags(self, version: Version):
        assert version.LATEST_VERSION_TAG == "latest"
        assert version.MINUS_1_VERSION_TAG == "-1"

    def test_text_formating(self, version: Version):
        assert f"{version}" == "1"
        assert f"{version!r}" == "Version<1>"

    @pytest.mark.parametrize(
        "version, is_latest",
        [(Version("latest"), True), (Version("-1"), True), (Version(10), False)],
    )
    def test_is_latest(self, version: Version, is_latest: bool):
        assert version.is_latest is is_latest

    @pytest.mark.parametrize("version", [Version("latest"), Version(10), Version(-1)])
    def test_validate(self, version: Version):
        assert version

    def test_validate_invalid(self):
        with pytest.raises(InvalidVersion):
            Version("invalid_version")
        with pytest.raises(InvalidVersion):
            Version(0)
        with pytest.raises(InvalidVersion):
            Version("0")
        with pytest.raises(InvalidVersion):
            Version(-20)
        with pytest.raises(InvalidVersion):
            Version("-10")

    @pytest.mark.parametrize(
        "version, resolved_version",
        [
            (Version("latest"), "latest"),
            (Version(-1), "latest"),
            (Version("-1"), "latest"),
            (Version(10), "10"),
        ],
    )
    def test_resolved(self, version: Version, resolved_version: str):
        assert version.resolved == resolved_version

    @pytest.mark.parametrize(
        "version, resolved_version",
        [
            (Version("-1"), 10),
            (Version(-1), 10),
            (Version(1), 1),
            (Version(10), 10),
            (Version("latest"), 10),
        ],
    )
    def test_resolve_from_schema_versions(
        self,
        version: Version,
        resolved_version: int,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(1))
        schema_versions.update(schema_versions_factory(2))
        schema_versions.update(schema_versions_factory(10))
        assert version.resolve_from_schema_versions(schema_versions) == resolved_version

    @pytest.mark.parametrize("nonexisting_version", [Version("100"), Version(2000)])
    def test_resolve_from_schema_versions_invalid(
        self,
        nonexisting_version: Version,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(1))
        with pytest.raises(VersionNotFoundException):
            nonexisting_version.resolve_from_schema_versions(schema_versions)
