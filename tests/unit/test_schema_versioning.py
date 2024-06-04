"""
karapace - Test schema models

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from avro.schema import Schema as AvroSchema
from karapace.errors import InvalidVersion, VersionNotFoundException
from karapace.schema_models import parse_avro_schema_definition, SchemaVersion, TypedSchema
from karapace.schema_type import SchemaType
from karapace.schema_versioning import Version, VersionTag
from typing import Any, Callable, Dict, Optional

import operator
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
    ) -> Callable[[Version, Dict[str, Any]], Dict[Version, SchemaVersion]]:
        def schema_versions(version: Version, schema_version_data: Optional[Dict[str, Any]] = None):
            schema_version_data = schema_version_data or dict()
            base_schema_version_data = dict(
                subject="test-topic",
                version=version,
                deleted=False,
                schema_id=1,
                schema=TypedSchema(
                    schema_type=SchemaType.AVRO,
                    schema_str=avro_schema,
                    schema=avro_schema_parsed,
                ),
                references=None,
            )
            return {version: SchemaVersion(**{**base_schema_version_data, **schema_version_data})}

        return schema_versions

    def test_version(self, version: Version):
        assert version == 1
        assert isinstance(version, Version)
        assert issubclass(Version, int)

    def test_tags(self, version: Version):
        assert version.LATEST_VERSION_TAG == "latest"
        assert version.MINUS_1_VERSION_TAG == -1

    @pytest.mark.parametrize("invalid_version", ["string", -10, 0])
    def test_invalid_version(self, invalid_version: VersionTag):
        with pytest.raises(InvalidVersion):
            Version(invalid_version)

    @pytest.mark.parametrize(
        "version, is_latest",
        [(Version(-1), True), (Version(1), False)],
    )
    def test_is_latest(self, version: Version, is_latest: bool):
        assert version.is_latest is is_latest

    @pytest.mark.parametrize("tag, resolved", [("latest", -1), (10, 10), ("20", 20)])
    def test_resolve_tag(self, tag: VersionTag, resolved: int):
        assert Version.resolve_tag(tag=tag) == resolved

    def test_text_formating(self, version: Version):
        assert f"{version}" == "1"
        assert f"{version!r}" == "Version=1"

    @pytest.mark.parametrize(
        "version, to_compare, comparer, valid",
        [
            (Version(1), Version(1), operator.eq, True),
            (Version(1), Version(2), operator.eq, False),
            (Version(2), Version(1), operator.gt, True),
            (Version(2), Version(1), operator.lt, False),
            (Version(2), Version(2), operator.ge, True),
            (Version(2), Version(1), operator.ge, True),
            (Version(1), Version(1), operator.le, True),
            (Version(1), Version(2), operator.le, True),
        ],
    )
    def test_comparisons(
        self,
        version: Version,
        to_compare: Version,
        comparer: Callable[[Version, Version], bool],
        valid: bool,
    ):
        assert comparer(version, to_compare) is valid

    @pytest.mark.parametrize(
        "version, resolved_version",
        [
            (Version(-1), Version(10)),
            (Version(1), Version(1)),
            (Version(10), Version(10)),
        ],
    )
    def test_from_schema_versions(
        self,
        version: Version,
        resolved_version: Version,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(Version(1)))
        schema_versions.update(schema_versions_factory(Version(2)))
        schema_versions.update(schema_versions_factory(Version(10)))
        assert version.from_schema_versions(schema_versions) == resolved_version

    @pytest.mark.parametrize("nonexisting_version", [Version(100), Version(2000)])
    def test_from_schema_versions_nonexisting(
        self,
        nonexisting_version: Version,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(Version(1)))
        with pytest.raises(VersionNotFoundException):
            nonexisting_version.from_schema_versions(schema_versions)

    @pytest.mark.parametrize("tag, resolved", [("latest", -1), (10, 10), ("20", 20), (-1, -1), ("-1", -1)])
    def test_factory_V(self, tag: VersionTag, resolved: int):
        version = Version.V(tag=tag)
        assert version == resolved
        assert isinstance(version, Version)

    @pytest.mark.parametrize("tag", ["latest", 10, -1, "-1"])
    def test_validate(self, tag: VersionTag, version: Version):
        version.validate_tag(tag=tag)

    @pytest.mark.parametrize("tag", ["invalid_version", 0, -20, "0"])
    def test_validate_invalid(self, tag: VersionTag, version: Version):
        with pytest.raises(InvalidVersion):
            version.validate_tag(tag=tag)
