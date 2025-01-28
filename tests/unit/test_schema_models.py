"""
karapace - Test schema models

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from avro.schema import Schema as AvroSchema
from collections.abc import Callable
from karapace.core.errors import InvalidVersion, VersionNotFoundException
from karapace.core.schema_models import parse_avro_schema_definition, SchemaVersion, TypedSchema, Versioner
from karapace.core.schema_type import SchemaType
from karapace.core.typing import Version, VersionTag
from typing import Any

import operator
import pytest

# Schema versions factory fixture type
SVFCallable = Callable[[None], Callable[[int, dict[str, Any]], dict[int, SchemaVersion]]]


class TestVersion:
    @pytest.fixture
    def version(self):
        return Versioner.V(1)

    def test_version(self, version: Version):
        assert version == Version(1)
        assert isinstance(version, Version)
        assert isinstance(version.value, int)

    def test_tags(self, version: Version):
        assert version.LATEST_VERSION_TAG == "latest"
        assert version.MINUS_1_VERSION_TAG == -1

    @pytest.mark.parametrize("invalid_version", ["string", -10, 0])
    def test_invalid_version(self, invalid_version: VersionTag):
        with pytest.raises(InvalidVersion):
            Versioner.V(invalid_version)

    @pytest.mark.parametrize(
        "version, is_latest",
        [(Versioner.V(-1), True), (Versioner.V(1), False)],
    )
    def test_is_latest(self, version: Version, is_latest: bool):
        assert version.is_latest is is_latest

    def version_0_its_constructable(self) -> None:
        version_0 = Version(0)
        assert version_0.value == 0

    def test_text_formating(self, version: Version):
        assert f"{version}" == "1"
        assert f"{version!r}" == "Version(1)"

    @pytest.mark.parametrize(
        "version, to_compare, comparer, valid",
        [
            (Versioner.V(1), Versioner.V(1), operator.eq, True),
            (Versioner.V(1), Versioner.V(2), operator.eq, False),
            (Versioner.V(2), Versioner.V(1), operator.gt, True),
            (Versioner.V(2), Versioner.V(1), operator.lt, False),
            (Versioner.V(2), Versioner.V(2), operator.ge, True),
            (Versioner.V(2), Versioner.V(1), operator.ge, True),
            (Versioner.V(1), Versioner.V(1), operator.le, True),
            (Versioner.V(1), Versioner.V(2), operator.le, True),
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


class TestVersioner:
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
    ) -> Callable[[Version, dict[str, Any]], dict[Version, SchemaVersion]]:
        def schema_versions(version: Version, schema_version_data: dict[str, Any] | None = None):
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

    @pytest.mark.parametrize("tag, resolved", [("latest", -1), (10, 10), ("20", 20)])
    def test_resolve_tag(self, tag: VersionTag, resolved: int):
        assert Versioner.resolve_tag(tag=tag) == resolved

    @pytest.mark.parametrize(
        "version, resolved_version",
        [
            (Versioner.V(-1), Versioner.V(10)),
            (Versioner.V(1), Versioner.V(1)),
            (Versioner.V(10), Versioner.V(10)),
        ],
    )
    def test_from_schema_versions(
        self,
        version: Version,
        resolved_version: Version,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(Versioner.V(1)))
        schema_versions.update(schema_versions_factory(Versioner.V(2)))
        schema_versions.update(schema_versions_factory(Versioner.V(10)))
        assert Versioner.from_schema_versions(schema_versions, version) == resolved_version

    @pytest.mark.parametrize("nonexisting_version", [Versioner.V(100), Versioner.V(2000)])
    def test_from_schema_versions_nonexisting(
        self,
        nonexisting_version: Version,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(Versioner.V(1)))
        with pytest.raises(VersionNotFoundException):
            Versioner.from_schema_versions(schema_versions, nonexisting_version)

    @pytest.mark.parametrize(
        "tag, resolved",
        [
            ("latest", Versioner.V(-1)),
            (10, Versioner.V(10)),
            ("20", Versioner.V(20)),
            (-1, Versioner.V(-1)),
            ("-1", Versioner.V(-1)),
        ],
    )
    def test_factory_V(self, tag: VersionTag, resolved: int):
        version = Versioner.V(tag=tag)
        assert version == resolved
        assert isinstance(version, Version)

    @pytest.mark.parametrize("tag", ["latest", 10, -1, "-1"])
    def test_validate(self, tag: VersionTag):
        Versioner.validate_tag(tag=tag)

    @pytest.mark.parametrize("tag", ["invalid_version", "0", -20])
    def test_validate_invalid(self, tag: VersionTag):
        """
        Tagger should still keep invalid version 0, we are only backwards compatible, and we should
        avoid generating 0 as a new tag for any schema.
        """
        with pytest.raises(InvalidVersion):
            Versioner.validate_tag(tag=tag)
