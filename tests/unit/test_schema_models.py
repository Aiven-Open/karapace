"""
karapace - Test schema models

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from avro.schema import Schema as AvroSchema
from karapace.errors import InvalidVersion, VersionNotFoundException
from karapace.schema_models import parse_avro_schema_definition, SchemaVersion, TypedSchema, VersionTEMP
from karapace.schema_type import SchemaType
from typing import Any, Callable, Dict, Optional, Union

import pytest

# Schema versions factory fixture type
SVFCallable = Callable[[None], Callable[[int, Dict[str, Any]], Dict[int, SchemaVersion]]]


class TestVersionTEMP:
    @pytest.fixture
    def version(self):
        return VersionTEMP(1)

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

    def test_tags(self, version: VersionTEMP):
        assert version.LATEST_VERSION_TAG == "latest"
        assert version.MINUS_1_VERSION_TAG == "-1"

    @pytest.mark.parametrize(
        "version, is_latest",
        [(VersionTEMP("latest"), True), (VersionTEMP("-1"), True), (VersionTEMP("-20"), False), (VersionTEMP(10), False)],
    )
    def test_is_latest(self, version: VersionTEMP, is_latest: bool):
        assert version.is_latest is is_latest

    @pytest.mark.parametrize("version", [VersionTEMP("latest"), VersionTEMP(10), VersionTEMP(-1)])
    def test_validate(self, version: VersionTEMP):
        version.validate()

    @pytest.mark.parametrize("invalid_version", [VersionTEMP("invalid_version"), VersionTEMP(0), VersionTEMP(-20)])
    def test_validate_invalid(self, invalid_version: VersionTEMP):
        with pytest.raises(InvalidVersion):
            assert not invalid_version.validate()

    @pytest.mark.parametrize(
        "version, resolved_version",
        [
            (VersionTEMP("latest"), "latest"),
            (VersionTEMP(-1), "latest"),
            (VersionTEMP("-1"), "latest"),
            (VersionTEMP(10), "10"),
        ],
    )
    def test_resolved(self, version: VersionTEMP, resolved_version: Union[str, int]):
        assert version.resolved == resolved_version

    @pytest.mark.parametrize(
        "version, resolved_version",
        [
            (VersionTEMP("-1"), 10),
            (VersionTEMP(-1), 10),
            (VersionTEMP(1), 1),
            (VersionTEMP(10), 10),
            (VersionTEMP("latest"), 10),
        ],
    )
    def test_resolve_from_schema_versions(
        self,
        version: VersionTEMP,
        resolved_version: int,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(1))
        schema_versions.update(schema_versions_factory(2))
        schema_versions.update(schema_versions_factory(10))
        assert version.resolve_from_schema_versions(schema_versions) == resolved_version

    @pytest.mark.parametrize(
        "invalid_version",
        [
            VersionTEMP("invalid_version"),
            VersionTEMP(0),
            VersionTEMP(-20),
            VersionTEMP("-10"),
            VersionTEMP("100"),
            VersionTEMP(2000),
        ],
    )
    def test_resolve_from_schema_versions_invalid(
        self,
        invalid_version: VersionTEMP,
        schema_versions_factory: SVFCallable,
    ):
        schema_versions = dict()
        schema_versions.update(schema_versions_factory(1))
        with pytest.raises(VersionNotFoundException):
            invalid_version.resolve_from_schema_versions(schema_versions)
