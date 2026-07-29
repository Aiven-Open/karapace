"""
karapace - Test schema models

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import json
import operator
import socket
from collections.abc import Callable
from typing import Any

import pytest
from avro.schema import Schema as AvroSchema

from karapace.core.compatibility import CompatibilityModes
from karapace.core.compatibility.schema_compatibility import SchemaCompatibility
from karapace.core.errors import InvalidSchema, InvalidVersion, VersionNotFoundException
from karapace.core.schema_models import (
    ParsedTypedSchema,
    SchemaVersion,
    TypedSchema,
    ValidatedTypedSchema,
    Versioner,
    parse_avro_schema_definition,
    parse_jsonschema_definition,
)
from karapace.core.schema_type import SchemaType
from karapace.core.typing import Version, VersionTag

# Schema versions factory fixture type
SVFCallable = Callable[[None], Callable[[int, dict[str, Any]], dict[int, SchemaVersion]]]

DRAFT7_URI = "http://json-schema.org/draft-07/schema#"
DRAFT201909_URI = "https://json-schema.org/draft/2019-09/schema"
DRAFT202012_URI = "https://json-schema.org/draft/2020-12/schema"


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


class TestJsonSchemaDraftRouting:
    @pytest.mark.parametrize(
        "schema_uri, expected_validator",
        [
            (None, "Draft7Validator"),
            ("http://json-schema.org/draft-04/schema#", "Draft4Validator"),
            ("http://json-schema.org/draft-06/schema#", "Draft6Validator"),
            (DRAFT7_URI, "Draft7Validator"),
            (DRAFT201909_URI, "Draft201909Validator"),
            (DRAFT202012_URI, "Draft202012Validator"),
        ],
    )
    def test_draft_selected_from_schema_keyword(self, schema_uri: str | None, expected_validator: str):
        schema: dict[str, Any] = {"type": "object", "properties": {"a": {"type": "string"}}}
        if schema_uri is not None:
            schema["$schema"] = schema_uri
        parsed = parse_jsonschema_definition(json.dumps(schema))
        assert type(parsed).__name__ == expected_validator

    def test_no_schema_keyword_defaults_to_draft7(self):
        """No `$schema` must parse as Draft-7, preserving prior behavior (no regression)."""
        parsed = parse_jsonschema_definition('{"type":"object"}')
        assert type(parsed).__name__ == "Draft7Validator"

    def test_unknown_schema_uri_defaults_to_draft7(self):
        """An unrecognized `$schema` URI falls back to Draft-7 rather than raising."""
        parsed = parse_jsonschema_definition('{"$schema":"http://example.com/unknown","type":"object"}')
        assert type(parsed).__name__ == "Draft7Validator"

    def test_2020_12_prefix_items_is_accepted(self):
        parsed = parse_jsonschema_definition(
            json.dumps({"$schema": DRAFT202012_URI, "type": "array", "prefixItems": [{"type": "integer"}]})
        )
        assert type(parsed).__name__ == "Draft202012Validator"

    def test_2020_12_defs_ref_is_accepted(self):
        parsed = parse_jsonschema_definition(
            json.dumps({"$schema": DRAFT202012_URI, "$defs": {"a": {"type": "string"}}, "$ref": "#/$defs/a"})
        )
        assert type(parsed).__name__ == "Draft202012Validator"

    def test_2019_09_dependent_required_is_accepted(self):
        parsed = parse_jsonschema_definition(
            json.dumps({"$schema": DRAFT201909_URI, "type": "object", "dependentRequired": {"a": ["b"]}})
        )
        assert type(parsed).__name__ == "Draft201909Validator"

    def test_invalid_schema_for_declared_draft_raises(self):
        """A schema that is invalid for its declared draft must be rejected, not silently accepted."""
        # `type` must be a string or array of strings; an integer is invalid in every draft.
        with pytest.raises(InvalidSchema):
            ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, json.dumps({"$schema": DRAFT202012_URI, "type": 123}))

    @pytest.mark.parametrize("bad_schema_value", [123, ["x"], {"k": 1}])
    def test_non_string_schema_keyword_raises_invalid_schema(self, bad_schema_value: Any):
        """A non-string `$schema` must raise InvalidSchema, not an uncaught AttributeError."""
        with pytest.raises(InvalidSchema):
            ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, json.dumps({"$schema": bad_schema_value, "type": "object"}))

    def test_no_network_fetch_on_parse(self, monkeypatch: pytest.MonkeyPatch):
        """Parsing/validating any built-in draft must not trigger network I/O."""

        def _no_connect(*args: Any, **kwargs: Any):
            raise AssertionError("network access attempted during schema parse")

        monkeypatch.setattr(socket.socket, "connect", _no_connect)
        for uri in (DRAFT7_URI, DRAFT201909_URI, DRAFT202012_URI):
            validator = parse_jsonschema_definition(
                json.dumps({"$schema": uri, "type": "object", "properties": {"a": {"type": "string"}}})
            )
            # iter_errors exercises the validator against an instance without any remote fetch.
            assert list(validator.iter_errors({"a": "x"})) == []

    def test_stored_newer_draft_schema_loads_and_is_compatible_checkable(self):
        """A previously stored 2020-12 schema must load and flow through compat without raising."""
        schema_str = json.dumps(
            {
                "$schema": DRAFT202012_URI,
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "additionalProperties": True,
            }
        )
        parsed = ParsedTypedSchema.parse(SchemaType.JSONSCHEMA, schema_str)
        assert type(parsed.schema).__name__ == "Draft202012Validator"

        result = SchemaCompatibility.check_compatibility(parsed, parsed, CompatibilityModes.BACKWARD)
        assert result is not None
