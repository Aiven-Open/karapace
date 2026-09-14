"""
karapace - Test JSON Schema Draft 2019-09 / 2020-12 compatibility

Copyright (c) 2024 Aiven Ltd
See LICENSE for details

Covers the compatibility behaviour for newer-draft keywords:
- renamed/relocated keywords are canonicalized to Draft-7 shapes and compared correctly
  (including cross-draft comparisons), and
- keywords the engine cannot yet evaluate are rejected (fail-closed) in compatibility-checked modes.

See docs/json-schema-draft-compatibility-strategy.md.
"""

import json
import warnings

import pytest
from avro.compatibility import SchemaCompatibilityType

from karapace.core.compatibility import CompatibilityModes
from karapace.core.compatibility.jsonschema.checks import compatibility
from karapace.core.compatibility.schema_compatibility import SchemaCompatibility
from karapace.core.schema_models import parse_jsonschema_definition, ParsedTypedSchema, ValidatedTypedSchema
from karapace.core.schema_type import SchemaType

DRAFT7_URI = "http://json-schema.org/draft-07/schema#"
DRAFT201909_URI = "https://json-schema.org/draft/2019-09/schema"
DRAFT202012_URI = "https://json-schema.org/draft/2020-12/schema"


def _validator(schema: dict):
    return parse_jsonschema_definition(json.dumps(schema))


def _is_compatible(reader: dict, writer: dict) -> bool:
    """Low-level engine check (post-canonicalization). reader can read writer's data."""
    result = compatibility(_validator(reader), _validator(writer))
    return result.compatibility is SchemaCompatibilityType.compatible


def _check(old: dict, new: dict, mode: CompatibilityModes):
    """Full compatibility check through the public entry point (exercises the fail-closed guard)."""
    return SchemaCompatibility.check_compatibility(
        ParsedTypedSchema.parse(SchemaType.JSONSCHEMA, json.dumps(old)),
        ValidatedTypedSchema.parse(SchemaType.JSONSCHEMA, json.dumps(new)),
        mode,
    )


class TestCanonicalEquivalence:
    """Renamed/relocated keywords compare as equivalent to their Draft-7 spelling."""

    def test_definitions_and_defs_are_equivalent(self):
        draft7 = {"$schema": DRAFT7_URI, "definitions": {"a": {"type": "string"}}, "$ref": "#/definitions/a"}
        draft2020 = {"$schema": DRAFT202012_URI, "$defs": {"a": {"type": "string"}}, "$ref": "#/$defs/a"}
        assert _is_compatible(reader=draft2020, writer=draft7)
        assert _is_compatible(reader=draft7, writer=draft2020)

    def test_items_list_and_prefix_items_are_equivalent(self):
        draft7 = {"$schema": DRAFT7_URI, "type": "array", "items": [{"type": "integer"}]}
        draft2020 = {"$schema": DRAFT202012_URI, "type": "array", "prefixItems": [{"type": "integer"}]}
        assert _is_compatible(reader=draft2020, writer=draft7)
        assert _is_compatible(reader=draft7, writer=draft2020)

    def test_closed_tuple_additional_items_and_items_false_are_equivalent(self):
        draft7 = {
            "$schema": DRAFT7_URI,
            "type": "array",
            "items": [{"type": "integer"}],
            "additionalItems": False,
        }
        draft2020 = {
            "$schema": DRAFT202012_URI,
            "type": "array",
            "prefixItems": [{"type": "integer"}],
            "items": False,
        }
        assert _is_compatible(reader=draft2020, writer=draft7)
        assert _is_compatible(reader=draft7, writer=draft2020)

    def test_dependencies_and_dependent_required_are_equivalent(self):
        draft7 = {"$schema": DRAFT7_URI, "type": "object", "dependencies": {"a": ["b"]}}
        draft2019 = {"$schema": DRAFT201909_URI, "type": "object", "dependentRequired": {"a": ["b"]}}
        assert _is_compatible(reader=draft2019, writer=draft7)
        assert _is_compatible(reader=draft7, writer=draft2019)


class TestEvolutionRulesAfterCanonicalization:
    """Draft-7 evolution rules still apply to the canonicalized newer-draft keywords."""

    def test_adding_prefix_items_position_is_backward_incompatible(self):
        # Reader requires a 2nd tuple position the writer does not produce -> reader restricts.
        writer = {"$schema": DRAFT202012_URI, "type": "array", "prefixItems": [{"type": "integer"}]}
        reader = {
            "$schema": DRAFT202012_URI,
            "type": "array",
            "prefixItems": [{"type": "integer"}, {"type": "string"}],
        }
        assert not _is_compatible(reader=reader, writer=writer)

    def test_dependent_required_matches_dependencies_behaviour(self):
        # dependentRequired must behave identically to Draft-7 dependencies for the same change.
        draft7_writer = {"$schema": DRAFT7_URI, "type": "object"}
        draft7_reader = {"$schema": DRAFT7_URI, "type": "object", "dependencies": {"a": ["b"]}}
        draft2020_writer = {"$schema": DRAFT202012_URI, "type": "object"}
        draft2020_reader = {"$schema": DRAFT202012_URI, "type": "object", "dependentRequired": {"a": ["b"]}}
        assert _is_compatible(reader=draft2020_reader, writer=draft2020_writer) == _is_compatible(
            reader=draft7_reader, writer=draft7_writer
        )


class TestCrossDraftCompatibility:
    """A subject may evolve across drafts; compatibility is computed on the canonical form."""

    @pytest.mark.parametrize(
        "mode",
        [CompatibilityModes.BACKWARD, CompatibilityModes.FORWARD, CompatibilityModes.FULL],
    )
    def test_structurally_equal_cross_draft_is_compatible(self, mode: CompatibilityModes):
        old = {"$schema": DRAFT7_URI, "type": "object", "properties": {"a": {"type": "string"}}}
        new = {"$schema": DRAFT202012_URI, "type": "object", "properties": {"a": {"type": "string"}}}
        result = _check(old, new, mode)
        assert result.compatibility is SchemaCompatibilityType.compatible

    def test_structurally_breaking_cross_draft_is_incompatible(self):
        # New (reader) narrows type from string to integer -> not backward compatible.
        old = {"$schema": DRAFT7_URI, "type": "object", "properties": {"a": {"type": "number"}}}
        new = {"$schema": DRAFT202012_URI, "type": "object", "properties": {"a": {"type": "integer"}}}
        result = _check(old, new, CompatibilityModes.BACKWARD)
        assert result.compatibility is SchemaCompatibilityType.incompatible


class TestFailClosedGuard:
    """Keywords the engine cannot evaluate are rejected in compatibility-checked modes."""

    @pytest.mark.parametrize(
        "keyword_fragment",
        [
            {"unevaluatedProperties": False},
            {"unevaluatedItems": False},
            {"$dynamicRef": "#node"},
            {"$dynamicAnchor": "node"},
            {"$recursiveRef": "#"},
            {"$vocabulary": {"https://json-schema.org/draft/2020-12/vocab/core": True}},
        ],
    )
    def test_unsupported_keyword_is_rejected(self, keyword_fragment: dict):
        old = {"$schema": DRAFT202012_URI, "type": "object"}
        new = {"$schema": DRAFT202012_URI, "type": "object", **keyword_fragment}
        result = _check(old, new, CompatibilityModes.BACKWARD)
        assert result.compatibility is SchemaCompatibilityType.incompatible
        keyword = next(iter(keyword_fragment))
        assert any(keyword in message for message in result.messages)

    def test_unsupported_keyword_allowed_under_none_mode(self):
        old = {"$schema": DRAFT202012_URI, "type": "object"}
        new = {"$schema": DRAFT202012_URI, "type": "object", "unevaluatedProperties": False}
        result = _check(old, new, CompatibilityModes.NONE)
        assert result.compatibility is SchemaCompatibilityType.compatible

    def test_unsupported_keyword_detected_when_nested(self):
        old = {"$schema": DRAFT202012_URI, "type": "object"}
        new = {
            "$schema": DRAFT202012_URI,
            "type": "object",
            "properties": {"a": {"type": "object", "unevaluatedProperties": False}},
        }
        result = _check(old, new, CompatibilityModes.BACKWARD)
        assert result.compatibility is SchemaCompatibilityType.incompatible

    @pytest.mark.parametrize(
        "schema_fragment",
        [
            # An author-chosen property name that collides with a keyword must NOT trip the guard.
            {"properties": {"unevaluatedItems": {"type": "string"}}},
            {"properties": {"$vocabulary": {"type": "string"}}},
            # A $defs entry named like a keyword is a name, not a keyword.
            {"$defs": {"unevaluatedProperties": {"type": "string"}}},
            # Keyword-like strings that are instance data, not schema keywords.
            {"enum": ["$dynamicRef", "unevaluatedItems"]},
            {"const": {"$vocabulary": {"x": True}}},
        ],
    )
    def test_keyword_like_names_and_values_do_not_trip_guard(self, schema_fragment: dict):
        old = {"$schema": DRAFT202012_URI, "type": "object", **schema_fragment}
        new = {"$schema": DRAFT202012_URI, "type": "object", **schema_fragment}
        result = _check(old, new, CompatibilityModes.BACKWARD)
        assert result.compatibility is SchemaCompatibilityType.compatible


class TestNoDraft7Regression:
    """Spot-check that plain Draft-7 schemas are unaffected by canonicalization."""

    def test_widening_a_constraint_still_compatible(self):
        # Reader relaxes the numeric upper bound the writer had -> backward compatible.
        writer = {"$schema": DRAFT7_URI, "type": "integer", "maximum": 10}
        reader = {"$schema": DRAFT7_URI, "type": "integer"}
        assert _is_compatible(reader=reader, writer=writer)


class TestNoResolverDeprecationNoise:
    """The resolver access shim keeps compatibility checks free of deprecation warnings."""

    def test_compat_check_emits_no_resolver_deprecation(self):
        old = {"$schema": DRAFT202012_URI, "type": "object", "properties": {"a": {"type": "string"}}}
        new = {"$schema": DRAFT202012_URI, "type": "object", "properties": {"a": {"type": "string"}}}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _check(old, new, CompatibilityModes.BACKWARD)
        resolver_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning) and "resolver" in str(w.message)
        ]
        assert resolver_warnings == []

    def test_type_narrowing_still_incompatible(self):
        writer = {"$schema": DRAFT7_URI, "type": "number"}
        reader = {"$schema": DRAFT7_URI, "type": "integer"}
        assert not _is_compatible(reader=reader, writer=writer)
