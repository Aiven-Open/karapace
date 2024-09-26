"""
karapace - Unit Test of AvroMerge class

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""


from karapace.schema_models import AvroMerge
from karapace.utils import json_decode, json_encode
from unittest.mock import MagicMock

import pytest


class TestAvroMerge:
    @pytest.fixture
    def avro_merge(self):
        schema_str = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}]}'
        dependencies = {"dependency1": MagicMock(schema=MagicMock(schema_str='{"type": "string"}', dependencies=None))}
        return AvroMerge(schema_str, dependencies)

    def test_init(self, avro_merge):
        assert avro_merge.schema_str == json_encode(json_decode(avro_merge.schema_str), compact=True, sort_keys=True)
        assert avro_merge.unique_id == 0

    def test_union_safe_schema_str_no_union(self, avro_merge):
        result = avro_merge.union_safe_schema_str('{"type": "string"}')
        expected = (
            '{"name": "___RESERVED_KARAPACE_WRAPPER_NAME_0___","type": "record", "fields": [{"name": "name", '
            '"type": [{"type": "string"}]}]}'
        )
        assert result == expected

    def test_union_safe_schema_str_with_union(self, avro_merge):
        result = avro_merge.union_safe_schema_str('["null", "string"]')
        expected = (
            '{"name": "___RESERVED_KARAPACE_WRAPPER_NAME_0___","type": "record", "fields": [{"name": "name", '
            '"type": ["null", "string"]}]}'
        )
        assert result == expected

    def test_builder_no_dependencies(self, avro_merge):
        avro_merge.dependencies = None
        result = avro_merge.builder(avro_merge.schema_str)
        expected = (
            '{"name": "___RESERVED_KARAPACE_WRAPPER_NAME_1___","type": "record", "fields": [{"name": "name", '
            '"type": [{"fields":[{"name":"field1","type":"string"}],"name":"Test","type":"record"}]}]}'
        )
        assert result == expected

    def test_builder_with_dependencies(self, avro_merge):
        result = avro_merge.builder(avro_merge.schema_str, avro_merge.dependencies)
        expected = (
            '{"name": "___RESERVED_KARAPACE_WRAPPER_NAME_1___","type": "record", "fields": [{"name": "name", "type": [{'
            '"type": "string"}]}]},'
            "\n"
            '{"name": "___RESERVED_KARAPACE_WRAPPER_NAME_2___","type": "record", "fields": [{"name": "name", "type": [{'
            '"fields":[{"name":"field1","type":"string"}],"name":"Test","type":"record"}]}]}'
        )
        assert result == expected

    def test_wrap(self, avro_merge):
        result = avro_merge.wrap()
        expected = (
            "[\n"
            + (
                '{"name": "___RESERVED_KARAPACE_WRAPPER_NAME_1___","type": "record", "fields": [{"name": "name", "type": [{'
                '"type": "string"}]}]},'
                "\n"
                '{"name": "___RESERVED_KARAPACE_WRAPPER_NAME_2___","type": "record", "fields": [{"name": "name", "type": [{'
                '"fields":[{"name":"field1","type":"string"}],"name":"Test","type":"record"}]}]}'
            )
            + "\n]"
        )
        assert result == expected
