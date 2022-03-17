"""
karapace - test anonymize avro

Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from karapace.anonymize_schemas.anonymize_avro import anonymize
from typing import Dict, Union

import json
import pytest

PRIMITIVE_TYPE_SCHEMA = json.loads('"int"')
EXPECTED_PRIMITIVE_TYPE_SCHEMA = "int"

SIMPLE_RECORD_SCHEMA = {
    "type": "record",
    "namespace": "io.aiven",
    "name": "myrecord",
    "fields": [
        {
            "type": "string",
            "name": "f1",
        },
    ],
}
EXPECTED_SIMPLE_RECORD_SCHEMA = {
    "type": "record",
    "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
    "name": "afe8733e983101f1f4ff50d24152890d0da71418",
    "fields": [
        {
            "type": "string",
            "name": "a09bb890b096f7306f688cc6d1dad34e7e52a223",
        },
    ],
}

SIMPLE_ENUM_SCHEMA = {
    "type": "enum",
    "name": "test_enum",
    "symbols": ["A"],
    "default": "B",
}
EXPECTED_SIMPLE_ENUM_SCHEMA = {
    "type": "enum",
    "name": "a6e1e571a3d054d3cd0b116384a282d9d3fd7cba",
    "symbols": ["adcd4ce23d88e2ee9568ba546c007c63d9131c1b"],
    "default": "ae4f281df5a5d0ff3cad6371f76d5c29b6d953ec",
}

COMPLEX_ENUM_SCHEMA = {
    "name": "complex_enum_schema",
    "type": "record",
    "fields": [
        {
            "name": "inner_enum",
            "type": [
                "int",
                "string",
                {
                    "type": "record",
                    "name": "inner_record",
                    "fields": [
                        {
                            "name": "string-field",
                            "type": "string",
                        },
                    ],
                },
            ],
        },
    ],
}
EXPECTED_COMPLEX_ENUM_SCHEMA = {
    "name": "af0524fff42d5747bf8290c713a4da536614919c",
    "type": "record",
    "fields": [
        {
            "name": "a694ca7880e6f0e0c8d616a590b2d2aef0d992ea",
            "type": [
                "int",
                "string",
                {
                    "name": "af7a8ffa0c719a8359f4e02b9212ee9afecc68b4",
                    "type": "record",
                    "fields": [
                        {
                            "name": "a477e6d87a95db19d02fb60126f68b35ccf7b694",
                            "type": "string",
                        }
                    ],
                },
            ],
        },
    ],
}


NO_TYPE_SCHEMA = {
    "name": "test_no_type",
}
EXPECTED_NO_TYPE_SCHEMA = {}

ALL_ELEMENTS_SCHEMA = {
    "type": "record",
    "namespace": "io.aiven",
    "name": "test.Name",
    "doc": "Documentation shall be removed.",
    "aliases": ["io.aiven.RecordAlias", "RecordAlias"],
    "fields": [
        {
            "type": "long",
            "name": "LongField",
            "namespace": "io.aiven",
            "doc": "Long field document shall be removed.",
            "order": "ascending",
        },
        {
            "type": "enum",
            "name": "Enumeration",
            "namespace": "io.aiven",
            "doc": "Enumeration document shall be removed.",
            "aliases": ["io.aiven.EnumAlias"],
            "symbols": ["One", "Two"],
            "default": "Two",
        },
        {
            "type": "array",
            "name": "ArrayField",
            "items": "string",
            "default": [],
            "extra": "Array extra attribute shall be removed.",
        },
        {"type": "array", "name": "ArrayFieldWithUnionType", "items": ["string", "io.aiven.LongField"], "default": []},
        {"type": "map", "name": "MapField", "items": "int", "default": {}},
        {"type": ["null", "io.aiven.test.Name"], "name": "UnionField"},
        {
            "type": "fixed",
            "name": "Fixed",
            "namespace": "io.aiven",
            "doc": "Fixed document shall be removed.",
            "aliases": ["io.aiven.FixedAlias"],
            "size": "16",
        },
    ],
}
EXPECTED_ALL_ELEMENTS_SCHEMA = {
    "type": "record",
    "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
    "name": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3.a09a23220f2c3d64d1e1d6d18c4d5280f8d82fca",
    "aliases": [
        "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a99e5e92725f9873fa5802326346a6067051ee61",  # pylint: disable=line-too-long
        "a99e5e92725f9873fa5802326346a6067051ee61",
    ],
    "fields": [
        {
            "type": "long",
            "name": "ac8fb489b3e9ff687f990ffb2b2f1ec08b0052ca",
            "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
            "order": "ascending",
        },
        {
            "type": "enum",
            "name": "abe0d2dc12d0aa622bacbe23b5516c2e7895e1d7",
            "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
            "aliases": [
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.afaba999b462193044dfff2af00fd144e3622c93"  # pylint: disable=line-too-long
            ],
            "symbols": ["a58b5a8ced9db48b30e008b148004c1065ce53b1", "a6e018ece5a1d3b750531de58d16b961de23d629"],
            "default": "a6e018ece5a1d3b750531de58d16b961de23d629",
        },
        {
            "type": "array",
            "name": "af227bcd25744bf96408ccc655a37521935c7ab1",
            "items": "string",
            "default": [],
        },
        {
            "type": "array",
            "name": "ab840f929bee7c863e52894210d3916a03226427",
            "items": [
                "string",
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.ac8fb489b3e9ff687f990ffb2b2f1ec08b0052ca",  # pylint: disable=line-too-long
            ],
            "default": [],
        },
        {"type": "map", "name": "a7a54c046df923a9827eec4b7f8e631840df1784", "items": "int", "default": {}},
        {
            "type": [
                "null",
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a94a8fe5ccb19ba61c4c0873d391e987982fbbd3.a09a23220f2c3d64d1e1d6d18c4d5280f8d82fca",  # pylint: disable=line-too-long
            ],
            "name": "ae69f15f11172a41f77b4a0aa0b7a60906eecb3c",
        },
        {
            "type": "fixed",
            "name": "ab98f5b85764b8561cacbb055a963cd334928cba",
            "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
            "aliases": [
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a6e4c69f975b291f655e5eaa65ed9eb97781cb01"  # pylint: disable=line-too-long
            ],
            "size": "16",
        },
    ],
}


NESTED_RECORD_SCHEMA = {
    "type": "record",
    "namespace": "io.aiven",
    "name": "test.Name",
    "doc": "Documentation shall be removed.",
    "aliases": ["io.aiven.RecordAlias", "RecordAlias"],
    "fields": [
        {
            "name": "RecordField",
            "type": {
                "type": "record",
                "name": "NestedRecord",
                "namespace": "io.aiven",
                "doc": "Nested record field document shall be removed.",
                "aliases": ["RecordInRecord"],
                "fields": [
                    {
                        "type": "bytes",
                        "name": "BytesFieldInNestedRecord",
                    },
                    {
                        "type": "enum",
                        "name": "Enumeration",
                        "namespace": "io.aiven",
                        "doc": "Enumeration document shall be removed.",
                        "aliases": ["io.aiven.EnumAlias"],
                        "symbols": ["One", "Two"],
                        "default": "Two",
                    },
                ],
            },
        },
    ],
}
EXPECTED_NESTED_RECORD_SCHEMA = {
    "type": "record",
    "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
    "name": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3.a09a23220f2c3d64d1e1d6d18c4d5280f8d82fca",
    "aliases": [
        "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a99e5e92725f9873fa5802326346a6067051ee61",  # pylint: disable=line-too-long
        "a99e5e92725f9873fa5802326346a6067051ee61",
    ],
    "fields": [
        {
            "name": "aa79d718c9cb28e9e6e88fd81b08b342bd2bcb7d",
            "type": {
                "type": "record",
                "name": "a1192f348debd06466f36e5e0cb7b2974ac78c52",
                "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
                "aliases": ["a21bdcb79cbc6a5d167e8c5206015ad1e8f4c14e"],
                "fields": [
                    {
                        "type": "bytes",
                        "name": "a4f78815810f70b4dcdd78be4d0eeff7ee0c6881",
                    },
                    {
                        "type": "enum",
                        "name": "abe0d2dc12d0aa622bacbe23b5516c2e7895e1d7",
                        "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
                        "aliases": [
                            "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.afaba999b462193044dfff2af00fd144e3622c93",  # pylint: disable=line-too-long
                        ],
                        "symbols": ["a58b5a8ced9db48b30e008b148004c1065ce53b1", "a6e018ece5a1d3b750531de58d16b961de23d629"],
                        "default": "a6e018ece5a1d3b750531de58d16b961de23d629",
                    },
                ],
            },
        },
    ],
}


# If a schema of JSON type is given the expected result is partial.
JSON_TYPE_SCHEMA = {
    "type": "object",
    "title": "JSON-schema",
    "description": "example",
    "properties": {"test": {"type": "integer", "title": "my test number", "default": 5}},
}
EXPECTED_JSON_TYPE_SCHEMA = {
    "type": "object",
}


ARRAY_SCHEMA = [
    {
        "items": "string",
        "name": "listofstrings",
        "type": "array",
    },
    "string",
]
EXPECTED_ARRAY_SCHEMA = [
    {
        "items": "string",
        "name": "a6b536863e14adde5b5ffa1bfc7c98be371a78dd",
        "type": "array",
    },
    "string",
]


@pytest.mark.parametrize(
    ["test_schema", "expected_schema"],
    [
        [PRIMITIVE_TYPE_SCHEMA, EXPECTED_PRIMITIVE_TYPE_SCHEMA],
        [SIMPLE_RECORD_SCHEMA, EXPECTED_SIMPLE_RECORD_SCHEMA],
        [SIMPLE_ENUM_SCHEMA, EXPECTED_SIMPLE_ENUM_SCHEMA],
        [COMPLEX_ENUM_SCHEMA, EXPECTED_COMPLEX_ENUM_SCHEMA],
        [NO_TYPE_SCHEMA, EXPECTED_NO_TYPE_SCHEMA],
        [ALL_ELEMENTS_SCHEMA, EXPECTED_ALL_ELEMENTS_SCHEMA],
        [NESTED_RECORD_SCHEMA, EXPECTED_NESTED_RECORD_SCHEMA],
        [JSON_TYPE_SCHEMA, EXPECTED_JSON_TYPE_SCHEMA],
        [ARRAY_SCHEMA, EXPECTED_ARRAY_SCHEMA],
    ],
)
def test_anonymize(test_schema: str, expected_schema: Union[str, Dict[str, str]]):
    res = anonymize(test_schema)
    assert res == expected_schema
