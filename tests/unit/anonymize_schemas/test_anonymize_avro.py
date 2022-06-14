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

# Pylint issue: https://github.com/PyCQA/pylint/issues/3368
# pylint: disable=line-too-long
SCHEMA_WITH_NAME = json.loads('"io.aiven.myrecord"')
EXPECTED_SCHEMA_WITH_NAME = "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.afe8733e983101f1f4ff50d24152890d0da71418"


FIXED_VALUE_SCHEMA_INVALID = json.loads("100.101")
EXPECTED_FIXED_VALUE_SCHEMA_INVALID = FIXED_VALUE_SCHEMA_INVALID


SIMPLE_RECORD_SCHEMA = {
    "type": "record",
    "namespace": "io.aiven",
    "name": "myrecord",
    "fields": [
        {
            "type": "string",
            "name": "f1",
            "default": "default_value",
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
            "default": "a6c8109f13a310f20c45ad334e5f0e159a4fb896",
        },
    ],
}


SPECIAL_CHARACTERS_SCHEMA = {
    "type": "record",
    "name": "!m@y#r$e%c^o&r*d(=.m)y_r-e+c[o]r<d>?",
}
EXPECTED_SPECIAL_CHARACTERS_SCHEMA = {
    "type": "record",
    "name": "a8961a6e24b2695754fca99058c5ac1ccd3470f0!#$%&(*=@^.a5f69786327ca01574e464383e995d5d333e0170)+-<>?[]",
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
                            "logicalType": "uuid",
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
                            "name": "a477e6d87a95db19d02fb60126f68b35ccf7b694-",
                            "type": "string",
                            "logicalType": "uuid",
                        }
                    ],
                },
            ],
        },
    ],
}


INVALID_ENUM_SCHEMA = {
    "type": "enum",
    "name": "test_enum",
    "symbols": ["1", "2"],
    "default": 1,
}

EXPECTED_INVALID_ENUM_SCHEMA = {
    "type": "enum",
    "name": "a6e1e571a3d054d3cd0b116384a282d9d3fd7cba",
    "symbols": ["a56a192b7913b04c54574d18c28d46e6395428ab", "aa4b9237bacccdf19c0760cab7aec4a8359010b0"],
    "default": 1,
}


NO_TYPE_SCHEMA = {
    "name": "test_no_type",
}
EXPECTED_NO_TYPE_SCHEMA = {
    "name": "a881d8506234c835bbad094140aec6a094c68e0a",
}


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
            "default": 1,
        },
        {
            "type": "float",
            "name": "FloatField",
            "namespace": "io.aiven",
            "doc": "Float field document shall be removed.",
            "order": "ascending",
            "default": 1.12345,
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
            "items": "io.aiven.test.Name",
            "default": [],
            "extra": "Array extra attribute shall be removed.",
        },
        {
            "type": "array",
            "name": "ArrayFieldWithUnionType",
            "items": [
                "string",
                "io.aiven.LongField",
            ],
            "default": [],
        },
        {
            "type": "map",
            "name": "MapField",
            "values": "int",
            "default": {},
        },
        {
            "type": [
                "null",
                "io.aiven.test.Name",
            ],
            "name": "UnionField",
        },
        {
            "type": "fixed",
            "name": "Fixed",
            "namespace": "io.aiven",
            "doc": "Fixed document shall be removed.",
            "aliases": ["io.aiven.FixedAlias"],
            "order": "invalidValue",
            "size": 16,
        },
    ],
}
EXPECTED_ALL_ELEMENTS_SCHEMA = {
    "type": "record",
    "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
    "name": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3.a09a23220f2c3d64d1e1d6d18c4d5280f8d82fca",
    "aliases": [
        "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a99e5e92725f9873fa5802326346a6067051ee61",
        "a99e5e92725f9873fa5802326346a6067051ee61",
    ],
    "fields": [
        {
            "type": "long",
            "name": "ac8fb489b3e9ff687f990ffb2b2f1ec08b0052ca",
            "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
            "order": "ascending",
            "default": 1,
        },
        {
            "type": "float",
            "name": "acdc1208676b23ea32d59df3dcc23e4161aa6a26",
            "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
            "order": "ascending",
            "default": 1.12345,
        },
        {
            "type": "enum",
            "name": "abe0d2dc12d0aa622bacbe23b5516c2e7895e1d7",
            "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
            "aliases": [
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.afaba999b462193044dfff2af00fd144e3622c93"
            ],
            "symbols": ["a58b5a8ced9db48b30e008b148004c1065ce53b1", "a6e018ece5a1d3b750531de58d16b961de23d629"],
            "default": "a6e018ece5a1d3b750531de58d16b961de23d629",
        },
        {
            "type": "array",
            "name": "af227bcd25744bf96408ccc655a37521935c7ab1",
            "items": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a94a8fe5ccb19ba61c4c0873d391e987982fbbd3.a09a23220f2c3d64d1e1d6d18c4d5280f8d82fca",
            "default": [],
            "a43c4b82570e182eb1c74072896167113d2c7345": "a0f1005a1091064f11247324586b3fe8b4504e26 .",
        },
        {
            "type": "array",
            "name": "ab840f929bee7c863e52894210d3916a03226427",
            "items": [
                "string",
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.ac8fb489b3e9ff687f990ffb2b2f1ec08b0052ca",
            ],
            "default": [],
        },
        {
            "type": "map",
            "name": "a7a54c046df923a9827eec4b7f8e631840df1784",
            "values": "int",
            "default": {},
        },
        {
            "type": [
                "null",
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a94a8fe5ccb19ba61c4c0873d391e987982fbbd3.a09a23220f2c3d64d1e1d6d18c4d5280f8d82fca",
            ],
            "name": "ae69f15f11172a41f77b4a0aa0b7a60906eecb3c",
        },
        {
            "type": "fixed",
            "name": "ab98f5b85764b8561cacbb055a963cd334928cba",
            "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
            "aliases": [
                "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a6e4c69f975b291f655e5eaa65ed9eb97781cb01"
            ],
            "order": "ae5d03fc0f0537f32701e8cab51a448a0f591d97",
            "size": 16,
        },
    ],
}

ALL_ELEMENTS_INVALID_DATA_SCHEMA = {
    "type": 1,
    "namespace": 1,
    "name": 1,
    "doc": 1,
    "aliases": [2, 3],
    "fields": [
        {
            "type": "long",
            "name": 4,
            "namespace": 4,
            "doc": ["documentation in an array"],
            "order": 4,
            "default": "invalid",
        },
        {
            "type": "enum",
            "name": 5,
            "namespace": 5,
            "doc": {"doc": "documentation in a dict"},
            "aliases": [6],
            "symbols": [7, 8],
            "default": 9,
        },
        {
            "type": "array",
            "name": 10,
            "items": 10,
            "default": 10,
            10: "invalid key",
        },
        {
            "type": "map",
            "name": 11,
            "values": 11,
            "default": 11,
        },
        {
            "type": [
                12,
                13,
            ],
            "name": 12,
        },
        {
            "type": "fixed",
            "name": 14,
            "namespace": 14,
            "order": 14,
            "size": "14",
        },
    ],
}
EXPECTED_ALL_ELEMENTS_INVALID_DATA_SCHEMA = {
    "type": 1,
    "namespace": 1,
    "name": 1,
    "aliases": [2, 3],
    "fields": [
        {
            "type": "long",
            "name": 4,
            "namespace": 4,
            "order": 4,
            "default": "a1f344a7686a80b4c5293e8fdc0b0160c82c06a8",
        },
        {
            "type": "enum",
            "name": 5,
            "namespace": 5,
            "aliases": [6],
            "symbols": [7, 8],
            "default": 9,
        },
        {
            "type": "array",
            "name": 10,
            "items": 10,
            "default": 10,
            10: "a0b769b8796e493b9e82cb872cc9871921d67af8 ",
        },
        {
            "type": "map",
            "name": 11,
            "values": 11,
            "default": 11,
        },
        {
            "type": [
                12,
                13,
            ],
            "name": 12,
        },
        {
            "type": "fixed",
            "name": 14,
            "namespace": 14,
            "order": 14,
            "size": "aa35e192121eabf3dabf9f5ea6abdbcbc107ac3b",
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
        "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.a99e5e92725f9873fa5802326346a6067051ee61",
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
                            "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382.afaba999b462193044dfff2af00fd144e3622c93",
                        ],
                        "symbols": ["a58b5a8ced9db48b30e008b148004c1065ce53b1", "a6e018ece5a1d3b750531de58d16b961de23d629"],
                        "default": "a6e018ece5a1d3b750531de58d16b961de23d629",
                    },
                ],
            },
        },
    ],
}


ARRAY_WITH_SCHEMA_IN_ITEMS = {
    "type": "array",
    "items": SIMPLE_RECORD_SCHEMA,
    "default": [],
}
EXPECTED_ARRAY_WITH_SCHEMA_IN_ITEMS = {
    "type": "array",
    "items": EXPECTED_SIMPLE_RECORD_SCHEMA,
    "default": [],
}


MAP_WITH_SCHEMA_IN_ITEMS = {
    "type": "map",
    "values": SIMPLE_RECORD_SCHEMA,
    "default": [],
}
EXPECTED_MAP_WITH_SCHEMA_IN_ITEMS = {
    "type": "map",
    "values": EXPECTED_SIMPLE_RECORD_SCHEMA,
    "default": [],
}


JSON_TYPE_SCHEMA = {
    "type": "object",
    "title": "JSON-schema",
    "description": "example",
    "properties": {"test": {"type": "integer", "title": "my test number", "default": 5}},
}
EXPECTED_JSON_TYPE_SCHEMA = {
    "type": "a615307cc4523f183e777df67f168c86908e8007",
    "ac6de1b7dd91465d437ef415f94f36afc1fbc8a8": "aa6778ddeb92053ff2f1bdefc1ad93e85cf2e67f-",
    "ab329146a0dd0d566b0628744d67936558741ffa": "a3499c2729730a7f807efb8676a92dcb6f8a3f8f",
    "a0449e86077449843777d1958aff83cf086dbcba": {
        "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3": {
            "type": "a178cafbd64bbbfa77f5ac0a9d5032ed88162781",
            "ac6de1b7dd91465d437ef415f94f36afc1fbc8a8": "ad2285a805023b2a4fc65e55cfa28ca26111c438 ",
            "default": 5,
        },
    },
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


LOGICAL_TYPE_SCHEMA = {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}
EXPECTED_LOGICAL_TYPE_SCHEMA = LOGICAL_TYPE_SCHEMA


EMPTY_DICT = {}
EMPTY_ARR = {}
EMPTY_STR = ""


@pytest.mark.parametrize(
    ["test_schema", "expected_schema"],
    [
        [PRIMITIVE_TYPE_SCHEMA, EXPECTED_PRIMITIVE_TYPE_SCHEMA],
        [SCHEMA_WITH_NAME, EXPECTED_SCHEMA_WITH_NAME],
        [FIXED_VALUE_SCHEMA_INVALID, EXPECTED_FIXED_VALUE_SCHEMA_INVALID],
        [SIMPLE_RECORD_SCHEMA, EXPECTED_SIMPLE_RECORD_SCHEMA],
        [SPECIAL_CHARACTERS_SCHEMA, EXPECTED_SPECIAL_CHARACTERS_SCHEMA],
        [SIMPLE_ENUM_SCHEMA, EXPECTED_SIMPLE_ENUM_SCHEMA],
        [COMPLEX_ENUM_SCHEMA, EXPECTED_COMPLEX_ENUM_SCHEMA],
        [NO_TYPE_SCHEMA, EXPECTED_NO_TYPE_SCHEMA],
        [ALL_ELEMENTS_SCHEMA, EXPECTED_ALL_ELEMENTS_SCHEMA],
        [ALL_ELEMENTS_INVALID_DATA_SCHEMA, EXPECTED_ALL_ELEMENTS_INVALID_DATA_SCHEMA],
        [NESTED_RECORD_SCHEMA, EXPECTED_NESTED_RECORD_SCHEMA],
        [ARRAY_WITH_SCHEMA_IN_ITEMS, EXPECTED_ARRAY_WITH_SCHEMA_IN_ITEMS],
        [JSON_TYPE_SCHEMA, EXPECTED_JSON_TYPE_SCHEMA],
        [ARRAY_SCHEMA, EXPECTED_ARRAY_SCHEMA],
        [INVALID_ENUM_SCHEMA, EXPECTED_INVALID_ENUM_SCHEMA],
        [LOGICAL_TYPE_SCHEMA, EXPECTED_LOGICAL_TYPE_SCHEMA],
        [EMPTY_ARR, EMPTY_ARR],
        [EMPTY_DICT, EMPTY_DICT],
        [EMPTY_STR, EMPTY_STR],
    ],
)
def test_anonymize(test_schema: str, expected_schema: Union[str, Dict[str, str]]):
    res = anonymize(test_schema)
    assert res == expected_schema
