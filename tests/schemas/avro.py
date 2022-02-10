from avro.schema import ArraySchema, Field, MapSchema, RecordSchema, UnionSchema
from karapace.avro_compatibility import parse_avro_schema_definition

import json

schema1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "myrecord",
            "fields": [
                {"type": "string", "name": "f1"},
            ],
        },
    )
)
schema2 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "myrecord",
            "fields": [
                {"type": "string", "name": "f1"},
                {"type": "string", "name": "f2", "default": "foo"},
            ],
        }
    )
)
schema3 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "myrecord",
            "fields": [
                {"type": "string", "name": "f1"},
                {"type": "string", "name": "f2"},
            ],
        }
    )
)
schema4 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "myrecord",
            "fields": [
                {"type": "string", "name": "f1_new", "aliases": ["f1"]},
            ],
        },
    )
)
schema6 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "myrecord",
            "fields": [
                {"type": ["null", "string"], "name": "f1", "doc": "doc of f1"},
            ],
        }
    )
)
schema7 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "myrecord",
            "fields": [
                {"type": ["null", "string", "int"], "name": "f1", "doc": "doc of f1"},
            ],
        }
    )
)

schema8 = parse_avro_schema_definition(
    # This is invalid JSON
    '{"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string",'
    '"name":"f2","default":"foo"}]},{"type":"string","name":"f3","default":"bar"}]}'
)
# badDefaultNullString = parse_avro_schema_definition(
#     json.dumps(
#         {
#             "type": "record",
#             "name": "myrecord",
#             "fields": [
#                 {"type": ["null", "string"], "name": "f1", "default": "null"},
#                 {"type": "string", "name": "f2", "default": "foo"},
#                 {"type": "string", "name": "f3", "default": "bar"},
#             ],
#         }
#     )
# )

BOOLEAN_SCHEMA = parse_avro_schema_definition(json.dumps("boolean"))
NULL_SCHEMA = parse_avro_schema_definition(json.dumps("null"))
INT_SCHEMA = parse_avro_schema_definition(json.dumps("int"))
LONG_SCHEMA = parse_avro_schema_definition(json.dumps("long"))
STRING_SCHEMA = parse_avro_schema_definition(json.dumps("string"))
BYTES_SCHEMA = parse_avro_schema_definition(json.dumps("bytes"))
FLOAT_SCHEMA = parse_avro_schema_definition(json.dumps("float"))
DOUBLE_SCHEMA = parse_avro_schema_definition(json.dumps("double"))
INT_ARRAY_SCHEMA = ArraySchema(INT_SCHEMA)
LONG_ARRAY_SCHEMA = ArraySchema(LONG_SCHEMA)
STRING_ARRAY_SCHEMA = ArraySchema(STRING_SCHEMA)
INT_MAP_SCHEMA = MapSchema(INT_SCHEMA)
LONG_MAP_SCHEMA = MapSchema(LONG_SCHEMA)
STRING_MAP_SCHEMA = MapSchema(STRING_SCHEMA)
ENUM1_AB_SCHEMA = parse_avro_schema_definition(json.dumps({"type": "enum", "name": "Enum1", "symbols": ["A", "B"]}))
ENUM1_ABC_SCHEMA = parse_avro_schema_definition(json.dumps({"type": "enum", "name": "Enum1", "symbols": ["A", "B", "C"]}))
ENUM1_BC_SCHEMA = parse_avro_schema_definition(json.dumps({"type": "enum", "name": "Enum1", "symbols": ["B", "C"]}))
ENUM2_AB_SCHEMA = parse_avro_schema_definition(json.dumps({"type": "enum", "name": "Enum2", "symbols": ["A", "B"]}))
# ENUM_ABC_ENUM_DEFAULT_A_SCHEMA = parse_avro_schema_definition(
#     json.dumps(
#         {
#             "type": "enum",
#             "name": "Enum",
#             "symbols": ["A", "B", "C"],
#             "default": "A",
#         }
#     )
# )
# ENUM_AB_ENUM_DEFAULT_A_SCHEMA = parse_avro_schema_definition(
#     json.dumps(
#         {
#             "type": "enum",
#             "name": "Enum",
#             "symbols": ["A", "B"],
#             "default": "A",
#         }
#     )
# )
ENUM_ABC_ENUM_DEFAULT_A_RECORD = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record",
            "fields": [
                {"name": "Field", "type": {"type": "enum", "name": "Enum", "symbols": ["A", "B", "C"], "default": "A"}}
            ],
        }
    )
)
ENUM_AB_ENUM_DEFAULT_A_RECORD = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record",
            "fields": [{"name": "Field", "type": {"type": "enum", "name": "Enum", "symbols": ["A", "B"], "default": "A"}}],
        }
    )
)
ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record",
            "fields": [
                {
                    "name": "Field",
                    "type": {"type": "enum", "name": "Enum", "symbols": ["A", "B", "C"], "default": "A"},
                    "default": "B",
                }
            ],
        }
    )
)
ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record",
            "fields": [
                {
                    "name": "Field",
                    "type": {"type": "enum", "name": "Enum", "symbols": ["A", "B"], "default": "B"},
                    "default": "A",
                }
            ],
        }
    )
)
EMPTY_UNION_SCHEMA = UnionSchema([])
# NULL_UNION_SCHEMA = UnionSchema([NULL_SCHEMA])
INT_UNION_SCHEMA = UnionSchema([INT_SCHEMA])
LONG_UNION_SCHEMA = UnionSchema([LONG_SCHEMA])
FLOAT_UNION_SCHEMA = UnionSchema([FLOAT_SCHEMA])
DOUBLE_UNION_SCHEMA = UnionSchema([DOUBLE_SCHEMA])
STRING_UNION_SCHEMA = UnionSchema([STRING_SCHEMA])
BYTES_UNION_SCHEMA = UnionSchema([BYTES_SCHEMA])
INT_STRING_UNION_SCHEMA = UnionSchema([INT_SCHEMA, STRING_SCHEMA])
STRING_INT_UNION_SCHEMA = UnionSchema([STRING_SCHEMA, INT_SCHEMA])
INT_FLOAT_UNION_SCHEMA = UnionSchema([INT_SCHEMA, FLOAT_SCHEMA])
INT_LONG_UNION_SCHEMA = UnionSchema([INT_SCHEMA, LONG_SCHEMA])
INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA = UnionSchema([INT_SCHEMA, LONG_SCHEMA, FLOAT_SCHEMA, DOUBLE_SCHEMA])
# NULL_INT_ARRAY_UNION_SCHEMA = UnionSchema([NULL_SCHEMA, INT_ARRAY_SCHEMA])
# NULL_INT_MAP_UNION_SCHEMA = UnionSchema([NULL_SCHEMA, INT_MAP_SCHEMA])
EMPTY_RECORD1 = parse_avro_schema_definition(json.dumps({"type": "record", "name": "Record1", "fields": []}))
EMPTY_RECORD2 = parse_avro_schema_definition(json.dumps({"type": "record", "name": "Record2", "fields": []}))
A_INT_RECORD1 = parse_avro_schema_definition(
    json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "a", "type": "int"}]})
)
A_LONG_RECORD1 = parse_avro_schema_definition(
    json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "a", "type": "long"}]})
)
A_INT_B_INT_RECORD1 = parse_avro_schema_definition(
    json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}]})
)
A_DINT_RECORD1 = parse_avro_schema_definition(
    json.dumps({"type": "record", "name": "Record1", "fields": [{"name": "a", "type": "int", "default": 0}]})
)
A_INT_B_DINT_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "int", "default": 0}],
        }
    )
)
A_DINT_B_DINT_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [{"name": "a", "type": "int", "default": 0}, {"name": "b", "type": "int", "default": 0}],
        }
    )
)
A_DINT_B_DFIXED_4_BYTES_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [
                {"name": "a", "type": "int", "default": 0},
                {"name": "b", "type": {"type": "fixed", "name": "Fixed", "size": 4}},
            ],
        }
    )
)
A_DINT_B_DFIXED_8_BYTES_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [
                {"name": "a", "type": "int", "default": 0},
                {"name": "b", "type": {"type": "fixed", "name": "Fixed", "size": 8}},
            ],
        }
    )
)
A_DINT_B_DINT_STRING_UNION_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [{"name": "a", "type": "int", "default": 0}, {"name": "b", "type": ["int", "string"], "default": 0}],
        }
    )
)
A_DINT_B_DINT_UNION_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [{"name": "a", "type": "int", "default": 0}, {"name": "b", "type": ["int"], "default": 0}],
        }
    )
)
A_DINT_B_DENUM_1_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [
                {"name": "a", "type": "int", "default": 0},
                {"name": "b", "type": {"type": "enum", "name": "Enum1", "symbols": ["A", "B"]}},
            ],
        }
    )
)
A_DINT_B_DENUM_2_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [
                {"name": "a", "type": "int", "default": 0},
                {"name": "b", "type": {"type": "enum", "name": "Enum2", "symbols": ["A", "B"]}},
            ],
        }
    )
)
FIXED_4_BYTES = parse_avro_schema_definition(json.dumps({"type": "fixed", "name": "Fixed", "size": 4}))
FIXED_8_BYTES = parse_avro_schema_definition(json.dumps({"type": "fixed", "name": "Fixed", "size": 8}))
NS_RECORD1 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [
                {
                    "name": "f1",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "InnerRecord1",
                                "namespace": "ns1",
                                "fields": [{"name": "a", "type": "int"}],
                            },
                        },
                    ],
                }
            ],
        }
    )
)
NS_RECORD2 = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [
                {
                    "name": "f1",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "InnerRecord1",
                                "namespace": "ns2",
                                "fields": [{"name": "a", "type": "int"}],
                            },
                        },
                    ],
                }
            ],
        }
    )
)
INT_LIST_RECORD = parse_avro_schema_definition(
    json.dumps({"type": "record", "name": "List", "fields": [{"name": "head", "type": "int"}]})
)
LONG_LIST_RECORD = parse_avro_schema_definition(
    json.dumps({"type": "record", "name": "List", "fields": [{"name": "head", "type": "long"}]})
)
int_reader_field = Field(name="tail", type=INT_LIST_RECORD, index=1, has_default=False)
long_reader_field = Field(name="tail", type=LONG_LIST_RECORD, index=1, has_default=False)

INT_LIST_RECORD._fields = (INT_LIST_RECORD.fields[0], int_reader_field)
LONG_LIST_RECORD._fields = (LONG_LIST_RECORD.fields[0], long_reader_field)

# pylint: disable=protected-access
INT_LIST_RECORD._field_map = RecordSchema._MakeFieldMap(INT_LIST_RECORD._fields)
LONG_LIST_RECORD._field_map = RecordSchema._MakeFieldMap(LONG_LIST_RECORD._fields)
INT_LIST_RECORD._props["fields"] = INT_LIST_RECORD._fields
LONG_LIST_RECORD._props["fields"] = LONG_LIST_RECORD._fields
# pylint: enable=protected-access
RECORD1_WITH_INT = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record1",
            "fields": [{"name": "field1", "type": "int"}],
        }
    )
)
RECORD2_WITH_INT = parse_avro_schema_definition(
    json.dumps(
        {
            "type": "record",
            "name": "Record2",
            "fields": [{"name": "field1", "type": "int"}],
        }
    )
)
UNION_INT_RECORD1 = UnionSchema([INT_SCHEMA, RECORD1_WITH_INT])
UNION_INT_RECORD2 = UnionSchema([INT_SCHEMA, RECORD2_WITH_INT])
UNION_INT_ENUM1_AB = UnionSchema([INT_SCHEMA, ENUM1_AB_SCHEMA])
UNION_INT_FIXED_4_BYTES = UnionSchema([INT_SCHEMA, FIXED_4_BYTES])
UNION_INT_BOOLEAN = UnionSchema([INT_SCHEMA, BOOLEAN_SCHEMA])
UNION_INT_ARRAY_INT = UnionSchema([INT_SCHEMA, INT_ARRAY_SCHEMA])
UNION_INT_MAP_INT = UnionSchema([INT_SCHEMA, INT_MAP_SCHEMA])
UNION_INT_NULL = UnionSchema([INT_SCHEMA, NULL_SCHEMA])
FIXED_4_ANOTHER_NAME = parse_avro_schema_definition(json.dumps({"type": "fixed", "name": "AnotherName", "size": 4}))
RECORD1_WITH_ENUM_AB = parse_avro_schema_definition(
    json.dumps(
        {"type": "record", "name": "Record1", "fields": [{"name": "field1", "type": dict(ENUM1_AB_SCHEMA.to_json())}]}
    )
)
RECORD1_WITH_ENUM_ABC = parse_avro_schema_definition(
    json.dumps(
        {"type": "record", "name": "Record1", "fields": [{"name": "field1", "type": dict(ENUM1_ABC_SCHEMA.to_json())}]}
    )
)
