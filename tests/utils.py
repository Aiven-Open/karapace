import json

schema_json = json.dumps({
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [{
        "name": "name",
        "type": "string"
    }, {
        "name": "favorite_number",
        "type": ["int", "null"]
    }, {
        "name": "favorite_color",
        "type": ["string", "null"]
    }]
})

test_objects = [
    {
        "name": "First Foo",
        "favorite_number": 2,
        "favorite_color": "bar"
    },
    {
        "name": "Second Foo",
        "favorite_number": 3,
        "favorite_color": "baz"
    },
    {
        "name": "Third Foo",
        "favorite_number": 5,
        "favorite_color": "quux"
    },
]
