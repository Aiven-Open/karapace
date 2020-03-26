import karapace.serialization


def test_basic_serialization(default_config_path, mock_registry_client):
    serializer = karapace.serialization.SchemaRegistryValueSerializer(
        config_path=default_config_path, registry_client=mock_registry_client)
    deserializer = karapace.serialization.SchemaRegistryValueDeserializer(
        config_path=default_config_path, registry_client=mock_registry_client)
    objects = [
        {"name": "First Foo", "favorite_number": 2, "favorite_color": "bar"},
        {"name": "Second Foo", "favorite_number": 3, "favorite_color": "baz"},
        {"name": "Third Foo", "favorite_number": 5, "favorite_color": "quux"},
    ]
    for o in objects:
        assert o == deserializer.deserialize("top", serializer.serialize("top", o))
    serializer.registry_client.get_latest_schema.assert_called_with("top-value")
    for o in serializer, deserializer:
        assert len(o.ids_to_schemas) == 1
        assert 1 in o.ids_to_schemas
        assert len(o.subjects_to_schemas) == 1
        assert "top-value" in o.subjects_to_schemas
