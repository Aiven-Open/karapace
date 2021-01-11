from karapace.schema_reader import SchemaType, TypedSchema
from tests.utils import schema_avro_json

import pytest


class MockClient:
    # pylint: disable=W0613
    def __init__(self, *args, **kwargs):
        pass

    async def get_schema_for_id(self, *args, **kwargs):
        return TypedSchema.parse(SchemaType.AVRO, schema_avro_json)

    async def get_latest_schema(self, *args, **kwargs):
        return 1, TypedSchema.parse(SchemaType.AVRO, schema_avro_json)

    async def post_new_schema(self, *args, **kwargs):
        return 1


@pytest.fixture(name="mock_registry_client")
def create_basic_registry_client() -> MockClient:
    return MockClient()
