from karapace.schema_reader import SchemaType, TypedSchema
from karapace.serialization import SchemaRegistryClient
from tests.utils import new_random_name, schema_avro_json


async def test_remote_client(registry_async_client):
    schema_avro = TypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client
    subject = new_random_name("subject")
    sc_id = await reg_cli.post_new_schema(subject, schema_avro)
    assert sc_id >= 0
    stored_schema = await reg_cli.get_schema_for_id(sc_id)
    assert stored_schema == schema_avro, f"stored schema {stored_schema.to_json()} is not {schema_avro.to_json()}"
    stored_id, stored_schema = await reg_cli.get_latest_schema(subject)
    assert stored_id == sc_id
    assert stored_schema == schema_avro



