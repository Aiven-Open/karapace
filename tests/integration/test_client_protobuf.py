from karapace.schema_reader import SchemaType, TypedSchema
from karapace.serialization import SchemaRegistryClient
from tests.schemas.protobuf import schema_protobuf_plain
from tests.utils import new_random_name


async def test_remote_client_protobuf(registry_async_client):
    schema_protobuf = TypedSchema.parse(SchemaType.PROTOBUF, schema_protobuf_plain)
    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client
    subject = new_random_name("subject")
    sc_id = await reg_cli.post_new_schema(subject, schema_protobuf)
    assert sc_id >= 0
    stored_schema = await reg_cli.get_schema_for_id(sc_id)
    assert stored_schema == schema_protobuf, f"stored schema {stored_schema} is not {schema_protobuf}"
    stored_id, stored_schema = await reg_cli.get_latest_schema(subject)
    assert stored_id == sc_id
    assert stored_schema == schema_protobuf