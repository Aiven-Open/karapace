"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.core.client import Client
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema
from karapace.core.serialization import SchemaRegistryClient
from karapace.core.typing import Version
from tests.utils import new_random_name, schema_avro_json


async def test_remote_client(registry_async_client: Client) -> None:
    schema_avro = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client
    subject = new_random_name("subject")
    sc_id = await reg_cli.post_new_schema(subject, schema_avro)
    assert sc_id >= 0
    stored_schema, _ = await reg_cli.get_schema_for_id(sc_id)
    assert stored_schema == schema_avro, f"stored schema {stored_schema.to_dict()} is not {schema_avro.to_dict()}"
    stored_id, stored_schema, stored_schema_version = await reg_cli.get_schema(subject)
    assert stored_id == sc_id
    assert stored_schema == schema_avro
    assert stored_schema_version == Version(1)


async def test_remote_client_tls(registry_async_client_tls: Client, server_ca: str) -> None:
    schema_avro = ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)
    reg_cli = SchemaRegistryClient(server_ca=server_ca)
    reg_cli.client = registry_async_client_tls
    subject = new_random_name("subject")
    sc_id = await reg_cli.post_new_schema(subject, schema_avro)
    assert sc_id >= 0
    stored_schema, _ = await reg_cli.get_schema_for_id(sc_id)
    assert stored_schema == schema_avro, f"stored schema {stored_schema.to_dict()} is not {schema_avro.to_dict()}"
    stored_id, stored_schema, stored_schema_version = await reg_cli.get_schema(subject)
    assert stored_id == sc_id
    assert stored_schema == schema_avro
    assert stored_schema_version == Version(1)
