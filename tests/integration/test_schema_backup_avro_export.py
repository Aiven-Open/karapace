"""
karapace - test schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.config import set_config_defaults
from karapace.schema_backup import anonymize_avro_schema_message, SchemaBackup
from pathlib import Path
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from typing import Any, Dict

import base64
import json
import os

baseurl = "http://localhost:8081"


JSON_SUBJECT = "json-schemas"
JSON_SUBJECT_HASH = "a2a0483c6ce0d38798ef218420e3f132608dbebf-"
JSON_SCHEMA = {
    "type": "object",
    "title": "JSON-schema",
    "description": "example",
    "properties": {"test": {"type": "integer", "title": "my test number", "default": 5}},
}

AVRO_SUBJECT = "avro-schemas"
AVRO_SUBJECT_HASH = "a801beafef1fb8c03907b44ec7baca341a58420d-"
AVRO_SCHEMA = {
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
EXPECTED_AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "namespace": "aa258230180d9c643f761089d7e33b8b52288ed3.ae02f26b082c5f3bc7027f72335dd1186a2cd382",
        "name": "afe8733e983101f1f4ff50d24152890d0da71418",
        "fields": [
            {
                "type": "string",
                "name": "a09bb890b096f7306f688cc6d1dad34e7e52a223",
            },
        ],
    },
    sort_keys=False,
)

COMPATIBILITY_SUBJECT = "compatibility_subject"
COMPATIBILITY_SUBJECT_HASH = "a0765805d57daad3b08200d5be1c5adb6f3cff54"
COMPATIBILITY_CHANGE = {"compatibility": "NONE"}
EXPECTED_COMPATIBILITY_CHANGE = {"compatibilityLevel": "NONE"}


async def insert_data(c: Client, schemaType: str, subject: str, data: Dict[str, Any]) -> None:
    schema_string = json.dumps(data)
    res = await c.post(
        f"subjects/{subject}/versions",
        json={"schema": f"{schema_string}", "schemaType": schemaType},
    )
    assert res.status_code == 200
    assert "id" in res.json()


async def insert_compatibility_level_change(c: Client, subject: str, data: Dict[str, Any]) -> None:
    res = await c.put(
        f"config/{subject}",
        json=data,
    )
    assert res.status_code == 200


async def test_export_anonymized_avro_schemas(
    registry_async_client: Client,
    kafka_servers: KafkaServers,
    tmp_path: Path,
    registry_cluster: RegistryDescription,
) -> None:
    await insert_data(registry_async_client, "AVRO", AVRO_SUBJECT, AVRO_SCHEMA)
    await insert_compatibility_level_change(registry_async_client, COMPATIBILITY_SUBJECT, COMPATIBILITY_CHANGE)

    # Get the backup
    export_location = tmp_path / "export.log"
    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": registry_cluster.schemas_topic,
        }
    )
    sb = SchemaBackup(config, str(export_location))
    sb.export(anonymize_avro_schema_message)

    # The export file has been created
    assert os.path.exists(export_location)

    expected_subject_hash_found = False
    compatibility_level_change_subject_hash_found = False
    with export_location.open("r") as fp:
        version_identifier = fp.readline()
        assert version_identifier.rstrip() == "/V2"
        for item in fp:
            hex_key, hex_value = item.strip().split("\t")
            key = json.loads(base64.b16decode(hex_key).decode("utf8"))
            schema_data = json.loads(base64.b16decode(hex_value).decode("utf8"))
            subject_hash = key.get("subject", None)
            if subject_hash == AVRO_SUBJECT_HASH:
                expected_subject_hash_found = True
                assert schema_data["subject"] == AVRO_SUBJECT_HASH
                assert schema_data["schema"] == EXPECTED_AVRO_SCHEMA
            if subject_hash == COMPATIBILITY_SUBJECT_HASH:
                compatibility_level_change_subject_hash_found = True
                assert schema_data == EXPECTED_COMPATIBILITY_CHANGE

    assert expected_subject_hash_found
    assert compatibility_level_change_subject_hash_found
