"""
karapace - test schema backup

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import base64
import json
import os
from pathlib import Path
from typing import Any

from karapace.backup import api
from karapace.backup.api import BackupVersion
from karapace.core.client import Client
from karapace.core.config import Config
from karapace.core.utils import json_encode
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers

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
EXPECTED_AVRO_SCHEMA = json_encode(
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
    compact=True,
    sort_keys=True,
)
AVRO_DELETE_SUBJECT = {
    "subject": AVRO_SUBJECT,
    "version": 1,
}
EXPECTED_AVRO_DELETE_SUBJECT = {
    "subject": "a801beafef1fb8c03907b44ec7baca341a58420d-",
    "version": 1,
}

COMPATIBILITY_SUBJECT = "compatibility_subject"
COMPATIBILITY_SUBJECT_HASH = "a0765805d57daad3b08200d5be1c5adb6f3cff54"
COMPATIBILITY_CHANGE = {"compatibility": "NONE"}
EXPECTED_COMPATIBILITY_CHANGE = {"compatibilityLevel": "NONE"}


async def insert_data(c: Client, schemaType: str, subject: str, data: dict[str, Any]) -> None:
    schema_string = json.dumps(data)
    res = await c.post(
        f"subjects/{subject}/versions",
        json={"schema": f"{schema_string}", "schemaType": schemaType},
    )
    assert res.status_code == 200
    assert "id" in res.json()


async def insert_compatibility_level_change(c: Client, subject: str, data: dict[str, Any]) -> None:
    res = await c.put(
        f"config/{subject}",
        json=data,
    )
    assert res.status_code == 200


async def insert_delete_subject(c: Client, subject: str) -> None:
    res = await c.delete(
        f"subjects/{subject}",
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
    await insert_delete_subject(registry_async_client, AVRO_SUBJECT)

    # Get the backup
    export_location = tmp_path / "export.log"
    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.topic_name = registry_cluster.schemas_topic
    api.create_backup(
        config=config,
        backup_location=export_location,
        topic_name=api.normalize_topic_name(None, config),
        version=BackupVersion.ANONYMIZE_AVRO,
    )

    # The export file has been created
    assert os.path.exists(export_location)

    compatibility_level_change_subject_hash_found = False
    with export_location.open("r") as fp:
        version_identifier = fp.readline()
        assert version_identifier.rstrip() == "/V2"
        for item in fp:
            hex_key, hex_value = item.strip().split("\t")
            key = json.loads(base64.b16decode(hex_key).decode("utf8"))
            value_data = json.loads(base64.b16decode(hex_value).decode("utf8"))
            if key["keytype"] == "SCHEMA":
                assert key["subject"] == AVRO_SUBJECT_HASH
                assert value_data["subject"] == AVRO_SUBJECT_HASH
                assert value_data["schema"] == EXPECTED_AVRO_SCHEMA
            if key["keytype"] == "DELETE_SUBJECT":
                assert key["subject"] == AVRO_SUBJECT_HASH
                assert value_data["subject"] == AVRO_SUBJECT_HASH
            if key["keytype"] == "CONFIG":
                compatibility_level_change_subject_hash_found = True
                assert key["subject"] == COMPATIBILITY_SUBJECT_HASH
                assert value_data == EXPECTED_COMPATIBILITY_CHANGE

    assert compatibility_level_change_subject_hash_found
