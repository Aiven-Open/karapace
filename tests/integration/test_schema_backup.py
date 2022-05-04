"""
karapace - test schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.config import set_config_defaults
from karapace.schema_backup import SchemaBackup, serialize_schema_message
from karapace.utils import Expiration
from pathlib import Path
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import new_random_name

import base64
import json
import os
import time

baseurl = "http://localhost:8081"


async def insert_data(client: Client) -> str:
    subject = new_random_name("subject")
    res = await client.post(
        f"subjects/{subject}/versions",
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    return subject


async def test_backup_get(
    registry_async_client,
    kafka_servers: KafkaServers,
    tmp_path: Path,
    registry_cluster: RegistryDescription,
) -> None:
    await insert_data(registry_async_client)

    # Get the backup
    backup_location = tmp_path / "schemas.log"
    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": registry_cluster.schemas_topic,
        }
    )
    sb = SchemaBackup(config, str(backup_location))
    sb.export(serialize_schema_message)

    # The backup file has been created
    assert os.path.exists(backup_location)

    lines = 0
    with open(backup_location, "r", encoding="utf8") as fp:
        for line in fp:
            lines += 1
            data = line.split("\t")
            assert len(data) == 2

    assert lines == 1


async def test_backup_restore(
    registry_async_client: Client,
    kafka_servers: KafkaServers,
    tmp_path: Path,
    registry_cluster: RegistryDescription,
) -> None:
    subject = new_random_name("subject")
    restore_location = tmp_path / "restore.log"

    with restore_location.open("w") as fp:
        json.dump(
            [
                [
                    {
                        "subject": subject,
                        "version": 1,
                        "magic": 1,
                        "keytype": "SCHEMA",
                    },
                    {
                        "deleted": False,
                        "id": 1,
                        "schema": '"string"',
                        "subject": subject,
                        "version": 1,
                    },
                ]
            ],
            fp,
        )

    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": registry_cluster.schemas_topic,
        }
    )
    sb = SchemaBackup(config, str(restore_location))
    sb.restore_backup()

    # The restored karapace should have the previously created subject
    all_subjects = []
    expiration = Expiration.from_timeout(timeout=10)
    while subject not in all_subjects:
        expiration.raise_timeout_if_expired(
            msg_format="{subject} not in {all_subjects}",
            subject=subject,
            all_subjects=all_subjects,
        )
        res = await registry_async_client.get("subjects")
        assert res.status_code == 200
        all_subjects = res.json()
        time.sleep(0.1)

    # Test a few exotic scenarios
    subject = new_random_name("subject")
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "NONE"})
    assert res.status_code == 200
    assert res.json()["compatibility"] == "NONE"

    # Restore a compatibility config remove message
    with open(restore_location, mode="w", encoding="utf8") as fp:
        fp.write(
            f"""
[
    [
        {{
            "subject": "{subject}",
            "magic": 0,
            "keytype": "CONFIG"
        }},
        null
    ]
]
        """
        )
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status_code == 200
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status_code == 404

    # Restore a complete schema delete message
    subject = new_random_name("subject")
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "NONE"})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "int"}'})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "float"}'})
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1, 2]
    with open(restore_location, mode="w", encoding="utf8") as fp:
        fp.write(
            f"""
[
    [
        {{
            "subject": "{subject}",
            "magic": 1,
            "keytype": "SCHEMA",
            "version": 2
        }},
        null
    ]
]
        """
        )
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1]

    # Schema delete for a nonexistent subject version is ignored
    subject = new_random_name("subject")
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "string"}'})
    with open(restore_location, mode="w", encoding="utf8") as fp:
        fp.write(
            f"""
[
    [
        {{
            "subject": "{subject}",
            "magic": 1,
            "keytype": "SCHEMA",
            "version": 2
        }},
        null
    ]
]
        """
        )
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1]


async def test_backup_restore_from_jsonlines_file(
    registry_async_client: Client,
    kafka_servers: KafkaServers,
    tmp_path: Path,
    registry_cluster: RegistryDescription,
) -> None:
    config = set_config_defaults(
        {
            "bootstrap_uri": kafka_servers.bootstrap_servers,
            "topic_name": registry_cluster.schemas_topic,
        }
    )
    restore_location = tmp_path / "restore.log"
    sb = SchemaBackup(config, str(restore_location))

    subject = new_random_name("subject")
    schema_0 = {"schema": """{"type": "record", "name": "testrecord", "fields": [{"type": "int", "name": "ti"}]}"""}
    res = await registry_async_client.post(f"subjects/{subject}/versions", json=schema_0)
    schema_id = res.json()["id"]
    with open(restore_location, mode="wb") as fp:
        key_1 = json.dumps({"subject": f"{subject}", "magic": 1, "keytype": "SCHEMA", "version": 1}, indent=4)
        schema_1 = json.dumps(
            {
                "schema": """{"type": "record", "name": "testrecord", "fields": [{"type": "int", "name": "ti"}]}""",
                "version": 1,
                "id": schema_id,
                "deleted": False,
                "subject": subject,
            },
            indent=4,
        )
        fp.write(base64.b16encode(key_1.encode("utf8")))
        fp.write("\t".encode("utf8"))
        fp.write(base64.b16encode(schema_1.encode("utf8")))
        fp.write("\n".encode("utf8"))

        key_2 = json.dumps({"subject": f"{subject}", "magic": 1, "keytype": "SCHEMA", "version": 2}, indent=4)
        schema_2 = json.dumps(
            {
                "schema": """{"type": "record", "name": "testrecord", """
                """"fields": [{"type": "int", "name": "ti"}, {"type": "long", "name": "tl"}, """
                """{"type": "string", "name": "ts"}]}""",
                "version": 2,
                "id": schema_id,
                "deleted": False,
                "subject": subject,
            },
            indent=4,
        )
        fp.write(base64.b16encode(key_2.encode("utf8")))
        fp.write("\t".encode("utf8"))
        fp.write(base64.b16encode(schema_2.encode("utf8")))
        fp.write("\n".encode("utf8"))
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1, 2]
