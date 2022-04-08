"""
karapace - test schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.client import Client
from karapace.config import set_config_defaults
from karapace.schema_backup import SchemaBackup
from karapace.utils import Expiration
from pathlib import Path
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import new_random_name

import os
import time
import ujson

baseurl = "http://localhost:8081"


async def insert_data(client: Client) -> str:
    subject = new_random_name("subject")
    res = await client.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    return subject


async def test_backup_get(registry_async_client, kafka_servers: KafkaServers, tmp_path: Path):
    await insert_data(registry_async_client)

    # Get the backup
    backup_location = tmp_path / "schemas.log"
    config = set_config_defaults({"bootstrap_uri": kafka_servers.bootstrap_servers})
    sb = SchemaBackup(config, str(backup_location))
    sb.request_backup()

    # The backup file has been created
    assert os.path.exists(backup_location)


async def test_backup_restore(
    registry_async_client: Client,
    kafka_servers: KafkaServers,
    tmp_path: Path,
    registry_cluster: RegistryDescription,
) -> None:
    subject = new_random_name("subject")
    restore_location = tmp_path / "restore.log"

    with restore_location.open("w") as fp:
        ujson.dump(
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
            """
[
    [
        {{
            "subject": "{subject_value}",
            "magic": 0,
            "keytype": "CONFIG"
        }},
        null
    ]
]
        """.format(
                subject_value=subject
            )
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
            """
[
    [
        {{
            "subject": "{subject_value}",
            "magic": 1,
            "keytype": "SCHEMA",
            "version": 2
        }},
        null
    ]
]
        """.format(
                subject_value=subject
            )
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
            """
[
    [
        {{
            "subject": "{subject_value}",
            "magic": 1,
            "keytype": "SCHEMA",
            "version": 2
        }},
        null
    ]
]
        """.format(
                subject_value=subject
            )
        )
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status_code == 200
    assert res.json() == [1]
