"""
karapace - test schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from karapace.schema_backup import SchemaBackup

import json as jsonlib
import os
import time

pytest_plugins = "aiohttp.pytest_plugin"
baseurl = "http://localhost:8081"


async def insert_data(c):
    subject = os.urandom(16).hex()
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    assert "id" in res.json()
    return subject


async def test_backup_get(registry_async, registry_async_client):
    _ = await insert_data(registry_async_client)

    # Get the backup
    backup_location = os.path.join(os.path.dirname(registry_async.config_path), "schemas.log")
    sb = SchemaBackup(registry_async.config_path, backup_location)
    sb.request_backup()

    # The backup file has been created
    assert os.path.exists(backup_location)


async def test_backup_restore(registry_async, registry_async_client):

    subject = os.urandom(16).hex()
    restore_location = os.path.join(os.path.dirname(registry_async.config_path), "restore.log")
    with open(restore_location, "w") as fp:
        jsonlib.dump([[
            {
                "subject": subject,
                "version": 1,
                "magic": 1,
                "keytype": "SCHEMA",
            },
            {
                "deleted": False,
                "id": 1,
                "schema": "\"string\"",
                "subject": subject,
                "version": 1,
            },
        ]],
                     fp=fp)
    sb = SchemaBackup(registry_async.config_path, restore_location)
    sb.restore_backup()

    # The restored karapace should have the previously created subject
    time.sleep(1.0)
    res = await registry_async_client.get("subjects")
    assert res.status_code == 200
    data = res.json()
    assert subject in data

    # Test a few exotic scenarios
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "NONE"})
    assert res.status == 200
    assert res.json()["compatibility"] == "NONE"

    # Restore a compatibility config remove message
    with open(restore_location, "w") as fp:
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
        """.format(subject_value=subject)
        )
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status == 200
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"config/{subject}")
    assert res.status == 404

    # Restore a complete schema delete message
    subject = os.urandom(16).hex()
    res = await registry_async_client.put(f"config/{subject}", json={"compatibility": "NONE"})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "int"}'})
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "float"}'})
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status == 200
    assert res.json() == [1, 2]
    with open(restore_location, "w") as fp:
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
        """.format(subject_value=subject)
        )
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status == 200
    assert res.json() == [1]

    # Schema delete for a nonexistent subject version is ignored
    subject = os.urandom(16).hex()
    res = await registry_async_client.post(f"subjects/{subject}/versions", json={"schema": '{"type": "string"}'})
    with open(restore_location, "w") as fp:
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
        """.format(subject_value=subject)
        )
    sb.restore_backup()
    time.sleep(1.0)
    res = await registry_async_client.get(f"subjects/{subject}/versions")
    assert res.status == 200
    assert res.json() == [1]
