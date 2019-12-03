"""
karapace - test schema backup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from .utils import Client
from karapace.karapace import Karapace
from karapace.schema_backup import SchemaBackup

import json as jsonlib
import os
import time

pytest_plugins = "aiohttp.pytest_plugin"
baseurl = "http://localhost:8081"


def create_karapace(config_path, topic_name, kafka_server):
    with open(config_path, "w") as fp:
        karapace_config = {
            "log_level": "DEBUG",
            "bootstrap_uri": "127.0.0.1:{}".format(kafka_server["kafka_port"]),
            "topic_name": topic_name,
        }
        fp.write(jsonlib.dumps(karapace_config))
    return Karapace(config_path)


async def insert_data(c):
    subject = os.urandom(16).hex()
    res = await c.post(
        "subjects/{}/versions".format(subject),
        json={"schema": '{"type": "string"}'},
    )
    assert res.status == 200
    assert "id" in res.json()
    return subject


async def test_backup_get(karapace, aiohttp_client):
    kc, datadir = karapace(topic_name="get_schemas")
    client = await aiohttp_client(kc.app)
    c = Client(client=client)

    _ = await insert_data(c)

    # Get the backup
    backup_location = os.path.join(datadir, "schemas.log")
    sb = SchemaBackup(kc.config_path, backup_location)
    sb.request_backup()

    # The backup file has been created
    assert os.path.exists(backup_location)


async def test_backup_restore(karapace, aiohttp_client):
    kc, datadir = karapace(topic_name="restore_schemas")
    client = await aiohttp_client(kc.app)
    c = Client(client=client)

    subject = os.urandom(16).hex()
    restore_location = os.path.join(datadir, "restore.log")
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
    sb = SchemaBackup(kc.config_path, restore_location)
    sb.restore_backup()

    # The restored karapace should have the previously created subject
    time.sleep(1.0)
    res = await c.get("subjects")
    assert res.status_code == 200
    data = res.json()
    assert subject in data
