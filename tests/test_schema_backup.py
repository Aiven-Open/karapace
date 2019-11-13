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
            "log_level": "INFO",
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


async def test_backup_get_and_restore(session_tmpdir, kafka_server, aiohttp_client):
    datadir = session_tmpdir()

    config_location = os.path.join(str(datadir), "karapace_config.json")
    kc = create_karapace(config_location, "get_schemas", kafka_server)
    client = await aiohttp_client(kc.app)
    c = Client(client=client)

    subject = await insert_data(c)

    # Get the backup
    backup_location = os.path.join(str(datadir), "schemas.log")
    sb = SchemaBackup(config_location, backup_location)
    sb.request_backup()

    # The backup file has been created
    assert os.path.exists(backup_location)
    kc.close()

    # Restore the backup to a different topic
    restore_config_location = os.path.join(str(datadir), "restore_karapace_config.json")
    kc2 = create_karapace(restore_config_location, "restore_schemas", kafka_server)
    client2 = await aiohttp_client(kc2.app)
    c2 = Client(client=client2)

    sb2 = SchemaBackup(restore_config_location, backup_location)
    sb2.restore_backup()

    # The restored karapace should have the previously created subject
    time.sleep(1.0)
    res = await c2.get("subjects")
    assert res.status_code == 200
    assert subject in res.json()

    kc2.close()
