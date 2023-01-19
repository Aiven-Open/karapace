"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from locust import FastHttpUser, task

import json
import random
import uuid

SUBJECTS = ["test-subject-1", "test-subject-2"]
UUIDS = {
    "test-subject-1": {"count": 0, "uuids": []},
    "test-subject-2": {"count": 0, "uuids": []},
}


def _get_new_schema(alias: uuid.UUID):
    return {
        "schema": json.dumps(
            {
                "type": "record",
                "name": "CpuUsage",
                "alias": alias,
                "fields": [
                    {"name": "pct", "type": "int"},
                ],
            }
        )
    }


class NewSchemaCreateUser(FastHttpUser):
    @task
    def post_new_schema(self) -> None:
        added = False
        for subject in SUBJECTS:
            if UUIDS[subject]["count"] < 2000:
                alias = str(uuid.uuid4())
                UUIDS[subject]["uuids"].append(alias)
                UUIDS[subject]["count"] = UUIDS[subject]["count"] + 1
                new_schema = _get_new_schema(alias)
                path = f"/subjects/{subject}/versions?NEW"
                self.client.post(path, json=new_schema)
                added = True
        if not added:
            subject = random.choice(SUBJECTS)
            alias = random.choice(UUIDS[subject]["uuids"])
            new_schema = _get_new_schema(alias)
            path = f"/subjects/{subject}/versions?EXISTING"
            self.client.post(path, json=new_schema)
