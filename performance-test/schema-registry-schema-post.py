"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass, field
from locust import FastHttpUser, task
from locust.contrib.fasthttp import ResponseContextManager
from typing import Dict

import json
import random
import uuid

SchemaId = int


@dataclass
class TestData:
    count: int = 0
    schemas: Dict[uuid.UUID, SchemaId] = field(default_factory=dict)


SUBJECTS = ["test-subject-1", "test-subject-2"]
SUBJECT_TEST_DATA = {subject: TestData() for subject in SUBJECTS}

NO_SCHEMAS_TO_REGISTER = 4000
SCHEMAS_PER_SUBJECT = NO_SCHEMAS_TO_REGISTER / len(SUBJECTS)


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
            test_data = SUBJECT_TEST_DATA[subject]
            if test_data.count < SCHEMAS_PER_SUBJECT:
                test_data.count += 1
                alias = str(uuid.uuid4())
                new_schema = _get_new_schema(alias)
                path = f"/subjects/{subject}/versions?NEW"
                with self.client.post(path, json=new_schema, catch_response=True) as response:
                    schema_id = int(response.json()["id"])
                    test_data.schemas[alias] = schema_id
                added = True
        if not added:
            subject = random.choice(SUBJECTS)
            test_data = SUBJECT_TEST_DATA[subject]
            alias = random.choice(list(test_data.schemas.keys()))
            new_schema = _get_new_schema(alias)
            path = f"/subjects/{subject}/versions?EXISTING"
            self.client.post(path, json=new_schema)

    @task
    def check_schema_registered(self) -> None:
        subject = random.choice(SUBJECTS)
        test_data = SUBJECT_TEST_DATA.get(subject, None)
        if test_data is None:
            return

        alias = random.choice(list(test_data.schemas.keys()))
        new_schema = _get_new_schema(alias)
        path = f"/subjects/{subject}?CHECK_REGISTERED"
        with self.client.post(path, json=new_schema, catch_response=True) as response:
            response: ResponseContextManager
            schema_id = int(response.json()["id"])
            expected_id = test_data.schemas.get(alias)
            if expected_id != schema_id:
                response.failure(f"Schema with alias {alias} has ID {expected_id}, check returned {schema_id}")
