"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from dataclasses import dataclass, field
from locust import FastHttpUser, task
from locust.contrib.fasthttp import ResponseContextManager

import json
import os
import random
import uuid

SchemaId = int


@dataclass
class TestData:
    count: int = 0
    schemas: dict[uuid.UUID, SchemaId] = field(default_factory=dict)


SUBJECTS = ["test-subject-1", "test-subject-2"]
SUBJECT_TEST_DATA = {subject: TestData() for subject in SUBJECTS}

SCHEMA_REGISTRY_HEADERS = {
    "Content-Type": "application/vnd.schemaregistry.v1+json",
    "Accept": "application/vnd.schemaregistry.v1+json, application/json, */*",
}
OIDC_TOKEN = os.environ.get("SCHEMA_REGISTRY_OIDC_TOKEN")
if OIDC_TOKEN:
    SCHEMA_REGISTRY_HEADERS["Authorization"] = f"Bearer {OIDC_TOKEN}"

SCHEMA_REGISTER_NEW_WEIGHT = int(os.environ.get("SCHEMA_REGISTER_NEW_WEIGHT", "10"))
SCHEMA_CHECK_REGISTERED_WEIGHT = int(os.environ.get("SCHEMA_CHECK_REGISTERED_WEIGHT", "10"))

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
    @task(SCHEMA_REGISTER_NEW_WEIGHT)
    def post_new_schema(self) -> None:
        added = False
        for subject in SUBJECTS:
            test_data = SUBJECT_TEST_DATA[subject]
            if test_data.count < SCHEMAS_PER_SUBJECT:
                test_data.count += 1
                alias = str(uuid.uuid4())
                new_schema = _get_new_schema(alias)
                path = f"/subjects/{subject}/versions"
                with self.client.post(
                    path,
                    headers=SCHEMA_REGISTRY_HEADERS,
                    json=new_schema,
                    name="/subjects/[subject]/versions NEW",
                    catch_response=True,
                ) as response:
                    try:
                        payload = response.json()
                    except Exception as exc:
                        response.failure(f"Schema registration did not return valid JSON: {exc}; body={response.text!r}")
                        continue
                    if response.status_code >= 400:
                        response.failure(f"Schema registration failed with HTTP {response.status_code}: {payload}")
                        continue
                    if "id" not in payload:
                        response.failure(f"Schema registration response missing id: {payload}")
                        continue

                    schema_id = int(payload["id"])
                    test_data.schemas[alias] = schema_id
                    added = True
        if not added:
            subjects_with_schemas = [subject for subject, test_data in SUBJECT_TEST_DATA.items() if test_data.schemas]
            if not subjects_with_schemas:
                return

            subject = random.choice(subjects_with_schemas)
            test_data = SUBJECT_TEST_DATA[subject]
            alias = random.choice(list(test_data.schemas.keys()))
            new_schema = _get_new_schema(alias)
            path = f"/subjects/{subject}/versions"
            self.client.post(
                path, headers=SCHEMA_REGISTRY_HEADERS, json=new_schema, name="/subjects/[subject]/versions EXISTING"
            )

    @task(SCHEMA_CHECK_REGISTERED_WEIGHT)
    def check_schema_registered(self) -> None:
        subject = random.choice(SUBJECTS)
        test_data = SUBJECT_TEST_DATA.get(subject, None)
        if test_data is None or not test_data.schemas:
            return

        alias = random.choice(list(test_data.schemas.keys()))
        new_schema = _get_new_schema(alias)
        path = f"/subjects/{subject}"
        with self.client.post(
            path,
            headers=SCHEMA_REGISTRY_HEADERS,
            json=new_schema,
            name="/subjects/[subject] CHECK_REGISTERED",
            catch_response=True,
        ) as response:
            response: ResponseContextManager
            try:
                payload = response.json()
            except Exception as exc:
                response.failure(f"Schema lookup did not return valid JSON: {exc}; body={response.text!r}")
                return
            if response.status_code >= 400:
                response.failure(f"Schema lookup failed with HTTP {response.status_code}: {payload}")
                return
            if "id" not in payload:
                response.failure(f"Schema lookup response missing id: {payload}")
                return

            schema_id = int(payload["id"])
            expected_id = test_data.schemas.get(alias)
            if expected_id != schema_id:
                response.failure(f"Schema with alias {alias} has ID {expected_id}, check returned {schema_id}")
