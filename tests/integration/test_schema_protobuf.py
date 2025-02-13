"""
karapace - schema tests

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

import pytest

from karapace.core.client import Client
from karapace.core.config import Config
from karapace.core.errors import InvalidTest
from karapace.core.protobuf.kotlin_wrapper import trim_margin
from karapace.core.schema_type import SchemaType
from karapace.core.typing import JsonData, SchemaMetadata, SchemaRuleSet
from tests.base_testcase import BaseTestCase
from tests.integration.utils.cluster import after_master_is_available, start_schema_registry_cluster
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import create_subject_name_factory

baseurl = "http://localhost:8081"


def add_slashes(text: str) -> str:
    escape_dict = {
        "\a": "\\a",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
        "\v": "\\v",
        "'": "\\'",
        '"': '\\"',
        "\\": "\\\\",
    }
    trans_table = str.maketrans(escape_dict)
    return text.translate(trans_table)


log = logging.getLogger(__name__)


# This test ProtoBuf schemas in subject registeration, compatibility of evolved version and querying the schema
# w.r.t. normalization of whitespace and other minor differences to verify equality and inequality comparison of such schemas
@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_schema_normalization(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_schema_compatibility")()

    res = await registry_async_client.put_config_subject(subject=subject, json={"compatibility": "BACKWARD"})
    assert res.status_code == 200

    original_schema = """
            |syntax = "proto3";
            |package a1;
            |message TestMessage {
            |    message Value {
            |        string str2 = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |}
            |"""

    original_schema = trim_margin(original_schema)

    # Same schema with different whitespaces to see API equality comparison works
    original_schema_with_whitespace = trim_margin(
        """
            |syntax = "proto3";
            |
            |package a1;
            |
            |
            |message TestMessage {
            |    message Value {
            |        string str2 = 1;
            |      int32 x = 2;
            |    }
            |  string test = 1;
            |      .a1.TestMessage.Value val = 2;
            |}
            |"""
    )

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    original_id = res.json()["id"]

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id == res.json()["id"], "No duplication"

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": original_schema_with_whitespace}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id == res.json()["id"], "No duplication with whitespace differences"

    res = await registry_async_client.post_subjects(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert "schema" in res.json()
    assert original_id == res.json()["id"], "Check returns original id"

    res = await registry_async_client.post_subjects(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": original_schema_with_whitespace}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert "schema" in res.json()
    assert original_id == res.json()["id"], "Check returns original id"

    evolved_schema = """
            |syntax = "proto3";
            |package a1;
            |message TestMessage {
            |    message Value {
            |        string str2 = 1;
            |        Enu x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |    enum Enu {
            |        A = 0;
            |        B = 1;
            |    }
            |}
            |"""
    evolved_schema = trim_margin(evolved_schema)

    res = await registry_async_client.post_compatibility_subject_version(
        subject=subject,
        version="latest",
        json={"schemaType": "PROTOBUF", "schema": evolved_schema},
    )
    assert res.status_code == 200
    assert res.json() == {"is_compatible": True}

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": evolved_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id != res.json()["id"], "Evolved is not equal"
    evolved_id = res.json()["id"]

    res = await registry_async_client.post_compatibility_subject_version(
        subject=subject,
        version="latest",
        json={"schemaType": "PROTOBUF", "schema": original_schema},
    )
    assert res.json() == {"is_compatible": True}
    assert res.status_code == 200
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": original_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert original_id == res.json()["id"], "Original id again"

    res = await registry_async_client.post_subjects(
        subject=subject, json={"schemaType": "PROTOBUF", "schema": evolved_schema}
    )
    assert res.status_code == 200
    assert "id" in res.json()
    assert "schema" in res.json()
    assert evolved_id == res.json()["id"], "Check returns evolved id"


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_schema_references(registry_async_client: Client) -> None:
    customer_schema = """
                |syntax = "proto3";
                |package a1;
                |import "Place.proto";
                |import "google/protobuf/duration.proto";
                |import "google/type/color.proto";
                |message Customer {
                |        string name = 1;
                |        int32 code = 2;
                |        Place place = 3;
                |        google.protobuf.Duration dur = 4;
                |        google.type.Color color = 5;
                |}
                |"""

    customer_schema = trim_margin(customer_schema)

    place_schema = """
            |syntax = "proto3";
            |package a1;
            |message Place {
            |        string city = 1;
            |        int32 zone = 2;
            |}
            |"""

    place_schema = trim_margin(place_schema)
    res = await registry_async_client.post_subjects_versions(
        subject="place", json={"schemaType": "PROTOBUF", "schema": place_schema}
    )
    assert res.status_code == 200

    assert "id" in res.json()

    customer_references = [{"name": "Place.proto", "subject": "place", "version": 1}]
    res = await registry_async_client.post_subjects_versions(
        subject="customer",
        json={"schemaType": "PROTOBUF", "schema": customer_schema, "references": customer_references},
    )
    assert res.status_code == 200

    assert "id" in res.json()
    customer_id = res.json()["id"]

    # Check if the schema has now been registered under the subject
    res = await registry_async_client.post_subjects(
        subject="customer",
        json={"schemaType": "PROTOBUF", "schema": customer_schema, "references": customer_references},
    )
    assert res.status_code == 200
    assert "subject" in res.json()
    assert "id" in res.json()
    assert customer_id == res.json()["id"]
    assert "version" in res.json()
    assert "schema" in res.json()

    original_schema = """
            |syntax = "proto3";
            |package a1;
            |import "Customer.proto";
            |message TestMessage {
            |    enum Enum {
            |       HIGH = 0;
            |       MIDDLE = 1;
            |       LOW = 2;
            |    }
            |    message Value {
            |        message Label{
            |              int32 Id = 1;
            |              string name = 2;
            |        }
            |        Customer customer = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |    oneof page_info {
            |      option (my_option) = true;
            |      int32 page_number = 3;
            |      int32 result_per_page = 4;
            |    }
            |}
            |"""

    original_schema = trim_margin(original_schema)
    references = [{"name": "Customer.proto", "subject": "customer", "version": 1}]
    res = await registry_async_client.post_subjects_versions(
        subject="test_schema",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": references},
    )
    assert res.status_code == 200

    assert "id" in res.json()

    res = await registry_async_client.get_subjects_subject_version_referenced_by(subject="customer", version="latest")
    assert res.status_code == 200

    myjson = res.json()
    referents = [3]
    assert not any(x != y for x, y in zip(myjson, referents))

    res = await registry_async_client.get_subjects_subject_version_referenced_by(subject="place", version="latest")
    assert res.status_code == 200

    res = await registry_async_client.delete("subjects/customer/versions/1")
    assert res.status_code == 422

    match_msg = "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=customer,version=1}."
    myjson = res.json()
    assert myjson["error_code"] == 42206 and myjson["message"] == match_msg

    res = await registry_async_client.delete_subjects_version(subject="test_schema", version=1)
    assert res.status_code == 200
    res = await registry_async_client.delete_subjects_version(subject="test_schema", version=1)
    myjson = res.json()
    match_msg = "Subject 'test_schema' Version 1 was soft deleted. Set permanent=true to delete permanently"

    assert res.status_code == 404

    assert myjson["error_code"] == 40406 and myjson["message"] == match_msg
    res = await registry_async_client.delete_subjects_version(subject="customer", version=1)
    myjson = res.json()
    match_msg = "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=customer,version=1}."

    assert res.status_code == 422
    assert myjson["error_code"] == 42206 and myjson["message"] == match_msg

    res = await registry_async_client.delete_subjects_version(subject="test_schema", version=1, permanent=True)
    assert res.status_code == 200

    res = await registry_async_client.delete_subjects_version(subject="customer", version=1)
    assert res.status_code == 200


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_schema_jjaakola_one(registry_async_client: Client) -> None:
    no_ref = """
             |syntax = "proto3";
             |
             |message NoReference {
             |    string name = 1;
             |}
             |"""

    no_ref = trim_margin(no_ref)
    res = await registry_async_client.post_subjects_versions(
        subject="sub1", json={"schemaType": "PROTOBUF", "schema": no_ref}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    with_first_ref = """
                |syntax = "proto3";
                |
                |import "NoReference.proto";
                |
                |message WithReference {
                |    string name = 1;
                |    NoReference ref = 2;
                |}"""

    with_first_ref = trim_margin(with_first_ref)
    references = [{"name": "NoReference.proto", "subject": "sub1", "version": 1}]
    res = await registry_async_client.post_subjects_versions(
        subject="sub2",
        json={"schemaType": "PROTOBUF", "schema": with_first_ref, "references": references},
    )
    assert res.status_code == 200
    assert "id" in res.json()

    no_ref_second = """
                    |syntax = "proto3";
                    |
                    |message NoReferenceTwo {
                    |    string name = 1;
                    |}
                    |"""

    no_ref_second = trim_margin(no_ref_second)
    res = await registry_async_client.post_subjects_versions(
        subject="sub3", json={"schemaType": "PROTOBUF", "schema": no_ref_second}
    )
    assert res.status_code == 200
    assert "id" in res.json()

    add_new_ref_in_sub2 = """
                             |syntax = "proto3";
                             |import "NoReference.proto";
                             |import "NoReferenceTwo.proto";
                             |message WithReference {
                             |    string name = 1;
                             |    NoReference ref = 2;
                             |    NoReferenceTwo refTwo = 3;
                             |}
                             |"""

    add_new_ref_in_sub2 = trim_margin(add_new_ref_in_sub2)

    references = [
        {"name": "NoReference.proto", "subject": "sub1", "version": 1},
        {"name": "NoReferenceTwo.proto", "subject": "sub3", "version": 1},
    ]

    res = await registry_async_client.post_subjects_versions(
        subject="sub2",
        json={"schemaType": "PROTOBUF", "schema": add_new_ref_in_sub2, "references": references},
    )
    assert res.status_code == 200
    assert "id" in res.json()


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_schema_verifier(registry_async_client: Client) -> None:
    customer_schema = """
            |syntax = "proto3";
            |package a1;
            |message Customer {
            |        string name = 1;
            |        int32 code = 2;
            |}
            |"""

    customer_schema = trim_margin(customer_schema)
    res = await registry_async_client.post_subjects_versions(
        subject="customer",
        json={"schemaType": "PROTOBUF", "schema": customer_schema},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    original_schema = """
            |syntax = "proto3";
            |package a1;
            |import "Customer.proto";
            |message TestMessage {
            |    enum Enum {
            |       HIGH = 0;
            |       MIDDLE = 1;
            |       LOW = 2;
            |    }
            |    message Value {
            |        message Label{
            |              int32 Id = 1;
            |              string name = 2;
            |        }
            |        Customer customer = 1;
            |        int32 x = 2;
            |    }
            |    string test = 1;
            |    .a1.TestMessage.Value val = 2;
            |    TestMessage.Value valx = 3;
            |
            |    oneof page_info {
            |      option (my_option) = true;
            |      int32 page_number = 5;
            |      int32 result_per_page = 6;
            |    }
            |}
            |"""

    original_schema = trim_margin(original_schema)
    references = [{"name": "Customer.proto", "subject": "customer", "version": 1}]
    res = await registry_async_client.post_subjects_versions(
        subject="test_schema",
        json={"schemaType": "PROTOBUF", "schema": original_schema, "references": references},
    )
    assert res.status_code == 200
    assert "id" in res.json()
    res = await registry_async_client.get_subjects_subject_version_referenced_by(subject="customer", version="latest")
    assert res.status_code == 200
    myjson = res.json()
    referents = [2]
    assert not any(x != y for x, y in zip(myjson, referents))

    res = await registry_async_client.delete_subjects_version(subject="customer", version=1)
    assert res.status_code == 422
    match_msg = "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=customer,version=1}."
    myjson = res.json()
    assert myjson["error_code"] == 42206 and myjson["message"] == match_msg

    res = await registry_async_client.delete_subjects_version(subject="test_schema", version=1)
    assert res.status_code == 200

    res = await registry_async_client.delete_subjects_version(subject="customer", version=1)
    assert res.status_code == 422

    res = await registry_async_client.delete_subjects_version(subject="test_schema", version=1, permanent=True)
    assert res.status_code == 200

    res = await registry_async_client.delete_subjects_version(subject="customer", version=1)
    assert res.status_code == 200


@dataclass
class TestCaseSchema:
    schema_type: SchemaType
    schema_str: str
    subject: str
    references: list[JsonData] | None = None
    expected: int = 200
    expected_msg: str = ""
    expected_error_code: int | None = None


TestCaseSchema.__test__ = False


@dataclass
class TestCaseDeleteSchema:
    subject: str
    version: int
    schema_id: int
    expected: int = 200
    expected_msg: str = ""
    expected_error_code: int | None = None


TestCaseDeleteSchema.__test__ = False


@dataclass
class TestCaseHardDeleteSchema(TestCaseDeleteSchema):
    pass


@dataclass
class ReferenceTestCase(BaseTestCase):
    schemas: list[TestCaseSchema | TestCaseDeleteSchema]


# Base case
SCHEMA_NO_REF = """\
syntax = "proto3";

message NoReference {
  string name = 1;
}
"""

SCHEMA_NO_REF_V2 = """\
syntax = "proto3";

message NoReference {
  string name = 1;
  string address = 2;
}
"""


SCHEMA_NO_REF_TWO = """\
syntax = "proto3";

message NoReferenceTwo {
  string name = 1;
}
"""

SCHEMA_WITH_REF = """\
syntax = "proto3";

import "NoReference.proto";

message WithReference {
  string name = 1;
  NoReference ref = 2;
}
"""

SCHEMA_WITH_2ND_LEVEL_REF = """\
syntax = "proto3";

import "WithReference.proto";

message With2ndLevelReference {
  string name = 1;
  WithReference ref = 2;
}
"""

SCHEMA_REMOVES_REFERENCED_FIELD_INCOMPATIBLE = """\
syntax = "proto3";

message WithReference {
  string name = 1;
}
"""

SCHEMA_ADDS_NEW_REFERENCE = """\
syntax = "proto3";

import "NoReference.proto";
import "NoReferenceTwo.proto";

message WithReference {
  string name = 1;
  NoReference ref = 2;
  NoReferenceTwo refTwo = 3;
}
"""

# Invalid schema
SCHEMA_INVALID_MISSING_CLOSING_BRACE = """\
syntax = "proto3";

import "NoReference.proto";

message SchemaMissingClosingBrace {
  string name = 1;
  NoReference ref = 2;

"""

# Schema having multiple messages
SCHEMA_NO_REF_TWO_MESSAGES = """\
syntax = "proto3";

message NoReferenceOne {
  string nameOne = 1;
}

message NoReferenceTwo {
  string nameTwo = 1;
}
"""

SCHEMA_WITH_REF_TO_NO_REFERENCE_TWO = """\
syntax = "proto3";

import "NoReferenceTwo.proto";

message WithReference {
  string name = 1;
  NoReferenceTwo ref = 2;
}
"""

# Nested references
SCHEMA_NO_REF_NESTED_MESSAGE = """\
syntax = "proto3";

message NoReference {
  message NoReferenceNested {
    string nameNested = 1;
  }
  string name = 1;
  NoReferenceNested ref = 2;
}
"""

SCHEMA_WITH_REF_TO_NESTED = """\
syntax = "proto3";

import "NoReferenceNested.proto";

message WithReference {
  string name = 1;
  NoReference.NoReferenceNested ref = 2;
}
"""


@pytest.mark.parametrize(
    "testcase",
    [
        ReferenceTestCase(
            test_name="No references",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF,
                    subject="nr_s1",
                    references=None,
                    expected=200,
                ),
                TestCaseDeleteSchema(
                    subject="nr_s1",
                    schema_id=1,
                    version=1,
                    expected=200,
                ),
            ],
        ),
        # Better error message should be given back, now it is only InvalidSchema
        ReferenceTestCase(
            test_name="With reference, ref schema does not exist",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF,
                    subject="wr_nonexisting_s1",
                    references=[{"name": "NoReference.proto", "subject": "wr_not_found", "version": 1}],
                    expected=422,
                    expected_msg=(
                        f"Invalid PROTOBUF schema. Error: Invalid schema {SCHEMA_WITH_REF} "
                        "with refs [{name='NoReference.proto', subject='wr_not_found', version=1}]"
                        f" of type {SchemaType.PROTOBUF}"
                    ),
                    expected_error_code=42201,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference, references not given",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF,
                    subject="wr_nonexisting_s1_missing_references",
                    references=None,
                    expected=422,
                    expected_msg='Invalid PROTOBUF schema. Error: type "NoReference" is not defined',
                    expected_error_code=42201,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF,
                    subject="wr_s1",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF,
                    subject="wr_s2",
                    references=[{"name": "NoReference.proto", "subject": "wr_s1", "version": 1}],
                    expected=200,
                ),
                TestCaseDeleteSchema(
                    subject="wr_s1",
                    schema_id=1,
                    version=1,
                    expected=422,
                    expected_msg=(
                        "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=wr_s1,version=1}."
                    ),
                    expected_error_code=42206,
                ),
                TestCaseDeleteSchema(
                    subject="wr_s2",
                    schema_id=2,
                    version=1,
                    expected=200,
                ),
                TestCaseHardDeleteSchema(
                    subject="wr_s2",
                    schema_id=2,
                    version=1,
                    expected=200,
                ),
                TestCaseDeleteSchema(
                    subject="wr_s1",
                    schema_id=1,
                    version=1,
                    expected=200,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference, remove referenced field causes incompatible schema",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF,
                    subject="wr_s1_test_incompatible_change",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF,
                    subject="wr_s2_test_incompatible_change",
                    references=[{"name": "NoReference.proto", "subject": "wr_s1_test_incompatible_change", "version": 1}],
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_REMOVES_REFERENCED_FIELD_INCOMPATIBLE,
                    subject="wr_s2_test_incompatible_change",
                    references=None,
                    expected=200,
                    # It is erroneous assumption, there FIELD_DROP only, and it is compatible.
                    # expected = 200
                    # expected_msg=(
                    #        "Incompatible schema, compatibility_mode=BACKWARD "
                    #        "Incompatible modification Modification.MESSAGE_DROP found"
                    # ),
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference, add new referenced field",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF,
                    subject="wr_s1_add_new_reference",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF,
                    subject="wr_s2_add_new_reference",
                    references=[{"name": "NoReference.proto", "subject": "wr_s1_add_new_reference", "version": 1}],
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF_TWO,
                    subject="wr_s3_the_new_reference",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_ADDS_NEW_REFERENCE,
                    subject="wr_s2_add_new_reference",
                    references=[
                        {"name": "NoReference.proto", "subject": "wr_s1_add_new_reference", "version": 1},
                        {"name": "NoReferenceTwo.proto", "subject": "wr_s3_the_new_reference", "version": 1},
                    ],
                    expected=200,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference chain, nonexisting schema",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF,
                    subject="wr_chain_s1",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF,
                    subject="wr_chain_s2",
                    references=[
                        {"name": "NoReference.proto", "subject": "wr_chain_s1", "version": 1},
                        {"name": "NotFoundReference.proto", "subject": "wr_chain_nonexisting", "version": 1},
                    ],
                    expected=422,
                    expected_msg=(
                        f"Invalid PROTOBUF schema. Error: Invalid schema {SCHEMA_WITH_REF} "
                        "with refs [{name='NoReference.proto', subject='wr_chain_s1', version=1}, "
                        "{name='NotFoundReference.proto', subject='wr_chain_nonexisting', version=1}] "
                        f"of type {SchemaType.PROTOBUF}"
                    ),
                    expected_error_code=42201,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference chain",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF,
                    subject="wr_chain_s1",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF,
                    subject="wr_chain_s2",
                    references=[{"name": "NoReference.proto", "subject": "wr_chain_s1", "version": 1}],
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_2ND_LEVEL_REF,
                    subject="wr_chain_s3",
                    references=[{"name": "WithReference.proto", "subject": "wr_chain_s2", "version": 1}],
                    expected=200,
                ),
                TestCaseDeleteSchema(
                    subject="wr_chain_s1",
                    schema_id=1,
                    version=1,
                    expected=422,
                ),
                TestCaseDeleteSchema(
                    subject="wr_chain_s2",
                    schema_id=2,
                    version=1,
                    expected=422,
                ),
                TestCaseDeleteSchema(
                    subject="wr_chain_s3",
                    schema_id=3,
                    version=1,
                    expected=200,
                ),
                TestCaseHardDeleteSchema(
                    subject="wr_chain_s3",
                    schema_id=3,
                    version=1,
                    expected=200,
                ),
                TestCaseDeleteSchema(
                    subject="wr_chain_s2",
                    schema_id=2,
                    version=1,
                    expected=200,
                ),
                TestCaseHardDeleteSchema(
                    subject="wr_chain_s2",
                    schema_id=2,
                    version=1,
                    expected=200,
                ),
                TestCaseDeleteSchema(
                    subject="wr_chain_s1",
                    schema_id=1,
                    version=1,
                    expected=200,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="Invalid schema missing closing brace",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF,
                    subject="wr_invalid_reference_ok_schema",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_INVALID_MISSING_CLOSING_BRACE,
                    subject="wr_invalid_missing_closing_brace",
                    references=[{"name": "NoReference.proto", "subject": "wr_invalid_reference_ok_schema", "version": 1}],
                    expected=422,
                    expected_msg=(
                        f"Invalid PROTOBUF schema. Error: Invalid schema {SCHEMA_INVALID_MISSING_CLOSING_BRACE} "
                        "with refs [{name='NoReference.proto', subject='wr_invalid_reference_ok_schema', version=1}] "
                        f"of type {SchemaType.PROTOBUF}"
                    ),
                    expected_error_code=42201,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference to message from schema file defining two messages",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF_TWO_MESSAGES,
                    subject="wr_s1_two_messages",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF_TO_NO_REFERENCE_TWO,
                    subject="wr_s2_referencing_message_two",
                    references=[{"name": "NoReferenceTwo.proto", "subject": "wr_s1_two_messages", "version": 1}],
                    expected=200,
                ),
            ],
        ),
        ReferenceTestCase(
            test_name="With reference to nested message",
            schemas=[
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_NO_REF_NESTED_MESSAGE,
                    subject="wr_s1_with_nested_message",
                    references=None,
                    expected=200,
                ),
                TestCaseSchema(
                    schema_type=SchemaType.PROTOBUF,
                    schema_str=SCHEMA_WITH_REF_TO_NESTED,
                    subject="wr_s2_referencing_nested_message",
                    references=[{"name": "NoReference.proto", "subject": "wr_s1_with_nested_message", "version": 1}],
                    expected=200,
                ),
            ],
        ),
    ],
    ids=str,
)
@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
@pytest.mark.parametrize("metadata", [None, {}])
@pytest.mark.parametrize("rule_set", [None, {}])
async def test_references(
    testcase: ReferenceTestCase, registry_async_client: Client, metadata: SchemaMetadata, rule_set: SchemaRuleSet
):
    for testdata in testcase.schemas:
        if isinstance(testdata, TestCaseSchema):
            print(f"Adding new schema, subject: '{testdata.subject}'\n{testdata.schema_str}")
            body = {
                "schemaType": testdata.schema_type,
                "schema": testdata.schema_str,
                "metadata": metadata,
                "ruleSet": rule_set,
            }
            if testdata.references:
                body["references"] = testdata.references
            res = await registry_async_client.post_subjects_versions(subject=testdata.subject, json=body)
        elif isinstance(testdata, TestCaseHardDeleteSchema):
            print(
                f"Permanently deleting schema, subject: '{testdata.subject}, "
                f"schema: {testdata.schema_id}, version: {testdata.version}' "
            )
            res = await registry_async_client.delete_subjects_version(
                subject=testdata.subject,
                version=testdata.version,
                permanent=True,
            )
        elif isinstance(testdata, TestCaseDeleteSchema):
            print(
                f"Deleting schema, subject: '{testdata.subject}, schema: {testdata.schema_id}, version: {testdata.version}' "
            )
            res = await registry_async_client.delete_subjects_version(
                subject=testdata.subject,
                version=testdata.version,
            )
        else:
            raise InvalidTest("Unknown test case.")

        assert res.status_code == testdata.expected
        if testdata.expected_msg:
            assert res.json_result.get("message", None) == testdata.expected_msg
        if testdata.expected_error_code:
            assert res.json_result.get("error_code") == testdata.expected_error_code
        if isinstance(testdata, TestCaseSchema):
            if testdata.expected == 200:
                schema_id = res.json().get("id")
                fetch_schema_res = await registry_async_client.get_schema_by_id(schema_id=schema_id)
                assert fetch_schema_res.status_code == 200
                if testdata.references:
                    assert "references" in fetch_schema_res.json()
                else:
                    assert "references" not in fetch_schema_res.json()
        if isinstance(testdata, TestCaseDeleteSchema):
            if testdata.expected == 200:
                fetch_res = await registry_async_client.get_subjects_subject_version(
                    subject=testdata.subject,
                    version=testdata.version,
                )
                assert fetch_res.status_code == 404
            else:
                fetch_schema_res = await registry_async_client.get_schema_by_id(schema_id=testdata.schema_id)
                assert fetch_schema_res.status_code == 200


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_reference_update_creates_new_schema_version(registry_async_client: Client):
    test_schemas = [
        TestCaseSchema(
            schema_type=SchemaType.PROTOBUF,
            schema_str=SCHEMA_NO_REF,
            subject="wr_s1",
            references=None,
            expected=200,
        ),
        TestCaseSchema(
            schema_type=SchemaType.PROTOBUF,
            schema_str=SCHEMA_WITH_REF,
            subject="wr_s2",
            references=[{"name": "NoReference.proto", "subject": "wr_s1", "version": 1}],
            expected=200,
        ),
        TestCaseSchema(
            schema_type=SchemaType.PROTOBUF,
            schema_str=SCHEMA_NO_REF_V2,
            subject="wr_s1",
            references=None,
            expected=200,
        ),
        TestCaseSchema(
            schema_type=SchemaType.PROTOBUF,
            schema_str=SCHEMA_WITH_REF,
            subject="wr_s2",
            references=[{"name": "NoReference.proto", "subject": "wr_s1", "version": 2}],
            expected=200,
        ),
    ]
    schema_ids: list[int] = []
    for testdata in test_schemas:
        body = {"schemaType": testdata.schema_type, "schema": testdata.schema_str}
        if testdata.references:
            body["references"] = testdata.references
        res = await registry_async_client.post_subjects_versions(subject=testdata.subject, json=body)
        assert res.status_code == testdata.expected
        schema_ids.append(res.json_result.get("id"))
    res = await registry_async_client.get_subjects_versions(subject="wr_s2")
    assert len(res.json_result) == 2, "Expected two versions of schemas as reference was updated."
    res = await registry_async_client.get_subjects_subject_version(subject="wr_s2", version=2)
    references = res.json_result.get("references")
    assert len(references) == 1
    assert references[0].get("name") == "NoReference.proto"
    assert references[0].get("subject") == "wr_s1"
    assert references[0].get("version") == 2

    # Assert when querying the schema id with schema version with references correct schema id is returned.
    for testdata, expected_schema_id in zip(test_schemas, schema_ids):
        body = {
            "schemaType": testdata.schema_type,
            "schema": testdata.schema_str,
        }
        if testdata.references:
            body["references"] = testdata.references
        res = await registry_async_client.post_subjects(subject=testdata.subject, json=body)
        assert res.json_result.get("id") == expected_schema_id


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_error(registry_async_client: Client) -> None:
    testdata = TestCaseSchema(
        schema_type=SchemaType.PROTOBUF,
        schema_str=SCHEMA_NO_REF,
        subject="wr_s1_test_incompatible_change",
        references=None,
        expected=200,
    )
    print(f"Adding new schema, subject: '{testdata.subject}'\n{testdata.schema_str}")
    body = {"schemaType": testdata.schema_type, "schema": testdata.schema_str}
    if testdata.references:
        body["references"] = testdata.references
    res = await registry_async_client.post_subjects_versions(subject=testdata.subject, json=body)

    assert res.status_code == 200

    testdata = TestCaseSchema(
        schema_type=SchemaType.PROTOBUF,
        schema_str=SCHEMA_WITH_REF,
        subject="wr_s2_test_incompatible_change",
        references=[{"name": "NoReference.proto", "subject": "wr_s1_test_incompatible_change", "version": 1}],
        expected=200,
    )
    print(f"Adding new schema, subject: '{testdata.subject}'\n{testdata.schema_str}")
    body = {"schemaType": testdata.schema_type, "schema": testdata.schema_str}
    if testdata.references:
        body["references"] = testdata.references
    res = await registry_async_client.post_subjects_versions(subject=testdata.subject, json=body)
    assert res.status_code == 200
    testdata = TestCaseSchema(
        schema_type=SchemaType.PROTOBUF,
        schema_str=SCHEMA_REMOVES_REFERENCED_FIELD_INCOMPATIBLE,
        subject="wr_s2_test_incompatible_change",
        references=None,
        expected=409,
        expected_msg=(
            # ACTUALLY THERE NO MESSAGE_DROP!!!
            "Incompatible schema, compatibility_mode=BACKWARD. "
            "Incompatibilities: Incompatible modification Modification.MESSAGE_DROP found"
        ),
    )
    print(f"Adding new schema, subject: '{testdata.subject}'\n{testdata.schema_str}")
    body = {"schemaType": testdata.schema_type, "schema": testdata.schema_str}
    if testdata.references:
        body["references"] = testdata.references
    res = await registry_async_client.post_subjects_versions(subject=testdata.subject, json=body)

    assert res.status_code == 200


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_missing_google_import(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_missing_google_import")()

    unknown_proto = """\
syntax = "proto3";
package a1;
message UsingGoogleTypesWithoutImport {
        string name = 1;
        google.type.PostalAddress p = 2;
}
"""
    body = {"schemaType": "PROTOBUF", "schema": unknown_proto}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)

    assert res.status_code == 422

    myjson = res.json()
    assert myjson["error_code"] == 42201 and '"google.type.PostalAddress" is not defined' in myjson["message"]


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_customer_update(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_customer_update")()

    customer_proto = """\
syntax = "proto3";
package a1;
import "google/type/postal_address.proto";
// @consumer: the first comment
message Customer {
        string name = 1;
        google.type.PostalAddress address = 2;
}
"""

    body = {"schemaType": "PROTOBUF", "schema": customer_proto}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)

    assert res.status_code == 200

    customer_proto = """\
syntax = "proto3";
package a1;
import "google/type/postal_address.proto";
// @consumer: the comment was incorrect, updating it now
message Customer {
        string name = 1;
        google.type.PostalAddress address = 2;
}
"""

    body = {"schemaType": "PROTOBUF", "schema": customer_proto}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)

    assert res.status_code == 200


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_binary_serialized(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_binary_serialized")()

    schema_plain = """\
syntax = "proto3";

message Key {
  int32 id = 1;
}
message Dog {
  string name = 1;
  int32 weight = 2;
  repeated string toys = 4;
}
"""
    schema_serialized = (
        "Cg5tZXNzYWdlcy5wcm90byIRCgNLZXkSCgoCaWQYASABKAUiMQoDRG9nEgwKBG5hbW"
        + "UYASABKAkSDgoGd2VpZ2h0GAIgASgFEgwKBHRveXMYBCADKAliBnByb3RvMw=="
    )

    body = {"schemaType": "PROTOBUF", "schema": schema_serialized}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)

    assert res.status_code == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    body = {"schemaType": "PROTOBUF", "schema": schema_plain}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)

    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"]

    res = await registry_async_client.get_schema_by_id(schema_id=schema_id)
    assert res.status_code == 200
    assert "schema" in res.json()
    assert res.json()["schema"] == schema_plain

    res = await registry_async_client.get_schema_by_id(schema_id=schema_id, params={"format": "serialized"})
    assert res.status_code == 200
    assert "schema" in res.json()
    assert res.json()["schema"]

    body = {"schemaType": "PROTOBUF", "schema": res.json()["schema"]}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)
    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id == res.json()["id"]


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_protobuf_update_ordering(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_update_ordering")()

    schema_v1 = """\
syntax = "proto3";
package tc4;

message Fred {
  HodoCode hodecode = 1;
}

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}
"""

    body = {"schemaType": "PROTOBUF", "schema": schema_v1}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)

    assert res.status_code == 200
    assert "id" in res.json()
    schema_id = res.json()["id"]

    schema_v2 = """\
syntax = "proto3";
package tc4;

enum HodoCode {
  HODO_CODE_UNSPECIFIED = 0;
}

message Fred {
  HodoCode hodecode = 1;
  string id = 2;
}
"""

    body = {"schemaType": "PROTOBUF", "schema": schema_v2}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)

    assert res.status_code == 200
    assert "id" in res.json()
    assert schema_id != res.json()["id"]


SCHEMA_WITH_OPTION_UNORDERDERED = """\
syntax = "proto3";
package tc4;

option java_package = "com.example";
option java_generate_equals_and_hash = true;
option java_string_check_utf8 = true;
option java_multiple_files = true;
option java_outer_classname = "FredProto";
option java_generic_services = true;

message Foo {
  string code = 1;
}
"""


SCHEMA_WITH_OPTION_ORDERED = """\
syntax = "proto3";
package tc4;

option java_generate_equals_and_hash = true;
option java_generic_services = true;
option java_multiple_files = true;
option java_outer_classname = "FredProto";
option java_package = "com.example";
option java_string_check_utf8 = true;

message Foo {
  string code = 1;
}
"""


async def test_registering_normalized_schema(session_logdir: Path, kafka_servers: KafkaServers) -> None:
    subject = create_subject_name_factory("test_protobuf_normalization")()

    config1 = Config()
    config1.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config1.waiting_time_before_acting_as_master_ms = 500

    config2 = Config()
    config2.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config2.use_protobuf_formatter = True
    config2.waiting_time_before_acting_as_master_ms = 500

    async with start_schema_registry_cluster(
        config_templates=[config1, config2],
        data_dir=session_logdir / subject,
    ) as endpoints:
        async with after_master_is_available(endpoints, None):
            servers = [server.endpoint.to_url() for server in endpoints]
            client1 = Client(server_uri=servers[0], server_ca=None)
            client2 = Client(server_uri=servers[1], server_ca=None)

            await asyncio.sleep(2)

            body = {"schemaType": "PROTOBUF", "schema": SCHEMA_WITH_OPTION_ORDERED}
            res = await client1.post_subjects_versions(subject=subject, json=body, params={"normalize": "true"})

            assert res.status_code == 200
            assert "id" in res.json()
            original_schema_id = res.json()["id"]

            body = {"schemaType": "PROTOBUF", "schema": SCHEMA_WITH_OPTION_UNORDERDERED}
            res = await client1.post_subjects(subject=subject, json=body)
            assert res.status_code == 404

            res = await client2.post_subjects(subject=subject, json=body, params={"normalize": "true"})

            assert res.status_code == 200
            assert "id" in res.json()
            assert original_schema_id == res.json()["id"]


@pytest.mark.parametrize("registry_cluster", [{"config": {}}, {"config": {"use_protobuf_formatter": True}}], indirect=True)
async def test_normalized_schema_idempotence_produce_and_fetch(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_protobuf_normalization")()

    body = {"schemaType": "PROTOBUF", "schema": SCHEMA_WITH_OPTION_UNORDERDERED}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body, params={"normalize": "true"})

    assert res.status_code == 200
    assert "id" in res.json()
    original_schema_id = res.json()["id"]

    body = {"schemaType": "PROTOBUF", "schema": SCHEMA_WITH_OPTION_ORDERED}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body, params={"normalize": "true"})

    assert res.status_code == 200
    assert "id" in res.json()
    assert original_schema_id == res.json()["id"]

    res = await registry_async_client.get_schema_by_id(schema_id=original_schema_id)
    assert res.status_code == 200
    assert "schema" in res.json()
    assert res.json()["schema"] == SCHEMA_WITH_OPTION_ORDERED
