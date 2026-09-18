"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import asyncio
from contextlib import closing
import json
import time
from textwrap import dedent
from unittest.mock import Mock

import pytest

from karapace.core.client import Client
from karapace.core.config import Config
from karapace.core.in_memory_database import InMemoryDatabase
from karapace.core.key_format import KeyFormatter
from karapace.core.offset_watcher import OffsetWatcher
from karapace.core.schema_reader import KafkaSchemaReader
from karapace.core.stats import StatsClient
from karapace.core.typing import Mode, SchemaId, Subject, Version
from tests.integration.utils.cluster import RegistryDescription
from tests.integration.utils.kafka_server import KafkaServers
from tests.utils import create_schema_name_factory, create_subject_name_factory, new_random_name

# Every test gets its own registry on a freshly named _schemas topic (see
# start_schema_registry_cluster), so a test may safely change the global mode.


def _avro_schema(name: str, field: str = "f") -> str:
    return json.dumps({"type": "record", "name": name, "fields": [{"name": field, "type": "string"}]})


def _avro_schema_with_optional(name: str, extra: str) -> str:
    """An evolution of _avro_schema(name) that is BACKWARD compatible with it."""
    return json.dumps(
        {
            "type": "record",
            "name": name,
            "fields": [
                {"name": "f", "type": "string"},
                {"name": extra, "type": ["null", "string"], "default": None},
            ],
        }
    )


# Normalization sorts the options alphabetically, so these two differ only in option order
# and the second is the normalized form of the first.
PROTOBUF_OPTIONS_UNORDERED = """\
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

PROTOBUF_OPTIONS_ORDERED = """\
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


async def _wait_until_ready(reader: KafkaSchemaReader) -> None:
    # ready() is a method: `while not reader.ready` would always be falsy and never wait.
    deadline = time.monotonic() + 30
    while not reader.ready():
        assert time.monotonic() < deadline, "schema reader did not catch up with the topic"
        await asyncio.sleep(0.1)


async def test_global_mode(registry_async_client: Client) -> None:
    res = await registry_async_client.get_mode()
    assert res.status_code == 200
    json_res = res.json()
    assert json_res == {"mode": str(Mode.readwrite)}


async def test_subject_mode(registry_async_client: Client) -> None:
    subject_name_factory = create_subject_name_factory("test_schema_same_subject")
    schema_name = create_schema_name_factory("test_schema_same_subject")()

    schema_str = json.dumps(
        {
            "type": "record",
            "name": schema_name,
            "fields": [
                {
                    "name": "f",
                    "type": "string",
                }
            ],
        }
    )
    subject = subject_name_factory()
    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema_str})
    assert res.status_code == 200

    res = await registry_async_client.get_mode_subject(subject=subject)
    assert res.status_code == 200
    json_res = res.json()
    assert json_res == {"mode": str(Mode.readwrite)}

    res = await registry_async_client.get_mode_subject(subject="unknown_subject")
    assert res.status_code == 404
    json_res = res.json()
    assert json_res == {"error_code": 40401, "message": "Subject 'unknown_subject' not found."}


async def test_get_mode_subject_default_to_global(registry_async_client: Client) -> None:
    res = await registry_async_client.get_mode_subject(subject="unknown_subject", defaultToGlobal=True)
    assert res.status_code == 200
    assert res.json() == {"mode": str(Mode.readwrite)}


async def test_invalid_mode_value_is_rejected(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_invalid_mode_value")()

    res = await registry_async_client.put_mode_subject(subject=subject, json={"mode": "BOGUS"})

    assert res.status_code == 422
    assert res.json()["error_code"] == 42204


async def test_readonly_mode_is_not_supported(registry_async_client: Client) -> None:
    """Karapace implements READWRITE and IMPORT only."""
    subject = create_subject_name_factory("test_readonly_mode")()

    res = await registry_async_client.put_mode_subject(subject=subject, json={"mode": "READONLY"})

    assert res.status_code == 422
    assert res.json()["error_code"] == 42204


async def test_import_roundtrip(registry_async_client: Client) -> None:
    """Ids and versions survive an import, and normal writes resume afterwards."""
    subject = create_subject_name_factory("test_import_roundtrip")()
    name_factory = create_schema_name_factory("test_import_roundtrip")
    latest_name = name_factory()
    schema_a = _avro_schema(name_factory())
    schema_b = _avro_schema(latest_name)
    # Compatible with schema_b, so it is accepted once compatibility checking is back on.
    schema_c = _avro_schema_with_optional(latest_name, "g")
    incompatible = _avro_schema(name_factory())

    # IMPORT may be set before the subject exists.
    res = await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})
    assert res.status_code == 200
    assert res.json() == {"mode": str(Mode.import_mode)}

    res = await registry_async_client.get_mode_subject(subject=subject)
    assert res.status_code == 200
    assert res.json() == {"mode": str(Mode.import_mode)}

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema_a, "id": 100001, "version": 5}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 100001}

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema_b, "id": 100002, "version": 6}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 100002}

    res = await registry_async_client.get_subjects_versions(subject=subject)
    assert res.status_code == 200
    assert res.json() == [5, 6]

    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=5)
    assert res.status_code == 200
    assert res.json()["id"] == 100001
    assert res.json()["version"] == 5

    # Versions 1 to 4 were never imported and must not be invented.
    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=1)
    assert res.status_code == 404
    assert res.json()["error_code"] == 40402

    res = await registry_async_client.get_subjects_subject_version(subject=subject, version="latest")
    assert res.status_code == 200
    assert res.json()["version"] == 6

    res = await registry_async_client.get_schema_by_id(schema_id=100001)
    assert res.status_code == 200
    assert json.loads(res.json()["schema"]) == json.loads(schema_a)

    res = await registry_async_client.delete_mode_subject(subject=subject)
    assert res.status_code == 200
    assert res.json() == {"mode": str(Mode.readwrite)}

    res = await registry_async_client.get_mode_subject(subject=subject)
    assert res.status_code == 200
    assert res.json() == {"mode": str(Mode.readwrite)}

    # Leaving IMPORT mode restores compatibility checking against the default BACKWARD.
    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": incompatible})
    assert res.status_code == 409
    assert res.json()["error_code"] == 409

    # Imported ids raised the global counter, so an ordinary write cannot collide with them.
    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema_c})
    assert res.status_code == 200
    assert res.json()["id"] > 100002

    res = await registry_async_client.get_subjects_versions(subject=subject)
    assert res.status_code == 200
    assert res.json() == [5, 6, 7]


async def test_import_replay_is_idempotent(registry_async_client: Client) -> None:
    """Re-posting an imported triple lets a failed import be replayed."""
    subject = create_subject_name_factory("test_import_replay")()
    schema = _avro_schema(create_schema_name_factory("test_import_replay")())

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    body = {"schema": schema, "id": 100101, "version": 3}
    first = await registry_async_client.post_subjects_versions(subject=subject, json=body)
    assert first.status_code == 200
    second = await registry_async_client.post_subjects_versions(subject=subject, json=body)
    assert second.status_code == 200
    assert first.json() == second.json() == {"id": 100101}

    res = await registry_async_client.get_subjects_versions(subject=subject)
    assert res.json() == [3]


async def test_import_skips_compatibility_checks(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_import_skips_compat")()
    name = create_schema_name_factory("test_import_skips_compat")()

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
    assert (
        await registry_async_client.put_config_subject(subject=subject, json={"compatibility": "FULL"})
    ).status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": _avro_schema(name, field="f"), "id": 100201, "version": 1}
    )
    assert res.status_code == 200

    # Renaming a required field is not FULL compatible, but IMPORT does not check.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": _avro_schema(name, field="renamed"), "id": 100202, "version": 2}
    )
    assert res.status_code == 200


async def test_import_version_collision_is_rejected(registry_async_client: Client) -> None:
    """A version already registered must not be silently repointed to another schema id."""
    subject = create_subject_name_factory("test_import_version_collision")()
    name_factory = create_schema_name_factory("test_import_version_collision")

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": _avro_schema(name_factory()), "id": 100301, "version": 5}
    )
    assert res.status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": _avro_schema(name_factory()), "id": 100302, "version": 5}
    )
    assert res.status_code == 409
    assert res.json()["error_code"] == 40901

    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=5)
    assert res.json()["id"] == 100301


async def test_import_rebinding_an_id_is_rejected(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_import_rebind_id")()
    name_factory = create_schema_name_factory("test_import_rebind_id")

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": _avro_schema(name_factory()), "id": 100401, "version": 1}
    )
    assert res.status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": _avro_schema(name_factory()), "id": 100401, "version": 2}
    )
    assert res.status_code == 409
    assert res.json()["error_code"] == 40901


async def test_import_same_content_under_a_different_id_is_allowed(registry_async_client: Client) -> None:
    """Sources that disagree about which id holds a schema still import."""
    subject = create_subject_name_factory("test_import_same_content")()
    schema = _avro_schema(create_schema_name_factory("test_import_same_content")())

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 100501, "version": 1}
    )
    assert res.status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 100502, "version": 2}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 100502}
    assert (await registry_async_client.get_subjects_versions(subject=subject)).json() == [1, 2]


async def test_set_import_mode_rejected_on_non_empty_subject(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_import_non_empty")()
    schema = _avro_schema(create_schema_name_factory("test_import_non_empty")())

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema})
    assert res.status_code == 200

    res = await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})
    assert res.status_code == 409
    assert res.json()["error_code"] == 40901

    res = await registry_async_client.get_mode_subject(subject=subject)
    assert res.json() == {"mode": str(Mode.readwrite)}


async def test_forced_import_mode_keeps_existing_schemas(registry_async_client: Client) -> None:
    """force skips the emptiness check only."""
    subject = create_subject_name_factory("test_import_forced")()
    schema = _avro_schema(create_schema_name_factory("test_import_forced")())

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema})
    assert res.status_code == 200
    original_id = res.json()["id"]

    res = await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"}, force=True)
    assert res.status_code == 200
    assert res.json() == {"mode": str(Mode.import_mode)}

    res = await registry_async_client.get_subjects_versions(subject=subject)
    assert res.json() == [1]
    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=1)
    assert res.json()["id"] == original_id
    assert json.loads(res.json()["schema"]) == json.loads(schema)


async def test_global_import_mode_on_empty_registry(registry_async_client: Client) -> None:
    """A whole registry can be migrated at once while it is still empty."""
    subject = create_subject_name_factory("test_global_import")()
    name = create_schema_name_factory("test_global_import")()
    schema_a = _avro_schema(name)
    # Compatible with schema_a, which matters once the registry is back in READWRITE.
    schema_b = _avro_schema_with_optional(name, "g")

    res = await registry_async_client.put_mode(json={"mode": "IMPORT"})
    assert res.status_code == 200
    assert res.json() == {"mode": str(Mode.import_mode)}
    assert (await registry_async_client.get_mode()).json() == {"mode": str(Mode.import_mode)}

    # A subject with no override of its own inherits the global mode.
    res = await registry_async_client.get_mode_subject(subject=subject, defaultToGlobal=True)
    assert res.json() == {"mode": str(Mode.import_mode)}

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema_a, "id": 100701, "version": 9}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 100701}
    assert (await registry_async_client.get_subjects_versions(subject=subject)).json() == [9]

    res = await registry_async_client.put_mode(json={"mode": "READWRITE"})
    assert res.status_code == 200
    assert (await registry_async_client.get_mode()).json() == {"mode": str(Mode.readwrite)}

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema_b})
    assert res.status_code == 200
    assert res.json()["id"] > 100701
    assert (await registry_async_client.get_subjects_versions(subject=subject)).json() == [9, 10]


async def test_subject_readwrite_overrides_global_import(registry_async_client: Client) -> None:
    """A subject can be held back from a registry wide migration."""
    subject = create_subject_name_factory("test_subject_holds_back")()
    schema = _avro_schema(create_schema_name_factory("test_subject_holds_back")())

    assert (await registry_async_client.put_mode(json={"mode": "IMPORT"})).status_code == 200
    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "READWRITE"})).status_code == 200

    assert (await registry_async_client.get_mode_subject(subject=subject)).json() == {"mode": str(Mode.readwrite)}

    # READWRITE wins, so an explicit id is rejected even though the registry is in IMPORT.
    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema, "id": 100801})
    assert res.status_code == 422
    assert res.json()["error_code"] == 42205


async def test_forced_import_over_existing_schemas(registry_async_client: Client) -> None:
    """Importing into a subject that already holds normally registered schemas."""
    subject = create_subject_name_factory("test_import_over_existing")()
    name_factory = create_schema_name_factory("test_import_over_existing")
    existing = _avro_schema(name_factory())
    incoming = _avro_schema(name_factory())
    third = _avro_schema(name_factory())

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": existing})
    assert res.status_code == 200
    existing_id = res.json()["id"]

    assert (
        await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"}, force=True)
    ).status_code == 200

    # Version 1 is taken by existing_id, so a different id may not claim it.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": incoming, "id": 100901, "version": 1}
    )
    assert res.status_code == 409
    assert res.json()["error_code"] == 40901

    # Content already registered under a server assigned id may also take an explicit one.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": existing, "id": 100902, "version": 2}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 100902}

    # A free version with fresh content imports cleanly alongside the existing schema.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": third, "id": 100903, "version": 3}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 100903}

    assert (await registry_async_client.get_subjects_versions(subject=subject)).json() == [1, 2, 3]
    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=1)
    assert res.json()["id"] == existing_id


async def test_import_same_id_and_content_into_two_subjects(registry_async_client: Client) -> None:
    """Several subjects may reference one schema id, which is normal after a migration."""
    subject_factory = create_subject_name_factory("test_import_shared_id")
    subject_a = subject_factory()
    subject_b = subject_factory()
    schema = _avro_schema(create_schema_name_factory("test_import_shared_id")())

    for subject in (subject_a, subject_b):
        assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
        res = await registry_async_client.post_subjects_versions(
            subject=subject, json={"schema": schema, "id": 101001, "version": 1}
        )
        assert res.status_code == 200
        assert res.json() == {"id": 101001}

    res = await registry_async_client.get_schema_by_id_versions(schema_id=101001)
    assert res.status_code == 200
    assert sorted(entry["subject"] for entry in res.json()) == sorted([subject_a, subject_b])


async def test_import_same_content_different_id_across_subjects_is_allowed(registry_async_client: Client) -> None:
    subject_factory = create_subject_name_factory("test_import_cross_subject_content")
    subject_a = subject_factory()
    subject_b = subject_factory()
    schema = _avro_schema(create_schema_name_factory("test_import_cross_subject_content")())

    for subject in (subject_a, subject_b):
        assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject_a, json={"schema": schema, "id": 101101, "version": 1}
    )
    assert res.status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject_b, json={"schema": schema, "id": 101102, "version": 1}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 101102}


async def test_import_over_soft_deleted_version(registry_async_client: Client) -> None:
    """A soft deleted version still owns its slot, but an exact replay restores it."""
    subject = create_subject_name_factory("test_import_soft_deleted")()
    name_factory = create_schema_name_factory("test_import_soft_deleted")
    schema = _avro_schema(name_factory())
    other = _avro_schema(name_factory())

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 101201, "version": 3}
    )
    assert res.status_code == 200

    res = await registry_async_client.delete_subjects_version(subject=subject, version=3)
    assert res.status_code == 200

    # The slot and the id binding survive the soft delete.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": other, "id": 101202, "version": 3}
    )
    assert res.status_code == 409
    assert res.json()["error_code"] == 40901

    # Replaying the original triple undeletes the version.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 101201, "version": 3}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 101201}
    assert (await registry_async_client.get_subjects_versions(subject=subject)).json() == [3]


async def test_global_import_mode_rejected_when_subjects_exist(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_global_import_rejected")()
    schema = _avro_schema(create_schema_name_factory("test_global_import_rejected")())
    assert (await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema})).status_code == 200

    res = await registry_async_client.put_mode(json={"mode": "IMPORT"})

    assert res.status_code == 409
    assert res.json()["error_code"] == 40901
    assert (await registry_async_client.get_mode()).json() == {"mode": str(Mode.readwrite)}


async def test_explicit_id_without_import_mode_is_rejected(registry_async_client: Client) -> None:
    """Returning a different id would break a migration unnoticed."""
    subject = create_subject_name_factory("test_explicit_id_no_import")()
    schema = _avro_schema(create_schema_name_factory("test_explicit_id_no_import")())

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema, "id": 100601})

    assert res.status_code == 422
    assert res.json()["error_code"] == 42205


@pytest.mark.parametrize("registry_cluster", [{"config": {"allow_duplicate_schema_ids": False}}], indirect=True)
async def test_duplicate_schema_ids_refused_when_flag_is_off(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_no_duplicate_ids")()
    schema = _avro_schema(create_schema_name_factory("test_no_duplicate_ids")())
    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 102101, "version": 1}
    )
    assert res.status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 102102, "version": 2}
    )
    assert res.status_code == 422
    assert res.json()["error_code"] == 42207
    assert "102101" in res.json()["message"]

    # An exact replay under the id the content already owns is still fine.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 102101, "version": 1}
    )
    assert res.status_code == 200
    assert (await registry_async_client.get_subjects_versions(subject=subject)).json() == [1]


@pytest.mark.parametrize("registry_cluster", [{"config": {"mode_mutability": False}}], indirect=True)
async def test_mode_changes_refused_when_mode_mutability_is_false(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_mode_immutable")()

    res = await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})
    assert res.status_code == 422
    assert res.json() == {"error_code": 42205, "message": "Mode changes are not allowed"}

    res = await registry_async_client.put_mode(json={"mode": "IMPORT"})
    assert res.status_code == 422
    assert res.json()["error_code"] == 42205

    # The gate runs before the "no override to delete" 404, so the answer does not depend
    # on whether an override exists.
    res = await registry_async_client.delete_mode_subject(subject=subject)
    assert res.status_code == 422
    assert res.json()["error_code"] == 42205

    # Reads are unaffected.
    assert (await registry_async_client.get_mode()).json() == {"mode": str(Mode.readwrite)}


async def test_registration_without_an_id_in_import_mode_is_rejected(registry_async_client: Client) -> None:
    """The id selects the mode, so its absence is a READWRITE registration."""
    subject = create_subject_name_factory("test_no_id_in_import")()
    schema = _avro_schema(create_schema_name_factory("test_no_id_in_import")())
    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema})
    assert res.status_code == 422
    assert res.json()["error_code"] == 42205

    # A version on its own is not enough either.
    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema, "version": 3})
    assert res.status_code == 422
    assert res.json()["error_code"] == 42205

    # Nothing was written, and the subject is still importable.
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 102001, "version": 3}
    )
    assert res.status_code == 200
    assert (await registry_async_client.get_subjects_versions(subject=subject)).json() == [3]


async def test_out_of_range_id_and_version_are_rejected(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_out_of_range")()
    schema = _avro_schema(create_schema_name_factory("test_out_of_range")())
    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema, "id": 0, "version": 1})
    assert res.status_code == 422
    assert res.json()["error_code"] == 42207
    assert isinstance(res.json()["message"], str)

    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 1, "version": -1}
    )
    assert res.status_code == 422
    assert res.json()["error_code"] == 42202
    assert isinstance(res.json()["message"], str)


async def test_delete_mode_without_subject_level_override(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_delete_mode_no_override")()
    schema = _avro_schema(create_schema_name_factory("test_delete_mode_no_override")())
    assert (await registry_async_client.post_subjects_versions(subject=subject, json={"schema": schema})).status_code == 200

    res = await registry_async_client.delete_mode_subject(subject=subject)

    assert res.status_code == 404
    assert res.json()["error_code"] == 40401


async def test_import_json_schema(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_import_json_schema")()
    schema = json.dumps({"type": "object", "properties": {"age": {"type": "number"}}})

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    body = {"schemaType": "JSON", "schema": schema, "id": 101501, "version": 4}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)
    assert res.status_code == 200
    assert res.json() == {"id": 101501}

    # Replay must stay idempotent, which depends on the stored form comparing equal.
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)
    assert res.status_code == 200
    assert res.json() == {"id": 101501}

    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=4)
    assert res.status_code == 200
    assert res.json()["id"] == 101501
    assert res.json()["schemaType"] == "JSON"


async def test_import_protobuf_schema(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_import_protobuf")()
    schema = dedent(
        """\
        syntax = "proto3";
        package com.example;
        message Person {
          string name = 1;
          int32 age = 2;
        }
        """
    )

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    body = {"schemaType": "PROTOBUF", "schema": schema, "id": 101601, "version": 2}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)
    assert res.status_code == 200
    assert res.json() == {"id": 101601}

    res = await registry_async_client.post_subjects_versions(subject=subject, json=body)
    assert res.status_code == 200
    assert res.json() == {"id": 101601}

    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=2)
    assert res.status_code == 200
    assert res.json()["id"] == 101601
    assert res.json()["schemaType"] == "PROTOBUF"


async def test_import_protobuf_schema_with_normalize(registry_async_client: Client) -> None:
    """Registering normalizes Protobuf while the reader parses unnormalized.

    If those two forms did not compare equal, an idempotent replay would be wrongly
    rejected as an attempt to rebind the id.
    """
    subject = create_subject_name_factory("test_import_protobuf_normalize")()
    schema = dedent(
        """\
        syntax = "proto3";
        package com.example;
        message Person {
          string name = 1;
          optional int32 age = 2;
          optional string nickname = 3;
        }
        """
    )

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    body = {"schemaType": "PROTOBUF", "schema": schema, "id": 101701, "version": 1}
    res = await registry_async_client.post_subjects_versions(subject=subject, json=body, params={"normalize": "true"})
    assert res.status_code == 200
    assert res.json() == {"id": 101701}

    res = await registry_async_client.post_subjects_versions(subject=subject, json=body, params={"normalize": "true"})
    assert res.status_code == 200
    assert res.json() == {"id": 101701}


async def test_normalize_is_applied_in_import_mode(registry_async_client: Client) -> None:
    """Karapace does not special case normalize for IMPORT, so the stored form is normalized."""
    subject = create_subject_name_factory("test_import_normalize")()
    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject,
        json={"schemaType": "PROTOBUF", "schema": PROTOBUF_OPTIONS_UNORDERED, "id": 102201, "version": 1},
        params={"normalize": "true"},
    )
    assert res.status_code == 200
    assert res.json() == {"id": 102201}

    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=1)
    assert res.status_code == 200
    assert res.json()["schema"] == PROTOBUF_OPTIONS_ORDERED, "IMPORT mode stores the normalized form"


async def test_normalize_not_requested_stores_the_schema_as_sent(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_import_no_normalize")()
    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=subject,
        json={"schemaType": "PROTOBUF", "schema": PROTOBUF_OPTIONS_UNORDERED, "id": 102301, "version": 1},
    )
    assert res.status_code == 200

    res = await registry_async_client.get_subjects_subject_version(subject=subject, version=1)
    assert res.json()["schema"] == PROTOBUF_OPTIONS_UNORDERED, "without normalize the option order is kept"


async def test_import_with_references(registry_async_client: Client) -> None:
    """References still resolve, so import the referenced schema first."""
    prefix = new_random_name("import-refs-")
    base_subject = f"{prefix}country"
    ref_subject = f"{prefix}address"
    country = json.dumps(
        {
            "type": "record",
            "name": "Country",
            "namespace": "com.example",
            "fields": [{"name": "code", "type": "string"}],
        }
    )
    address = json.dumps(
        {
            "type": "record",
            "name": "Address",
            "namespace": "com.example",
            "fields": [{"name": "street", "type": "string"}, {"name": "country", "type": "Country"}],
        }
    )
    references = [{"name": "country.avsc", "subject": base_subject, "version": 1}]

    for subject in (base_subject, ref_subject):
        assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=base_subject, json={"schema": country, "id": 101801, "version": 1}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 101801}

    res = await registry_async_client.post_subjects_versions(
        subject=ref_subject, json={"schema": address, "id": 101802, "version": 1, "references": references}
    )
    assert res.status_code == 200
    assert res.json() == {"id": 101802}

    res = await registry_async_client.get_subjects_subject_version(subject=ref_subject, version=1)
    assert res.status_code == 200
    assert res.json()["references"] == references

    # Delete protection proves the reference was recorded.
    res = await registry_async_client.delete_subjects_version(subject=base_subject, version=1)
    assert res.status_code == 422
    assert res.json()["error_code"] == 42206


async def test_import_referencing_schema_before_its_reference_fails(registry_async_client: Client) -> None:
    prefix = new_random_name("import-missing-ref-")
    ref_subject = f"{prefix}address"
    address = json.dumps(
        {
            "type": "record",
            "name": "Address",
            "namespace": "com.example",
            "fields": [{"name": "country", "type": "Country"}],
        }
    )

    assert (await registry_async_client.put_mode_subject(subject=ref_subject, json={"mode": "IMPORT"})).status_code == 200

    res = await registry_async_client.post_subjects_versions(
        subject=ref_subject,
        json={
            "schema": address,
            "id": 101901,
            "version": 1,
            "references": [{"name": "country.avsc", "subject": f"{prefix}country", "version": 1}],
        },
    )
    assert res.status_code == 422
    assert res.json()["error_code"] == 42201


async def test_imported_state_survives_a_restart(
    registry_async_client: Client,
    registry_cluster: RegistryDescription,
    kafka_servers: KafkaServers,
) -> None:
    """Replaying the topic into a fresh reader is what a restart does.

    The only check that MODE records and imported ids round trip through Kafka.
    """
    subject = create_subject_name_factory("test_import_survives_restart")()
    schema = _avro_schema(create_schema_name_factory("test_import_survives_restart")())

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
    res = await registry_async_client.post_subjects_versions(
        subject=subject, json={"schema": schema, "id": 101301, "version": 8}
    )
    assert res.status_code == 200

    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.topic_name = registry_cluster.schemas_topic
    config.group_id = new_random_name("group_id")

    database = InMemoryDatabase()
    reader = KafkaSchemaReader(
        config=config,
        offset_watcher=OffsetWatcher(),
        key_formatter=KeyFormatter(),
        master_coordinator=None,
        database=database,
        stats=Mock(spec=StatsClient),
    )
    reader.start()
    with closing(reader):
        await _wait_until_ready(reader)

        assert database.get_subject_mode(subject=Subject(subject)) == Mode.import_mode
        versions = database.find_subject_schemas(subject=Subject(subject), include_deleted=False)
        assert list(versions) == [Version(8)]
        assert versions[Version(8)].schema_id == SchemaId(101301)
        # The id counter has to be rebuilt too, or a later write would reuse an imported id.
        assert database.global_schema_id >= SchemaId(101301)


async def test_deleted_mode_survives_a_restart(
    registry_async_client: Client,
    registry_cluster: RegistryDescription,
    kafka_servers: KafkaServers,
) -> None:
    """The MODE tombstone must replay as a removed override."""
    subject = create_subject_name_factory("test_deleted_mode_restart")()
    schema = _avro_schema(create_schema_name_factory("test_deleted_mode_restart")())

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200
    assert (
        await registry_async_client.post_subjects_versions(
            subject=subject, json={"schema": schema, "id": 101401, "version": 1}
        )
    ).status_code == 200
    assert (await registry_async_client.delete_mode_subject(subject=subject)).status_code == 200

    config = Config()
    config.bootstrap_uri = kafka_servers.bootstrap_servers[0]
    config.topic_name = registry_cluster.schemas_topic
    config.group_id = new_random_name("group_id")

    database = InMemoryDatabase()
    reader = KafkaSchemaReader(
        config=config,
        offset_watcher=OffsetWatcher(),
        key_formatter=KeyFormatter(),
        master_coordinator=None,
        database=database,
        stats=Mock(spec=StatsClient),
    )
    reader.start()
    with closing(reader):
        await _wait_until_ready(reader)

        assert database.get_subject_mode(subject=Subject(subject)) is None
        assert database.get_global_mode() == Mode.readwrite


async def test_subject_mode_overrides_global_mode(registry_async_client: Client) -> None:
    subject = create_subject_name_factory("test_subject_overrides_global")()

    assert (await registry_async_client.get_mode()).json() == {"mode": str(Mode.readwrite)}
    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "IMPORT"})).status_code == 200

    assert (await registry_async_client.get_mode_subject(subject=subject)).json() == {"mode": str(Mode.import_mode)}
    assert (await registry_async_client.get_mode()).json() == {"mode": str(Mode.readwrite)}

    assert (await registry_async_client.put_mode_subject(subject=subject, json={"mode": "READWRITE"})).status_code == 200
    assert (await registry_async_client.get_mode_subject(subject=subject)).json() == {"mode": str(Mode.readwrite)}
