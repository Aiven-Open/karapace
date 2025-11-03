"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from karapace.core.protobuf.kotlin_wrapper import trim_margin
from karapace.core.schema_models import SchemaType, ValidatedTypedSchema
from karapace.core.serialization import SchemaRegistryClient

from karapace.core.dependency import Dependency
from karapace.core.schema_references import Reference
from karapace.core.typing import Version
from tests.schemas.protobuf import (
    schema_protobuf_order_after,
    schema_protobuf_order_before,
    schema_protobuf_plain,
    schema_protobuf_references,
)
from tests.utils import new_random_name


async def test_remote_client_protobuf(registry_async_client):
    schema_protobuf = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, schema_protobuf_plain)
    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client
    subject = new_random_name("subject")
    sc_id = await reg_cli.post_new_schema(subject, schema_protobuf, None)
    assert sc_id >= 0
    stored_schema, _ = await reg_cli.get_schema_for_id(sc_id)
    assert stored_schema == schema_protobuf, f"stored schema {stored_schema} is not {schema_protobuf}"

    stored_id, stored_schema, _ = await reg_cli.get_schema(subject)
    assert stored_id == sc_id
    assert stored_schema == schema_protobuf

    # get same schema a second time to hit cache
    stored_id, stored_schema, _ = await reg_cli.get_schema(subject)
    assert stored_id == sc_id
    assert stored_schema == schema_protobuf


async def test_remote_client_protobuf2(registry_async_client):
    schema_protobuf = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf_order_before))
    schema_protobuf_after = ValidatedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf_order_after))
    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client
    subject = new_random_name("subject")
    sc_id = await reg_cli.post_new_schema(subject, schema_protobuf, None)
    assert sc_id >= 0
    stored_schema, _ = await reg_cli.get_schema_for_id(sc_id)
    assert stored_schema == schema_protobuf, f"stored schema {stored_schema} is not {schema_protobuf}"
    stored_id, stored_schema, _ = await reg_cli.get_schema(subject)
    assert stored_id == sc_id
    assert stored_schema == schema_protobuf_after


async def test_protobuf_schema_with_reference(registry_async_client):
    # Simulate referenced imported schema (container2.proto)
    schema_container = ValidatedTypedSchema.parse(
        SchemaType.PROTOBUF,
        """syntax = "proto3";
        package a1.container;
        message H {
            string id = 1;
        }""",
    )

    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client

    # Register the imported schema first (container2.proto)
    ref_subject = "container2"
    ref_id = await reg_cli.post_new_schema(ref_subject, schema_container, None)
    assert ref_id >= 0

    # Parse the main schema with references and dependencies
    # Since schema_protobuf_references imports "container2.proto", we need to provide
    # both references (metadata) and dependencies (resolved schemas) for parsing
    references = [Reference(name="container2.proto", subject=ref_subject, version=Version(1))]
    dep = Dependency(name="container2.proto", subject=ref_subject, version=Version(1), target_schema=schema_container)
    dependencies = {"container2.proto": dep}

    schema_ref = ValidatedTypedSchema.parse(
        SchemaType.PROTOBUF,
        schema_protobuf_references,
        references=references,
        dependencies=dependencies,
    )

    # Register main schema referencing container2.proto
    main_subject = "a1-TestMessage"
    main_id = await reg_cli.post_new_schema(main_subject, schema_ref, references)
    assert main_id >= 0

    # --- Verify that schemas are stored and retrievable ---
    stored_schema_main, _ = await reg_cli.get_schema_for_id(main_id)
    assert stored_schema_main == schema_ref, "Main schema mismatch after registration"

    stored_schema_ref, _ = await reg_cli.get_schema_for_id(ref_id)
    assert stored_schema_ref == schema_container, "Referenced schema mismatch after registration"

    # --- Fetch via subject to confirm registry linkage ---
    fetched_id, fetched_schema, _ = await reg_cli.get_schema(main_subject)
    assert fetched_id == main_id
    assert fetched_schema == schema_ref

    # Ensure cross reference consistency
    assert any(ref.name == "container2.proto" for ref in references)


async def test_protobuf_get_schema_for_id_with_references_multiple_calls(registry_async_client):
    """Test that get_schema_for_id can be called multiple times without coroutine reuse errors.

    This integration test verifies the fix for RuntimeError: cannot reuse already awaited coroutine.
    The test creates schemas with references and calls get_schema_for_id multiple times to ensure
    the @alru_cache doesn't cause issues when the same schema is fetched concurrently.
    """
    reg_cli = SchemaRegistryClient()
    reg_cli.client = registry_async_client

    # Create and register a referenced schema
    ref_schema = ValidatedTypedSchema.parse(
        SchemaType.PROTOBUF,
        """syntax = "proto3";
        package a1.container;
        message Hint {
            string hint_str = 1;
        }""",
    )
    ref_subject = new_random_name("ref")
    ref_id = await reg_cli.post_new_schema(ref_subject, ref_schema, None)
    assert ref_id >= 0

    # Create and register a main schema with reference
    # Note: When parsing protobuf schemas with imports, we need both:
    # 1. references - metadata about what to reference
    # 2. dependencies - the actual resolved schema objects
    references = [Reference(name="container.proto", subject=ref_subject, version=Version(1))]
    # Create a Dependency object from the referenced schema
    # ref_schema is already a ValidatedTypedSchema, so we can use it directly
    dep = Dependency(name="container.proto", subject=ref_subject, version=Version(1), target_schema=ref_schema)
    dependencies = {"container.proto": dep}

    main_schema = ValidatedTypedSchema.parse(
        SchemaType.PROTOBUF,
        """syntax = "proto3";
        package a1;
        import "container.proto";
        message TestMessage {
            message Value {
                .a1.container.Hint hint = 1;
                int32 x = 2;
            }
            string test = 1;
            .a1.TestMessage.Value val = 2;
        }""",
        references=references,
        dependencies=dependencies,
    )
    main_subject = new_random_name("main")
    main_id = await reg_cli.post_new_schema(main_subject, main_schema, references)
    assert main_id >= 0

    # Call get_schema_for_id multiple times - this should not raise RuntimeError
    # The fix ensures that even if @alru_cache returns the same coroutine, it's handled correctly
    import asyncio

    async def fetch_schema():
        schema, subjects = await reg_cli.get_schema_for_id(main_id)
        return schema, subjects

    # Fetch multiple times concurrently to test the fix
    results = await asyncio.gather(
        fetch_schema(),
        fetch_schema(),
        fetch_schema(),
    )

    # Verify all calls succeeded
    for schema, subjects in results:
        assert schema is not None
        assert main_subject in subjects or any(str(s) == str(main_subject) for s in subjects)

    # Also test sequential calls
    schema1, _ = await reg_cli.get_schema_for_id(main_id)
    schema2, _ = await reg_cli.get_schema_for_id(main_id)
    assert schema1 == schema2

    await reg_cli.close()
