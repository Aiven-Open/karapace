"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from karapace.dependency import Dependency
from karapace.schema_models import AvroResolver, SchemaType, ValidatedTypedSchema
from karapace.schema_references import Reference
from karapace.typing import Subject, Version

import pytest
import textwrap


def create_validated_schema(schema_str: str, dependencies=None) -> ValidatedTypedSchema:
    """Helper function to create a validated typed schema."""
    return ValidatedTypedSchema(schema_str=schema_str, schema_type="AVRO", dependencies=dependencies or {})


@pytest.fixture(name="base_schema")
def fixture_base_schema():
    return '{"type": "record", "name": "BaseRecord", "fields": [{"name": "field1", "type": "string"}]}'


@pytest.fixture(name="dependency_schema")
def fixture_dependency_schema():
    return '{"type": "record", "name": "DependencyRecord", "fields": [{"name": "depField", "type": "int"}]}'


@pytest.fixture(name="another_dependency_schema")
def fixture_another_dependency_schema():
    return '{"type": "record", "name": "AnotherDependency", "fields": [{"name": "anotherField", "type": "boolean"}]}'


def test_resolver_without_dependencies(base_schema):
    resolver = AvroResolver(schema_str=base_schema)
    resolved_schemas = resolver.resolve()
    assert resolved_schemas == [base_schema], "Expected single schema in resolved list without dependencies"


def test_resolver_with_single_dependency(base_schema, dependency_schema):
    dependency = Dependency(
        name="Dependency1",
        subject=Subject("TestSubject"),
        version=Version(1),
        target_schema=create_validated_schema(dependency_schema),
    )
    dependencies = {"Dependency1": dependency}
    resolver = AvroResolver(schema_str=base_schema, dependencies=dependencies)
    resolved_schemas = resolver.resolve()
    assert resolved_schemas == [dependency_schema, base_schema], "Expected dependency to be resolved before base schema"


def test_resolver_with_multiple_dependencies(base_schema, dependency_schema, another_dependency_schema):
    dependency1 = Dependency(
        name="Dependency1",
        subject=Subject("TestSubject1"),
        version=Version(1),
        target_schema=create_validated_schema(dependency_schema),
    )
    dependency2 = Dependency(
        name="Dependency2",
        subject=Subject("TestSubject2"),
        version=Version(1),
        target_schema=create_validated_schema(another_dependency_schema),
    )
    dependencies = {"Dependency1": dependency1, "Dependency2": dependency2}
    resolver = AvroResolver(schema_str=base_schema, dependencies=dependencies)
    resolved_schemas = resolver.resolve()

    # Validate both dependencies appear before the base schema, without assuming their specific order
    assert dependency_schema in resolved_schemas
    assert another_dependency_schema in resolved_schemas
    assert resolved_schemas[-1] == base_schema, "Base schema should be the last in the resolved list"


def test_builder_unique_id_increment(base_schema, dependency_schema):
    dependency = Dependency(
        name="Dependency1",
        subject=Subject("TestSubject"),
        version=Version(1),
        target_schema=create_validated_schema(dependency_schema),
    )
    dependencies = {"Dependency1": dependency}
    resolver = AvroResolver(schema_str=base_schema, dependencies=dependencies)
    resolver.builder(base_schema, dependencies)
    assert resolver.unique_id == 2, "Unique ID should be incremented for each processed schema"


def test_resolver_with_nested_dependencies(base_schema, dependency_schema, another_dependency_schema):
    # Create nested dependency structure
    nested_dependency = Dependency(
        name="NestedDependency",
        subject=Subject("NestedSubject"),
        version=Version(1),
        target_schema=create_validated_schema(another_dependency_schema),
    )
    dependency_with_nested = Dependency(
        name="Dependency1",
        subject=Subject("TestSubject"),
        version=Version(1),
        target_schema=create_validated_schema(dependency_schema, dependencies={"NestedDependency": nested_dependency}),
    )
    dependencies = {"Dependency1": dependency_with_nested}
    resolver = AvroResolver(schema_str=base_schema, dependencies=dependencies)
    resolved_schemas = resolver.resolve()

    # Ensure all schemas are resolved in the correct order
    assert another_dependency_schema in resolved_schemas
    assert dependency_schema in resolved_schemas
    assert resolved_schemas[-1] == base_schema, "Base schema should be the last in the resolved list"
    assert resolved_schemas.index(another_dependency_schema) < resolved_schemas.index(
        dependency_schema
    ), "Nested dependency should be resolved before its parent"


def test_avro_reference() -> None:
    country_schema = ValidatedTypedSchema.parse(
        schema_type=SchemaType.AVRO,
        schema_str=textwrap.dedent(
            """\
            {
                "type": "record",
                "name": "Country",
                "namespace": "com.netapp",
                "fields": [{"name": "name", "type": "string"}, {"name": "code", "type": "string"}]
            }
            """
        ),
    )
    address_schema = ValidatedTypedSchema.parse(
        schema_type=SchemaType.AVRO,
        schema_str=textwrap.dedent(
            """\
            {
                "type": "record",
                "name": "Address",
                "namespace": "com.netapp",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "postalCode", "type": "string"},
                    {"name": "country", "type": "Country"}
                ]
            }
            """
        ),
        references=[Reference(name="country.avsc", subject=Subject("country"), version=Version(1))],
        dependencies={
            "country": Dependency(
                name="country",
                subject=Subject("country"),
                version=Version(1),
                target_schema=country_schema,
            ),
        },
    )

    # Check that the reference schema (Country) has been inlined
    assert address_schema.schema == textwrap.dedent(
        """\
        {
            "type": "record",
            "name": "Address",
            "namespace": "com.netapp",
            "fields": [
                {
                    "type": "string",
                    "name": "street"
                },
                {
                    "type": "string",
                    "name": "city"
                },
                {
                    "type": "string",
                    "name": "postalCode"
                },
                {
                    "type": {
                        "type": "record",
                        "name": "Country",
                        "namespace": "com.netapp",
                        "fields": [
                            {
                                "type": "string",
                                "name": "name"
                            },
                            {
                                "type": "string",
                                "name": "code"
                            }
                        ]
                    },
                    "name": "country"
                }
            ]
        }
        """
    )


def test_avro_reference2() -> None:
    # country.avsc
    country_schema = ValidatedTypedSchema.parse(
        schema_type=SchemaType.AVRO,
        schema_str=textwrap.dedent(
            """\
            {
                "type": "record",
                "name": "Country",
                "namespace": "com.netapp",
                "fields": [{"name": "name", "type": "string"}, {"name": "code", "type": "string"}]
            }
            """
        ),
    )

    # address.avsc
    address_schema = ValidatedTypedSchema.parse(
        schema_type=SchemaType.AVRO,
        schema_str=textwrap.dedent(
            """\
            {
                "type": "record",
                "name": "Address",
                "namespace": "com.netapp",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "postalCode", "type": "string"},
                    {"name": "country", "type": "Country"}
                ]
            }
            """
        ),
        references=[Reference(name="country.avsc", subject=Subject("country"), version=Version(1))],
        dependencies={
            "country": Dependency(
                name="country",
                subject=Subject("country"),
                version=Version(1),
                target_schema=country_schema,
            ),
        },
    )

    # job.avsc
    job_schema = ValidatedTypedSchema.parse(
        schema_type=SchemaType.AVRO,
        schema_str=textwrap.dedent(
            """\
            {
                "type": "record",
                "name": "Job",
                "namespace": "com.netapp",
                "fields": [
                    {"name": "title", "type": "string"},
                    {"name": "salary", "type": "double"}
                ]
            }
            """
        ),
    )

    # person.avsc
    person_schema = ValidatedTypedSchema.parse(
        schema_type=SchemaType.AVRO,
        schema_str=textwrap.dedent(
            """\
            {
                "type": "record",
                "name": "Person",
                "namespace": "com.netapp",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "address", "type": "Address"},
                    {"name": "job", "type": "Job"}
                ]
            }
            """
        ),
        references=[
            Reference(name="address.avsc", subject=Subject("address"), version=Version(1)),
            Reference(name="job.avsc", subject=Subject("job"), version=Version(1)),
        ],
        dependencies={
            "address": Dependency(
                name="address",
                subject=Subject("address"),
                version=Version(1),
                target_schema=address_schema,
            ),
            "job": Dependency(
                name="job",
                subject=Subject("job"),
                version=Version(1),
                target_schema=job_schema,
            ),
        },
    )

    # Check that the Address and Job schemas are correctly inlined within the Person schema
    expected_schema = textwrap.dedent(
        """\
        {
            "type": "record",
            "name": "Person",
            "namespace": "com.netapp",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
                {
                    "name": "address",
                    "type": {
                        "type": "record",
                        "name": "Address",
                        "namespace": "com.netapp",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "city", "type": "string"},
                            {"name": "postalCode", "type": "string"},
                            {
                                "name": "country",
                                "type": {
                                    "type": "record",
                                    "name": "Country",
                                    "namespace": "com.netapp",
                                    "fields": [
                                        {"name": "name", "type": "string"},
                                        {"name": "code", "type": "string"}
                                    ]
                                }
                            }
                        ]
                    }
                },
                {
                    "name": "job",
                    "type": {
                        "type": "record",
                        "name": "Job",
                        "namespace": "com.netapp",
                        "fields": [
                            {"name": "title", "type": "string"},
                            {"name": "salary", "type": "double"}
                        ]
                    }
                }
            ]
        }
        """
    )

    # Check that the reference schemas (Address and Job, including nested Country) have been correctly inlined
    assert person_schema.schema == expected_schema
