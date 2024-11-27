"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.container import KarapaceContainer
from schema_registry.schema_registry_apis import KarapaceSchemaRegistryController


class SchemaRegistryContainer(containers.DeclarativeContainer):
    karapace_container = providers.Container(KarapaceContainer)
    schema_registry_controller = providers.Singleton(
        KarapaceSchemaRegistryController,
        config=karapace_container.config,
        schema_registry=karapace_container.schema_registry,
        stats=karapace_container.statsd,
    )
