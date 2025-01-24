"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.core.container import KarapaceContainer
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.registry import KarapaceSchemaRegistry
from karapace.api.telemetry.container import TelemetryContainer


class SchemaRegistryContainer(containers.DeclarativeContainer):
    karapace_container = providers.Container(KarapaceContainer)
    telemetry_container = providers.Container(TelemetryContainer)

    schema_registry = providers.Singleton(KarapaceSchemaRegistry, config=karapace_container.config)

    schema_registry_controller = providers.Singleton(
        KarapaceSchemaRegistryController,
        config=karapace_container.config,
        schema_registry=schema_registry,
        stats=karapace_container.statsd,
    )
