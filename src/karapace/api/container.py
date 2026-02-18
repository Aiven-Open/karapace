"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.api.controller import KarapaceSchemaRegistryController
from karapace.api.telemetry.container import TelemetryContainer
from karapace.core.container import KarapaceContainer
from karapace.core.metrics_container import MetricsContainer
from karapace.core.schema_registry import KarapaceSchemaRegistry
from karapace.core.tls import TlsContextHolder


class SchemaRegistryContainer(containers.DeclarativeContainer):
    karapace_container = providers.Container(KarapaceContainer)
    metrics_container = providers.Container(MetricsContainer)
    telemetry_container = providers.Container(TelemetryContainer)

    tls_context_holder = providers.Singleton(TlsContextHolder, config=karapace_container.config)

    schema_registry = providers.Singleton(
        KarapaceSchemaRegistry,
        config=karapace_container.config,
        stats=metrics_container.stats,
    )

    schema_registry_controller = providers.Singleton(
        KarapaceSchemaRegistryController,
        config=karapace_container.config,
        schema_registry=schema_registry,
        stats=metrics_container.stats,
    )
