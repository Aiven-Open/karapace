"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.container import KarapaceContainer
from opentelemetry.sdk.trace import TracerProvider
from schema_registry.controller import KarapaceSchemaRegistryController
from schema_registry.registry import KarapaceSchemaRegistry
from schema_registry.telemetry.container import TelemetryContainer
from schema_registry.telemetry.tracer import Tracer


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

    tracer_provider = providers.Singleton(TracerProvider)
    tracer = providers.Singleton(Tracer)
