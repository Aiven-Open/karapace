"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.config import Config
from karapace.container import KarapaceContainer
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.semconv.attributes import telemetry_attributes as T
from schema_registry.telemetry.tracer import Tracer


def create_tracing_resource(config: Config) -> Resource:
    return Resource.create(
        {
            "service.name": config.telemetry.resource_service_name,
            "service.instance.id": config.telemetry.resource_service_instance_id,
            T.TELEMETRY_SDK_NAME: config.telemetry.resource_telemetry_sdk_name,
            T.TELEMETRY_SDK_LANGUAGE: config.telemetry.resource_telemetry_sdk_language,
            T.TELEMETRY_SDK_VERSION: config.telemetry.resource_telemetry_sdk_version,
        }
    )


class TelemetryContainer(containers.DeclarativeContainer):
    karapace_container = providers.Container(KarapaceContainer)
    tracing_resource = providers.Factory(create_tracing_resource, config=karapace_container.config)
    tracer_provider = providers.Singleton(TracerProvider, resource=tracing_resource)
    tracer = providers.Singleton(Tracer)
