"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from karapace.api.telemetry.metrics import HTTPRequestMetrics
from karapace.core.instrumentation.tracer import Tracer
from karapace.core.config import Config
from karapace.core.container import KarapaceContainer
from karapace.core.metrics_container import MetricsContainer
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.semconv.attributes import telemetry_attributes as T


def create_telemetry_resource(config: Config) -> Resource:
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
    telemetry_resource = providers.Factory(create_telemetry_resource, config=karapace_container.config)
    metrics_container = providers.Container(MetricsContainer)
    http_request_metrics = providers.Singleton(HTTPRequestMetrics, meter=metrics_container.meter)
    tracer = providers.Singleton(Tracer)
    tracer_provider = providers.Singleton(TracerProvider, resource=telemetry_resource)
