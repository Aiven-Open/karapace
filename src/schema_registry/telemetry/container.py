"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector import containers, providers
from opentelemetry.sdk.trace import TracerProvider
from schema_registry.telemetry.tracer import Tracer


class TelemetryContainer(containers.DeclarativeContainer):
    tracer_provider = providers.Singleton(TracerProvider)
    tracer = providers.Singleton(Tracer)
