"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from schema_registry.telemetry.container import TelemetryContainer
from schema_registry.telemetry.tracer import Tracer

import logging

LOG = logging.getLogger(__name__)


@inject
def setup_tracing(
    tracer_provider: TracerProvider = Provide[TelemetryContainer.tracer_provider],
    tracer: Tracer = Provide[TelemetryContainer.tracer],
) -> None:
    LOG.info("Setting OTel tracing provider")
    tracer_provider.add_span_processor(tracer.get_span_processor())
    trace.set_tracer_provider(tracer_provider)
