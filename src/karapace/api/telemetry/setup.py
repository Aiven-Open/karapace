"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from karapace.api.telemetry.container import TelemetryContainer
from karapace.api.telemetry.meter import Meter
from karapace.api.telemetry.tracer import Tracer

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


@inject
def setup_metering(
    meter: Meter = Provide[TelemetryContainer.meter],
    telemetry_resource: Resource = Provide[TelemetryContainer.telemetry_resource],
) -> None:
    LOG.info("Setting OTel meter provider")
    metrics.set_meter_provider(MeterProvider(resource=telemetry_resource, metric_readers=[meter.get_metric_reader()]))