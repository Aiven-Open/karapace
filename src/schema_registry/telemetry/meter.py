"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from karapace.config import Config
from karapace.container import KarapaceContainer
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    MetricExporter,
    MetricReader,
    PeriodicExportingMetricReader,
)


class Meter:
    @staticmethod
    @inject
    def get_meter(config: Config = Provide[KarapaceContainer.config]) -> metrics.Meter:
        return metrics.get_meter_provider().get_meter(f"{config.tags.app}.meter")

    @staticmethod
    @inject
    def get_metric_reader(config: Config = Provide[KarapaceContainer.config]) -> MetricReader:
        exporter: MetricExporter = ConsoleMetricExporter()
        if config.telemetry.otel_endpoint_url:
            exporter = OTLPMetricExporter(endpoint=config.telemetry.otel_endpoint_url)
        return PeriodicExportingMetricReader(
            exporter=exporter,
            export_interval_millis=config.telemetry.metrics_export_interval_milliseconds,
        )
