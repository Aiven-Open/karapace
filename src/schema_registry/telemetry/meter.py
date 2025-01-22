"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from dependency_injector.wiring import inject, Provide
from karapace.config import Config, KarapaceTelemetryOTelExporter
from karapace.container import KarapaceContainer
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    MetricExporter,
    MetricReader,
    PeriodicExportingMetricReader,
    MetricsData,
    MetricExportResult,
)
from typing import Any


class NOOPMetricExporter(MetricExporter):
    """Implementation of :class:`MetricExporter` that does nothing.

    This class is intended to be used when metrics exporting to an OTel backend is disabled
    and the ConsoleExporter is too verbose to be used.
    """

    def export(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 0,
        **kwargs: Any,
    ) -> MetricExportResult:
        return MetricExportResult.SUCCESS

    def shutdown(self, timeout_millis: float = 0, **kwargs: Any) -> None:
        pass

    def force_flush(self, _: float = 0) -> bool:
        return True


class Meter:
    @staticmethod
    @inject
    def get_meter(config: Config = Provide[KarapaceContainer.config]) -> metrics.Meter:
        return metrics.get_meter_provider().get_meter(f"{config.tags.app}.meter")

    @staticmethod
    def get_metric_exporter(config: Config) -> MetricExporter:
        match config.telemetry.otel_exporter:
            case KarapaceTelemetryOTelExporter.NOOP:
                return NOOPMetricExporter()
            case KarapaceTelemetryOTelExporter.CONSOLE:
                return ConsoleMetricExporter()
            case KarapaceTelemetryOTelExporter.OTLP:
                return OTLPMetricExporter(endpoint=config.telemetry.otel_endpoint_url)

    @staticmethod
    @inject
    def get_metric_reader(config: Config = Provide[KarapaceContainer.config]) -> MetricReader:
        exporter: MetricExporter = Meter.get_metric_exporter(config)
        return PeriodicExportingMetricReader(
            exporter=exporter,
            export_interval_millis=config.telemetry.metrics_export_interval_milliseconds,
        )
