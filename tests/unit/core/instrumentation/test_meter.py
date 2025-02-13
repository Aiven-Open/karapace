"""
schema_registry - telemetry meter tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from unittest.mock import patch

from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, MetricExporter

from karapace.core.instrumentation.meter import Meter, NOOPMetricExporter
from karapace.core.config import KarapaceTelemetry
from karapace.core.container import KarapaceContainer


def test_meter(karapace_container: KarapaceContainer):
    with patch("karapace.core.instrumentation.meter.metrics") as mock_metrics:
        Meter.get_meter(config=karapace_container.config())
        mock_metrics.get_meter_provider.return_value.get_meter.assert_called_once_with("Karapace.meter")


def test_get_metric_exporter_noop(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={
            "telemetry": KarapaceTelemetry(
                otel_endpoint_url="http://otel:4317",
                otel_exporter="NOOP",
            )
        }
    )
    exporter: MetricExporter = Meter.get_metric_exporter(config=config)
    assert isinstance(exporter, NOOPMetricExporter)


def test_get_metric_exporter_console(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={
            "telemetry": KarapaceTelemetry(
                otel_endpoint_url="http://otel:4317",
                otel_exporter="CONSOLE",
            )
        }
    )
    exporter: MetricExporter = Meter.get_metric_exporter(config=config)
    assert isinstance(exporter, ConsoleMetricExporter)


def test_get_metric_exporter_otlp(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={
            "telemetry": KarapaceTelemetry(
                otel_endpoint_url="http://otel:4317",
                otel_exporter="OTLP",
            )
        }
    )
    with patch("karapace.core.instrumentation.meter.OTLPMetricExporter") as mock_otlp_exporter:
        exporter: MetricExporter = Meter.get_metric_exporter(config=config)
        mock_otlp_exporter.assert_called_once_with(endpoint="http://otel:4317")
        assert exporter is mock_otlp_exporter.return_value


def test_get_metric_reader_without_otel_endpoint(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={"telemetry": KarapaceTelemetry(otel_endpoint_url=None)}
    )
    with (
        patch("karapace.core.instrumentation.meter.NOOPMetricExporter") as mock_noop_exporter,
        patch("karapace.core.instrumentation.meter.PeriodicExportingMetricReader") as mock_periodic_exporting_metric_reader,
    ):
        reader = Meter.get_metric_reader(config=config)
        mock_noop_exporter.assert_called_once()
        mock_periodic_exporting_metric_reader.assert_called_once_with(
            exporter=mock_noop_exporter.return_value,
            export_interval_millis=10000,
        )
        assert reader is mock_periodic_exporting_metric_reader.return_value


def test_get_metric_reader_with_otel_endpoint(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={
            "telemetry": KarapaceTelemetry(
                otel_endpoint_url="http://otel:4317",
                otel_exporter="OTLP",
            )
        }
    )
    with (
        patch("karapace.core.instrumentation.meter.OTLPMetricExporter") as mock_otlp_exporter,
        patch("karapace.core.instrumentation.meter.PeriodicExportingMetricReader") as mock_periodic_exporting_metric_reader,
    ):
        reader = Meter.get_metric_reader(config=config)
        mock_otlp_exporter.assert_called_once_with(endpoint="http://otel:4317")
        mock_periodic_exporting_metric_reader.assert_called_once_with(
            exporter=mock_otlp_exporter.return_value,
            export_interval_millis=10000,
        )
        assert reader is mock_periodic_exporting_metric_reader.return_value
