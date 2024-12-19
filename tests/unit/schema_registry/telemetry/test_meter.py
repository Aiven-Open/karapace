"""
schema_registry - telemetry meter tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.config import KarapaceTelemetry
from karapace.container import KarapaceContainer
from schema_registry.telemetry.meter import Meter
from unittest.mock import patch


def test_meter(karapace_container: KarapaceContainer):
    with patch("schema_registry.telemetry.meter.metrics") as mock_metrics:
        Meter.get_meter(config=karapace_container.config())
        mock_metrics.get_meter_provider.return_value.get_meter.assert_called_once_with("Karapace.meter")


def test_get_metric_reader_without_otel_endpoint(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={"telemetry": KarapaceTelemetry(otel_endpoint_url=None)}
    )
    with (
        patch("schema_registry.telemetry.meter.ConsoleMetricExporter") as mock_console_exporter,
        patch("schema_registry.telemetry.meter.PeriodicExportingMetricReader") as mock_periodic_exporting_metric_reader,
    ):
        reader = Meter.get_metric_reader(config=config)
        mock_console_exporter.assert_called_once()
        mock_periodic_exporting_metric_reader.assert_called_once_with(
            exporter=mock_console_exporter.return_value,
            export_interval_millis=10000,
        )
        assert reader is mock_periodic_exporting_metric_reader.return_value


def test_get_metric_reader_with_otel_endpoint(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={"telemetry": KarapaceTelemetry(otel_endpoint_url="http://otel:4317")}
    )
    with (
        patch("schema_registry.telemetry.meter.OTLPMetricExporter") as mock_otlp_exporter,
        patch("schema_registry.telemetry.meter.PeriodicExportingMetricReader") as mock_periodic_exporting_metric_reader,
    ):
        reader = Meter.get_metric_reader(config=config)
        mock_otlp_exporter.assert_called_once_with(endpoint="http://otel:4317")
        mock_periodic_exporting_metric_reader.assert_called_once_with(
            exporter=mock_otlp_exporter.return_value,
            export_interval_millis=10000,
        )
        assert reader is mock_periodic_exporting_metric_reader.return_value
