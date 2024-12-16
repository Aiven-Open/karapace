"""
schema_registry - telemetry middleware tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from karapace.config import KarapaceTelemetry
from karapace.container import KarapaceContainer
from opentelemetry.sdk.trace.export import SpanProcessor
from schema_registry.telemetry.tracer import Tracer
from unittest.mock import patch


def test_tracer(karapace_container: KarapaceContainer):
    with patch("schema_registry.telemetry.tracer.trace") as mock_trace:
        Tracer.get_tracer(config=karapace_container.config())
        mock_trace.get_tracer.assert_called_once_with("Karapace.tracer")


def test_get_name_from_caller():
    def test_function():
        return Tracer.get_name_from_caller()

    assert test_function() == "test_function"


def test_get_name_from_caller_with_class():
    class Test:
        def test_function(self):
            return Tracer.get_name_from_caller_with_class(self, self.test_function)

    assert Test().test_function() == "Test.test_function()"


def test_get_span_processor_with_otel_endpoint(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        new_config={"telemetry": KarapaceTelemetry(otel_endpoint_url="http://otel:4317")}
    )
    with (
        patch("schema_registry.telemetry.tracer.OTLPSpanExporter") as mock_otlp_exporter,
        patch("schema_registry.telemetry.tracer.BatchSpanProcessor") as mock_batch_span_processor,
    ):
        processor: SpanProcessor = Tracer.get_span_processor(config=config)
        mock_otlp_exporter.assert_called_once_with(endpoint="http://otel:4317")
        mock_batch_span_processor.assert_called_once_with(mock_otlp_exporter.return_value)
        assert processor is mock_batch_span_processor.return_value


def test_get_span_processor_without_otel_endpoint(karapace_container: KarapaceContainer) -> None:
    with (
        patch("schema_registry.telemetry.tracer.ConsoleSpanExporter") as mock_console_exporter,
        patch("schema_registry.telemetry.tracer.SimpleSpanProcessor") as mock_simple_span_processor,
    ):
        processor: SpanProcessor = Tracer.get_span_processor(config=karapace_container.config())
        mock_simple_span_processor.assert_called_once_with(mock_console_exporter.return_value)
        assert processor is mock_simple_span_processor.return_value
