"""
schema_registry - telemetry middleware tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from fastapi import Request, Response
from karapace.config import KarapaceTelemetry
from karapace.container import KarapaceContainer
from opentelemetry.sdk.trace.export import SpanProcessor
from opentelemetry.trace.span import Span
from schema_registry.telemetry.tracer import Tracer
from unittest.mock import call, MagicMock, patch


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


def test_add_span_attribute():
    span = MagicMock(spec=Span)

    # Test when span is not recording
    span.is_recording.return_value = False
    Tracer.add_span_attribute(span=span, key="key", value="value")
    assert not span.set_attribute.called

    # Test when span is recording
    span.is_recording.return_value = True
    Tracer.add_span_attribute(span=span, key="key", value="value")
    span.set_attribute.assert_called_once_with("key", "value")


def test_update_span_with_request():
    span = MagicMock(spec=Span)
    span.is_recording.return_value = True

    request = MagicMock(spec=Request)
    request.headers = {"content-type": "application/json", "connection": "keep-alive", "user-agent": "pytest"}
    request.method = "GET"
    request.url = MagicMock(port=8081, scheme="http", path="/test", hostname="server")
    request.client = MagicMock(host="client", port=8080)

    Tracer.update_span_with_request(request=request, span=span)
    span.set_attribute.assert_has_calls(
        [
            call("client.address", "client"),
            call("client.port", 8080),
            call("server.address", "server"),
            call("server.port", 8081),
            call("url.scheme", "http"),
            call("url.path", "/test"),
            call("http.request.method", "GET"),
            call("http.request.header.connection", "keep-alive"),
            call("http.request.header.user_agent", "pytest"),
            call("http.request.header.content_type", "application/json"),
        ]
    )


def test_update_span_with_response():
    span = MagicMock(spec=Span)

    response = MagicMock(spec=Response)
    response.status_code = 200
    response.headers = {"content-type": "application/json", "content-length": 8}

    span.is_recording.return_value = True
    Tracer.update_span_with_response(response=response, span=span)
    span.set_attribute.assert_has_calls(
        [
            call("http.response.status_code", 200),
            call("http.response.header.content_type", "application/json"),
            call("http.response.header.content_length", 8),
        ]
    )
