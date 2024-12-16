"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Callable
from dependency_injector.wiring import inject, Provide
from fastapi import Request, Response
from karapace.config import Config
from karapace.container import KarapaceContainer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor, SpanProcessor
from opentelemetry.semconv.attributes import (
    client_attributes as C,
    http_attributes as H,
    server_attributes as S,
    url_attributes as U,
)
from opentelemetry.trace.span import Span
from typing import Any

import inspect


class Tracer:
    @staticmethod
    @inject
    def get_tracer(config: Config = Provide[KarapaceContainer.config]) -> trace.Tracer:
        return trace.get_tracer(f"{config.tags.app}.tracer")

    @staticmethod
    @inject
    def get_span_processor(config: Config = Provide[KarapaceContainer.config]) -> SpanProcessor:
        if config.telemetry.otel_endpoint_url:
            otlp_span_exporter = OTLPSpanExporter(endpoint=config.telemetry.otel_endpoint_url)
            return BatchSpanProcessor(otlp_span_exporter)
        return SimpleSpanProcessor(ConsoleSpanExporter())

    @staticmethod
    def get_name_from_caller() -> str:
        return inspect.stack()[1].function

    @staticmethod
    def get_name_from_caller_with_class(function_class: object, function: Callable[[Any], Any]) -> str:
        return f"{type(function_class).__name__}.{function.__name__}()"

    @staticmethod
    def add_span_attribute(span: Span, key: str, value: str | int) -> None:
        if span.is_recording():
            span.set_attribute(key, value)

    @staticmethod
    def update_span_with_request(request: Request, span: Span) -> None:
        if span.is_recording():
            span.set_attribute(C.CLIENT_ADDRESS, request.client.host or "" if request.client else "")
            span.set_attribute(C.CLIENT_PORT, request.client.port or "" if request.client else "")
            span.set_attribute(S.SERVER_ADDRESS, request.url.hostname or "")
            span.set_attribute(S.SERVER_PORT, request.url.port or "")
            span.set_attribute(U.URL_SCHEME, request.url.scheme)
            span.set_attribute(U.URL_PATH, request.url.path)
            span.set_attribute(H.HTTP_REQUEST_METHOD, request.method)
            span.set_attribute(f"{H.HTTP_REQUEST_HEADER_TEMPLATE}.connection", request.headers.get("connection", ""))
            span.set_attribute(f"{H.HTTP_REQUEST_HEADER_TEMPLATE}.user_agent", request.headers.get("user-agent", ""))
            span.set_attribute(f"{H.HTTP_REQUEST_HEADER_TEMPLATE}.content_type", request.headers.get("content-type", ""))

    @staticmethod
    def update_span_with_response(response: Response, span: Span) -> None:
        if span.is_recording():
            span.set_attribute(H.HTTP_RESPONSE_STATUS_CODE, response.status_code)
            span.set_attribute(f"{H.HTTP_RESPONSE_HEADER_TEMPLATE}.content_type", response.headers.get("content-type", ""))
            span.set_attribute(
                f"{H.HTTP_RESPONSE_HEADER_TEMPLATE}.content_length", response.headers.get("content-length", "")
            )
