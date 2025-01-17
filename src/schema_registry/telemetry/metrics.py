"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from opentelemetry.metrics import Counter, Histogram, UpDownCounter
from schema_registry.telemetry.meter import Meter
from typing import Final


class Metrics:
    START_TIME_KEY: Final = "start_time"

    def __init__(self, meter: Meter):
        self.karapace_http_requests_in_progress: UpDownCounter = meter.get_meter().create_up_down_counter(
            name="karapace_http_requests_in_progress",
            description="In-progress requests for HTTP/TCP Protocol",
        )
        self.karapace_http_requests_duration_seconds: Histogram = meter.get_meter().create_histogram(
            unit="seconds",
            name="karapace_http_requests_duration_seconds",
            description="Request Duration for HTTP/TCP Protocol",
        )
        self.karapace_http_requests_total: Counter = meter.get_meter().create_counter(
            name="karapace_http_requests_total",
            description="Total Request Count for HTTP/TCP Protocol",
        )
