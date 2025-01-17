"""
schema_registry - telemetry metrics tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from schema_registry.telemetry.metrics import Metrics
from schema_registry.telemetry.meter import Meter
from unittest.mock import call, MagicMock


def test_metrics():
    meter = MagicMock(spec=Meter)
    metrics = Metrics(meter=meter)

    assert hasattr(metrics, "karapace_http_requests_in_progress")
    assert hasattr(metrics, "karapace_http_requests_duration_seconds")
    assert hasattr(metrics, "karapace_http_requests_total")
    assert hasattr(metrics, "START_TIME_KEY")
    assert metrics.START_TIME_KEY == "start_time"

    meter.assert_has_calls(
        [
            call.get_meter(),
            call.get_meter().create_up_down_counter(
                name="karapace_http_requests_in_progress", description="In-progress requests for HTTP/TCP Protocol"
            ),
            call.get_meter(),
            call.get_meter().create_histogram(
                unit="seconds",
                name="karapace_http_requests_duration_seconds",
                description="Request Duration for HTTP/TCP Protocol",
            ),
            call.get_meter(),
            call.get_meter().create_counter(
                name="karapace_http_requests_total", description="Total Request Count for HTTP/TCP Protocol"
            ),
        ]
    )
