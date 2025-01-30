"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Mapping
from fastapi import HTTPException, Request, Response
from karapace.api.telemetry.meter import Meter
from opentelemetry.metrics import Counter, Histogram, UpDownCounter
from typing import Final

import time


class HTTPRequestMetrics:
    START_TIME_KEY: Final = "start_time"

    def __init__(self, meter: Meter):
        self.meter = meter
        self.karapace_http_requests_in_progress: Final[UpDownCounter] = self.meter.get_meter().create_up_down_counter(
            name="karapace_http_requests_in_progress",
            description="In-progress requests for HTTP/TCP Protocol",
        )
        self.karapace_http_requests_duration_seconds: Final[Histogram] = self.meter.get_meter().create_histogram(
            unit="seconds",
            name="karapace_http_requests_duration_seconds",
            description="Request Duration for HTTP/TCP Protocol",
        )
        self.karapace_http_requests_total: Final[Counter] = self.meter.get_meter().create_counter(
            name="karapace_http_requests_total",
            description="Total Request Count for HTTP/TCP Protocol",
        )

    def get_resource_from_request(self, request: Request) -> str:
        return request.url.path.split("/")[1]

    def start_request(self, request: Request) -> Mapping[str, str | int]:
        # Set start time for request
        setattr(request.state, self.START_TIME_KEY, time.monotonic())

        PATH = request.url.path
        METHOD = request.method
        ATTRIBUTES = {"method": METHOD, "path": PATH, "resource": self.get_resource_from_request(request=request)}

        self.karapace_http_requests_in_progress.add(amount=1, attributes=ATTRIBUTES)
        return ATTRIBUTES

    def finish_request(self, ATTRIBUTES: Mapping[str, str | int], request: Request, response: Response | None) -> None:
        status = response.status_code if response else 0
        self.karapace_http_requests_total.add(amount=1, attributes={**ATTRIBUTES, "status": status})
        self.karapace_http_requests_duration_seconds.record(
            amount=(time.monotonic() - getattr(request.state, self.START_TIME_KEY)),
            attributes=ATTRIBUTES,
        )
        self.karapace_http_requests_in_progress.add(amount=-1, attributes=ATTRIBUTES)

    def record_request_exception(self, ATTRIBUTES: Mapping[str, str | int], exc: Exception) -> None:
        status = exc.status_code if isinstance(exc, HTTPException) else 0
        self.karapace_http_requests_total.add(amount=1, attributes={**ATTRIBUTES, "status": status})
