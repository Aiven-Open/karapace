"""
schema_registry - telemetry setup tests

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from opentelemetry.sdk.trace import TracerProvider
from karapace.api.telemetry.setup import setup_tracing
from karapace.api.telemetry.tracer import Tracer
from unittest.mock import MagicMock, patch

import logging


def test_setup_telemetry(caplog: LogCaptureFixture):
    tracer_provider = MagicMock(spec=TracerProvider)
    tracer = MagicMock(spec=Tracer)
    with (
        caplog.at_level(logging.INFO, logger="karapace.api.telemetry.setup"),
        patch("karapace.api.telemetry.setup.trace") as mock_trace,
    ):
        tracer.get_span_processor.return_value = "span_processor"
        setup_tracing(tracer_provider=tracer_provider, tracer=tracer)

        tracer_provider.add_span_processor.assert_called_once_with("span_processor")
        mock_trace.set_tracer_provider.assert_called_once_with(tracer_provider)
        for log in caplog.records:
            assert log.name == "karapace.api.telemetry.setup"
            assert log.levelname == "INFO"
            assert log.message == "Setting OTel tracing provider"
