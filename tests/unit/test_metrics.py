"""
unit test of Metrics module

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from typing import cast
from unittest.mock import MagicMock

import pytest

from karapace.config import Config
from karapace.metrics import Singleton, Metrics, MetricsException
from karapace.prometheus import PrometheusClient
from karapace.statsd import StatsdClient


@pytest.fixture(autouse=True)
def reset_singleton():
    # Reset the Singleton instance before each test
    Singleton._instance = None
    yield


@pytest.fixture
def metrics():
    return Metrics()


def test_singleton(metrics):
    # Ensure only one instance is created
    metrics2 = Metrics()
    assert metrics is metrics2


def test_setup_prometheus(metrics):
    config = cast(Config, {
        "metrics_extended": True,
        "stats_service": "prometheus"
    })
    metrics.setup(config)
    assert metrics.is_ready
    assert isinstance(metrics.stats_client, PrometheusClient)


def test_setup_invalid_service(metrics):
    config = cast(Config, {
        "metrics_extended": True,
        "stats_service": "invalid_service"
    })
    with pytest.raises(MetricsException):
        metrics.setup(config)


def test_request(metrics):
    metrics.is_ready = True
    metrics.stats_client = MagicMock(spec=StatsdClient)
    metrics.request(100)
    metrics.stats_client.increase.assert_any_call("request-size-total", 100)
    metrics.stats_client.increase.assert_any_call("request-count", 1)


def test_response(metrics):
    metrics.is_ready = True
    metrics.stats_client = MagicMock(spec=StatsdClient)
    metrics.response(200)
    metrics.stats_client.increase.assert_any_call("response-size-total", 200)
    metrics.stats_client.increase.assert_any_call("response-count", 1)


def test_are_we_master(metrics):
    metrics.is_ready = True
    metrics.stats_client = MagicMock(spec=StatsdClient)
    metrics.are_we_master(True)
    metrics.stats_client.gauge.assert_called_with("master-slave-role", 1)
    metrics.are_we_master(False)
    metrics.stats_client.gauge.assert_called_with("master-slave-role", 0)


def test_latency(metrics):
    metrics.is_ready = True
    metrics.stats_client = MagicMock(spec=StatsdClient)
    metrics.latency(123.45)
    metrics.stats_client.timing.assert_called_with("latency_ms", 123.45)


def test_error(metrics):
    metrics.is_ready = True
    metrics.stats_client = MagicMock(spec=StatsdClient)
    metrics.error()
    metrics.stats_client.increase.assert_called_with("error-total", 1)


def test_cleanup(metrics):
    metrics.stats_client = MagicMock(spec=StatsdClient)
    metrics.cleanup()
    metrics.stats_client.close.assert_called_once()
