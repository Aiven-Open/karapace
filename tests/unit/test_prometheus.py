"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from typing import cast
from unittest.mock import patch, MagicMock
from prometheus_client import start_http_server
from karapace.config import Config

import pytest

from karapace.prometheus import PrometheusClient, PrometheusException, HOST, PORT


def test_prometheus_client():
    config = cast(Config, {"prometheus_host": HOST, "prometheus_port": PORT})
    client = PrometheusClient(config)

    assert client.server_is_active
    assert client._gauge == {}
    assert client._summary == {}
    assert client._counter == {}

    with pytest.raises(PrometheusException, match="Double instance of Prometheus interface"):
        config = cast(Config, {"prometheus_host": HOST, "prometheus_port": PORT})
        PrometheusClient(config)

    config = cast(Config, {"prometheus_port": PORT})
    with pytest.raises(PrometheusException, match="prometheus_host host is undefined"):
        PrometheusClient(config)

    config = cast(Config, {"prometheus_host": HOST})
    with pytest.raises(PrometheusException, match="prometheus_host port is undefined"):
        PrometheusClient(config)

    client.gauge('test_metric', 1.1)
    assert 'test_metric' in client._gauge
    assert client._gauge['test_metric']._value.get() == 1.1

    client.increase('test_metric1', 5)
    assert 'test_metric1' in client._counter
    assert client._counter['test_metric1']._value.get() == 5

    client.increase('test_metric1', 3)
    assert client._counter['test_metric1']._value.get() == 8

    client.timing('test_metric3', 0.5)
    assert 'test_metric3' in client._summary
    # Since we can't access the summary's internal state directly, we assume it's being called
    # Proper testing of this would require more integration or acceptance tests
