"""
karapace - prometheus

Supports telegraf's statsd protocol extension for 'key=value' tags:

  https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from karapace.base_stats import StatsClient
from karapace.config import Config
from prometheus_client import Counter, Gauge, start_http_server, Summary
from typing import Final

import logging

LOG = logging.getLogger(__name__)
HOST: Final = "127.0.0.1"
PORT: Final = 8005


class PrometheusException(Exception):
    pass


class PrometheusClient(StatsClient):
    server_is_active = False

    def __init__(self, config: Config, host: str = HOST, port: int = PORT) -> None:
        super().__init__(config)

        _host = config.get("prometheus_host") if "prometheus_host" in config else host
        _port = config.get("prometheus_port") if "prometheus_port" in config else port
        if _host is None:
            raise PrometheusException("prometheus_host host is undefined")
        if _port is None:
            raise PrometheusException("prometheus_host port is undefined")
        if not self.server_is_active:
            start_http_server(_port, _host)
            self.server_is_active = True
        else:
            raise PrometheusException("Double instance of Prometheus interface")
        self._gauge: dict[str, Gauge] = dict()
        self._summary: dict[str, Summary] = dict()
        self._counter: dict[str, Counter] = dict()

    def gauge(self, metric: str, value: float, tags: dict | None = None) -> None:
        m = self._gauge.get(metric)
        if m is None:
            m = Gauge(metric, metric)
            self._gauge[metric] = m
        m.set(value)

    def increase(self, metric: str, inc_value: int = 1, tags: dict | None = None) -> None:
        m = self._counter.get(metric)
        if m is None:
            m = Counter(metric, metric)
            self._counter[metric] = m
        m.inc(inc_value)

    def timing(self, metric: str, value: float, tags: dict | None = None) -> None:
        m = self._summary.get(metric)
        if m is None:
            m = Summary(metric, metric)
            self._summary[metric] = m
        m.observe(value)
