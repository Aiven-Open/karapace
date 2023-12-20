"""
karapace - metrics
Supports collection of system metrics
list of supported metrics:
connections-active - The number of active HTTP(S) connections to server.
                     Data collected inside aiohttp request handler.

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from karapace.base_stats import StatsClient
from karapace.config import Config
from karapace.prometheus import PrometheusClient
from karapace.statsd import StatsdClient

import threading

class MetricsException(Exception):
    pass


class Singleton(type):
    _instance: Singleton | None = None

    def __call__(cls, *args: str, **kwargs: int) -> Singleton:
        if cls._instance is None:
            instance = super().__call__(*args, **kwargs)
            cls._instance = instance
        return cls._instance


class Metrics(metaclass=Singleton):
    stats_client: StatsClient

    def __init__(
        self,
    ) -> None:
        self.is_ready = False
        self.lock = threading.Lock()

    def setup(self, config: Config) -> None:
        with self.lock:
            if self.is_ready:
                return

            stats_service = config.get("stats_service")
            if not config.get("metrics_extended"):
                return
            if stats_service == "statsd":
                self.stats_client = StatsdClient(config=config)
            elif stats_service == "prometheus":
                self.stats_client = PrometheusClient(config=config)
            else:
                raise MetricsException('Config variable "stats_service" is not defined')
            self.is_ready = True

    def request(self, size: int) -> None:
        if not self.is_ready or self.stats_client is None:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.gauge("request-size", size)

    def response(self, size: int) -> None:
        if not self.is_ready or self.stats_client is None:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.gauge("response-size", size)

    def are_we_master(self, is_master: bool) -> None:
        if not self.is_ready or self.stats_client is None:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.gauge("master-slave-role", int(is_master))

    def latency(self, latency_ms: float) -> None:
        if not self.is_ready or self.stats_client is None:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.timing("latency_ms", latency_ms)

    def error(self) -> None:
        if not self.is_ready or self.stats_client is None:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.increase("error_total", 1)

    def cleanup(self) -> None:
        if self.stats_client:
            self.stats_client.close()
        if not self.is_ready:
            return
