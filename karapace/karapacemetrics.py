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

from datetime import datetime
from kafka.metrics import MetricName, Metrics
from kafka.metrics.measurable_stat import AbstractMeasurableStat
from kafka.metrics.stats import Avg, Max, Rate, Total
from karapace.config import Config
from karapace.statsd import StatsClient

import schedule
import threading
import time


class Value(AbstractMeasurableStat):
    """
    An AbstractSampledStat that maintains a simple average over its samples.
    """

    def __init__(self) -> None:
        super().__init__()
        self.value = 0.0

    # pylint: disable=unused-argument
    def measure(self, config: object, now: int) -> float:
        return self.value

    def record(self, config: object, value: float, time_ms: int) -> None:
        self.value = value


class Singleton(type):
    _instance: Singleton | None = None

    def __call__(cls, *args: str, **kwargs: int) -> Singleton:
        if cls._instance is None:
            instance = super().__call__(*args, **kwargs)
            cls._instance = instance
        return cls._instance


class KarapaceMetrics(metaclass=Singleton):
    def __init__(self) -> None:
        self.active: object | None = None
        self.stats_client: StatsClient | None = None
        self.is_ready = False
        self.metrics = Metrics()
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self.worker)
        self.lock = threading.Lock()

    def setup(self, stats_client: StatsClient, config: Config) -> None:
        self.active = config.get("metrics_extended")
        if not self.active:
            return
        with self.lock:
            if self.is_ready:
                return
            self.is_ready = True

        sensor = self.metrics.sensor("connections-active")
        sensor.add(MetricName("connections-active", "kafka-metrics"), Total())

        sensor = self.metrics.sensor("request-size")
        sensor.add(MetricName("request-size-max", "kafka-metrics"), Max())
        sensor.add(MetricName("request-size-avg", "kafka-metrics"), Avg())

        sensor = self.metrics.sensor("response-size")
        sensor.add(MetricName("response-size-max", "kafka-metrics"), Max())
        sensor.add(MetricName("response-size-avg", "kafka-metrics"), Avg())

        sensor = self.metrics.sensor("master-slave-role")
        sensor.add(MetricName("master-slave-role", "kafka-metrics"), Value())

        sensor = self.metrics.sensor("request-error-rate")
        sensor.add(MetricName("request-error-rate", "kafka-metrics"), Rate())

        sensor = self.metrics.sensor("request-rate")
        sensor.add(MetricName("request-rate", "kafka-metrics"), Rate())

        sensor = self.metrics.sensor("response-rate")
        sensor.add(MetricName("response-rate", "kafka-metrics"), Rate())

        sensor = self.metrics.sensor("response-byte-rate")
        sensor.add(MetricName("response-byte-rate", "kafka-metrics"), Rate())

        sensor = self.metrics.sensor("latency")
        sensor.add(MetricName("latency-max", "kafka-metrics"), Max())
        sensor.add(MetricName("latency-avg", "kafka-metrics"), Avg())

        self.stats_client = stats_client

        schedule.every(10).seconds.do(self.report)

        self.worker_thread.start()

    def connection(self) -> None:
        if not self.active:
            return
        timestamp = int(datetime.utcnow().timestamp() * 1e3)
        self.metrics.get_sensor("connections-active").record(1.0, timestamp)

    def request(self, size: int) -> None:
        if not self.active:
            return
        timestamp = int(datetime.utcnow().timestamp() * 1e3)
        self.metrics.get_sensor("request-size").record(size, timestamp)
        self.metrics.get_sensor("request-rate").record(1, timestamp)

    def response(self, size: int) -> None:
        if not self.active:
            return
        timestamp = int(datetime.utcnow().timestamp() * 1e3)
        self.metrics.get_sensor("connections-active").record(-1.0, timestamp)
        self.metrics.get_sensor("response-size").record(size, timestamp)
        self.metrics.get_sensor("response-byte-rate").record(size, timestamp)
        self.metrics.get_sensor("response-rate").record(1, timestamp)

    def are_we_master(self, is_master: bool) -> None:
        if not self.active:
            return
        timestamp = int(datetime.utcnow().timestamp() * 1e3)
        self.metrics.get_sensor("master-slave-role").record(int(is_master), timestamp)

    def latency(self, latency_ms: float) -> None:
        if not self.active:
            return
        timestamp = int(datetime.utcnow().timestamp() * 1e3)
        self.metrics.get_sensor("latency").record(latency_ms, timestamp)

    def error(self) -> None:
        if not self.active:
            return
        timestamp = int(datetime.utcnow().timestamp() * 1e3)
        self.metrics.get_sensor("request-error-rate").record(1, timestamp)

    def report(self) -> None:
        if not self.active or not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")

        for metric_name in self.metrics.metrics:
            value = self.metrics.metrics[metric_name].value()
            self.stats_client.gauge(metric_name.name, value)

    def worker(self) -> None:
        while True:
            if self.stop_event.is_set():
                break
            schedule.run_pending()
            time.sleep(1)

    def cleanup(self) -> None:
        if not self.active:
            return
        self.report()
        self.stop_event.set()
        self.worker_thread.join()
