"""
karapace - metrics
Supports collection of system metrics
list of supported metrics:
connections-active - The number of active HTTP(S) connections to server.
                     Data collected inside aiohttp request handler.

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from datetime import datetime
from kafka.metrics import MetricName, Metrics
from kafka.metrics.measurable_stat import AbstractMeasurableStat
from kafka.metrics.stats import Avg, Max, Rate, Total
from karapace.config import Config
from karapace.statsd import StatsClient
from typing import Optional

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

    def measure(self, config: object, now: int) -> float:
        return self.value

    def record(self, config: object, value: float, time_ms: int) -> None:
        self.value = value


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class KarapaceMetrics(metaclass=Singleton):
    _instance = None

    def __init__(self) -> None:
        self.active: Optional[object] = None
        self.stats_client: Optional[StatsClient] = None

    def setup(self, stats_client: StatsClient, prefix: str, config: Config) -> None:
        metrics = Metrics()
        sensor = metrics.sensor("connections-active")
        sensor.add(MetricName(f"{prefix}-connections-active", "kafka-metrics"), Total())

        sensor = metrics.sensor("request-size")
        sensor.add(MetricName(f"{prefix}-request-size-max", "kafka-metrics"), Max())
        sensor.add(MetricName(f"{prefix}-request-size-avg", "kafka-metrics"), Avg())

        sensor = metrics.sensor("response-size")
        sensor.add(MetricName(f"{prefix}-response-size-max", "kafka-metrics"), Max())
        sensor.add(MetricName(f"{prefix}-response-size-avg", "kafka-metrics"), Avg())

        sensor = metrics.sensor("master-slave-role")
        sensor.add(MetricName(f"{prefix}-master-slave-role", "kafka-metrics"), Value())

        sensor = metrics.sensor("request-error-rate")
        sensor.add(MetricName(f"{prefix}-request-error-rate", "kafka-metrics"), Rate())

        sensor = metrics.sensor("request-rate")
        sensor.add(MetricName(f"{prefix}-request-rate", "kafka-metrics"), Rate())

        sensor = metrics.sensor("response-rate")
        sensor.add(MetricName(f"{prefix}-response-rate", "kafka-metrics"), Rate())

        sensor = metrics.sensor("response-byte-rate")
        sensor.add(MetricName(f"{prefix}-response-byte-rate", "kafka-metrics"), Rate())
        self.active = config.get("metrics_extended")
        self.stats_client = stats_client
        self.metrics = metrics
        if not self.active:
            return

        schedule.every(10).seconds.do(self.schedule)
        self.event = threading.Event()
        self.worker_thread = threading.Thread(target=self.worker)
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

    def error(self) -> None:
        if not self.active:
            return
        timestamp = int(datetime.utcnow().timestamp() * 1e3)
        self.metrics.get_sensor("request-error-rate").record(1, timestamp)

    def report(self) -> None:
        if not self.active:
            return
        if isinstance(self.stats_client, StatsClient):
            for metric_name in self.metrics.metrics:
                value = self.metrics.metrics[metric_name].value()
                self.stats_client.gauge(metric_name.name, value)

    def schedule(self) -> None:
        self.report()

    def worker(self) -> None:
        while True:
            if self.event.is_set():
                break
            schedule.run_pending()
            time.sleep(1)

    def cleanup(self) -> None:
        if not self.active:
            return
        self.report()
        self.event.set()
        self.worker_thread.join()
