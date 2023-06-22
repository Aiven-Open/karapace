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

from kafka.metrics import Metrics
from karapace.config import Config
from karapace.statsd import StatsClient

import os
import psutil
import schedule
import threading
import time


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
        self.error_count = 0
        self.app_host = ""
        self.app_port = 8081

    def setup(self, stats_client: StatsClient, config: Config) -> None:
        self.active = config.get("metrics_extended")
        if not self.active:
            return
        with self.lock:
            if self.is_ready:
                return
            self.is_ready = True
        if self.stats_client:
            self.stats_client = stats_client
        else:
            self.active = False
            return
        app_host = config.get("host")
        app_port = config.get("port")
        if app_host and app_port:
            self.app_host = app_host
            self.app_port = app_port
        else:
            raise RuntimeError("No application host or port defined in application")

        schedule.every(10).seconds.do(self.connections)

        self.worker_thread.start()

    def request(self, size: int) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.gauge("request-size", size)

    def response(self, size: int) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.gauge("response-size", size)

    def are_we_master(self, is_master: bool) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.gauge("master-slave-role", int(is_master))

    def latency(self, latency_ms: float) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.gauge("master-slave-role", latency_ms)

    def error(self) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.error_count += 1
        self.stats_client.gauge("error", self.error_count)

    def connections(self) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        psutil.Process(os.getpid()).connections()
        connections = 0
        for conn in psutil.net_connections(kind="tcp"):
            if not conn.laddr:
                continue
            if conn.laddr[0] == self.app_host and conn.laddr[1] == self.app_port and conn.status == "ESTABLISHED":
                connections += 1
        self.stats_client.gauge("connections-active", connections)

    def worker(self) -> None:
        while True:
            if self.stop_event.is_set():
                break
            schedule.run_pending()
            time.sleep(1)

    def cleanup(self) -> None:
        if not self.active:
            return
        self.stop_event.set()
        self.worker_thread.join()
