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


class Metrics(metaclass=Singleton):
    def __init__(self) -> None:
        self.active = False
        self.stats_client: StatsClient | None = None
        self.is_ready = False
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self.worker)
        self.lock = threading.Lock()

    def setup(self, stats_client: StatsClient, config: Config) -> None:
        self.active = config.get("metrics_extended") or False
        if not self.active:
            return
        with self.lock:
            if self.is_ready:
                return
            self.is_ready = True
        if not self.stats_client:
            self.stats_client = stats_client
        else:
            self.active = False
            return

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
        self.stats_client.timing("latency_ms", latency_ms)

    def error(self) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        self.stats_client.increase("error_total", 1)

    def connections(self) -> None:
        if not self.active:
            return
        if not isinstance(self.stats_client, StatsClient):
            raise RuntimeError("no StatsClient available")
        connections = 0
        karapace_proc = psutil.Process(os.getpid())

        for conn in karapace_proc.connections(kind="tcp"):
            if conn.laddr and conn.status == "ESTABLISHED":
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
        if self.worker_thread.is_alive():
            self.worker_thread.join()
