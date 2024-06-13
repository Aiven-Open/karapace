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
from prometheus_client import Counter, Gauge, REGISTRY, Summary
from prometheus_client.exposition import make_wsgi_app
from socketserver import ThreadingMixIn
from typing import Final, Any, Union, Tuple
from wsgiref.simple_server import make_server, WSGIRequestHandler, WSGIServer

import logging
import socket
import threading

LOG = logging.getLogger(__name__)
HOST: Final = "127.0.0.1"
PORT: Final = 8005


class PrometheusException(Exception):
    pass


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    """Thread per request HTTP server."""

    # Make worker threads "fire and forget". Beginning with Python 3.7 this
    # prevents a memory leak because ``ThreadingMixIn`` starts to gather all
    # non-daemon threads in a list in order to join on them at server close.
    daemon_threads = True


class _SilentHandler(WSGIRequestHandler):
    """WSGI handler that does not log requests."""

    # pylint: disable=W0622

    def log_message(self, format:str, *args: Any) -> None:
        """Log nothing."""


def get_family(address: Union[bytes, str, None], port: Union[int, str, None]) -> Tuple[socket.AddressFamily, str]:
    infos = socket.getaddrinfo(address, port)
    family, _, _, _, sockaddr = next(iter(infos))
    return family, sockaddr[0]


class PrometheusClient(StatsClient):
    server_is_active: bool = False

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.lock = threading.Lock()
        self.thread = None
        with self.lock:
            _host = config.get("prometheus_host", None)
            _port = config.get("prometheus_port", None)
            if _host is None:
                raise PrometheusException("prometheus_host host is undefined")
            if _port is None:
                raise PrometheusException("prometheus_host port is undefined")
            if not PrometheusClient.server_is_active:
                # We wrapped httpd server creation from prometheus client to allow stop this server"""
                class TmpServer(ThreadingWSGIServer):
                    pass

                TmpServer.address_family, addr = get_family(_host, _port)
                app = make_wsgi_app(REGISTRY)
                self.httpd = make_server(addr, _port, app, TmpServer, handler_class=_SilentHandler)
                self.thread = threading.Thread(target=self.httpd.serve_forever)
                self.thread.daemon = True
                self.thread.start()
                PrometheusClient.server_is_active = True

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

    def stop_server(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        if isinstance( self.thread, threading.Thread):
            self.thread.join()

    def close(self) -> None:
        with self.lock:
            if self.server_is_active:
                self.stop_server()
                PrometheusClient.server_is_active = False
