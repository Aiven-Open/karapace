"""
karapace - statsd

Supports telegraf's statsd protocol extension for 'key=value' tags:

  https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

import datetime
import logging
import os
import socket
import time

STATSD_HOST = "127.0.0.1"
STATSD_PORT = 8125


class StatsClient:
    def __init__(self, host: str = STATSD_HOST, port: int = STATSD_PORT, sentry_config: Dict = None) -> None:
        self.sentry_config: Dict

        self.log = logging.getLogger("StatsClient")
        if sentry_config is None:
            self.sentry_config = {
                "dsn": os.environ.get("SENTRY_DSN"),
                "tags": {},
            }
        else:
            self.sentry_config = sentry_config.copy()
        self.update_sentry_config(
            {
                "ignore_exceptions": [
                    "ClientConnectorError",  # influxdb, aiohttp
                    "ClientPayloadError",  # infludb (aiohttp)
                    "ConnectionLoss",  # kazoo, zkwrap
                    "ConnectionRefusedError",  # mostly kafka (asyncio)
                    "ConnectionResetError",  # paramiko, kafka, requests
                    "IncompleteReadError",  # kafka (asyncio)
                    "ServerDisconnectedError",  # influxdb (aiohttp)
                    "ServerTimeoutError",  # influxdb (aiohttp)
                    "TimeoutError",  # kafka, redis
                ]
            }
        )
        self._dest_addr = (host, port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tags = self.sentry_config.get("tags", dict())

    @contextmanager
    def timing_manager(self, metric: str, tags: Optional[Dict] = None) -> Iterator[None]:
        start_time = time.monotonic()
        yield
        self.timing(metric, time.monotonic() - start_time, tags)

    def update_sentry_config(self, config: Dict) -> None:
        new_config = self.sentry_config.copy()
        new_config.update(config)
        if new_config == self.sentry_config:
            return

        self.sentry_config = new_config
        if self.sentry_config.get("dsn"):
            try:
                # Lazy-load raven as this file is loaded by a lot of tools
                import raven  # pylint: disable=import-outside-toplevel

                self.raven_client = raven.Client(**self.sentry_config)
            except ImportError:
                self.raven_client = None
                self.log.warning("Cannot enable Sentry.io sending: importing 'raven' failed")
        else:
            self.raven_client = None

    def gauge(self, metric: str, value: float, tags: Optional[Dict] = None) -> None:
        self._send(metric, b"g", value, tags)

    def increase(self, metric: str, inc_value: int = 1, tags: Optional[Dict] = None) -> None:
        self._send(metric, b"c", inc_value, tags)

    def timing(self, metric: str, value: float, tags: Optional[Dict] = None) -> None:
        self._send(metric, b"ms", value, tags)

    def unexpected_exception(
        self, ex: Exception, where: str, tags: Optional[Dict] = None, *, elapsed: Optional[float] = None
    ) -> None:
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
        }
        all_tags.update(tags or {})
        self.increase("exception", tags=all_tags)
        if self.raven_client:
            raven_tags = {**(tags or {}), "where": where}
            self.raven_client.captureException(tags=raven_tags, time_spent=elapsed)

    def _send(self, metric: str, metric_type: bytes, value: Any, tags: Optional[Dict]) -> None:
        if None in self._dest_addr:
            # stats sending is disabled
            return

        try:
            # format: "user.logins,service=payroll,region=us-west:1|c"
            parts = [metric.encode("utf-8"), b":", str(value).encode("utf-8"), b"|", metric_type]
            send_tags = self._tags.copy()
            send_tags.update(tags or {})
            for tag, tag_value in sorted(send_tags.items()):
                if tag_value is None:
                    tag_value = ""
                elif isinstance(tag_value, datetime.datetime):
                    if tag_value.tzinfo:
                        tag_value = tag_value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                    tag_value = tag_value.isoformat()[:19].replace("-", "").replace(":", "") + "Z"
                elif isinstance(tag_value, datetime.timedelta):
                    tag_value = "{}s".format(int(tag_value.total_seconds()))
                elif not isinstance(tag_value, str):
                    tag_value = str(tag_value)
                if " " in tag_value or ":" in tag_value or "|" in tag_value or "=" in tag_value:
                    tag_value = "INVALID"
                parts.insert(1, ",{}={}".format(tag, tag_value).encode("utf-8"))

            self._socket.sendto(b"".join(parts), self._dest_addr)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error("Unexpected exception in statsd send: %s: %s", ex.__class__.__name__, ex)

    def close(self) -> None:
        self._socket.close()
