"""
karapace - statsd

Supports telegraf's statsd protocol extension for 'key=value' tags:

  https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from karapace.core.config import Config, KarapaceTags
from karapace.core.sentry import get_sentry_client
from typing import Any, Final

import datetime
import logging
import socket
import time

LOG = logging.getLogger(__name__)


class StatsClient:
    def __init__(self, *, config: Config) -> None:
        self._dest_addr: Final = (config.statsd_host, config.statsd_port)
        self._socket: Final = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tags: Final[KarapaceTags] = config.tags
        self.sentry_client: Final = get_sentry_client(sentry_config=(config.sentry or None))

    @contextmanager
    def timing_manager(self, metric: str, tags: dict | None = None) -> Iterator[None]:
        start_time = time.monotonic()
        yield
        self.timing(metric, time.monotonic() - start_time, tags)

    def gauge(self, metric: str, value: float, tags: dict | None = None) -> None:
        self._send(metric, b"g", value, tags)

    def increase(self, metric: str, inc_value: int = 1, tags: dict | None = None) -> None:
        self._send(metric, b"c", inc_value, tags)

    def timing(self, metric: str, value: float, tags: dict | None = None) -> None:
        self._send(metric, b"ms", value, tags)

    def unexpected_exception(self, ex: Exception, where: str, tags: dict | None = None) -> None:
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
        }
        all_tags.update(tags or {})
        self.increase("exception", tags=all_tags)
        scope_args = {**(tags or {}), "where": where}
        self.sentry_client.unexpected_exception(error=ex, where=where, tags=scope_args)

    def _send(self, metric: str, metric_type: bytes, value: Any, tags: dict | None) -> None:
        if None in self._dest_addr:
            # stats sending is disabled
            return

        try:
            # format: "user.logins,service=payroll,region=us-west:1|c"
            parts = [metric.encode("utf-8"), b":", str(value).encode("utf-8"), b"|", metric_type]
            send_tags = dict(self._tags)
            send_tags.update(tags or {})
            for tag, tag_value in sorted(send_tags.items()):
                if tag_value is None:
                    tag_value = ""
                elif isinstance(tag_value, datetime.datetime):
                    if tag_value.tzinfo:
                        tag_value = tag_value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                    tag_value = tag_value.isoformat()[:19].replace("-", "").replace(":", "") + "Z"
                elif isinstance(tag_value, datetime.timedelta):
                    tag_value = f"{int(tag_value.total_seconds())}s"
                elif not isinstance(tag_value, str):
                    tag_value = str(tag_value)
                if " " in tag_value or ":" in tag_value or "|" in tag_value or "=" in tag_value:
                    tag_value = "INVALID"
                parts.insert(1, f",{tag}={tag_value}".encode())

            self._socket.sendto(b"".join(parts), self._dest_addr)
        except Exception:
            LOG.exception("Unexpected exception in statsd send")

    def close(self) -> None:
        self._socket.close()
        self.sentry_client.close()
