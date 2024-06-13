"""
karapace - basestats

Supports base class for statsd and prometheus protocols:

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from karapace.config import Config
from karapace.sentry import get_sentry_client
from typing import Final, Iterator

import time


class StatsClient(ABC):
    @abstractmethod
    def __init__(
        self,
        config: Config,
    ) -> None:
        self.sentry_client: Final = get_sentry_client(sentry_config=config.get("sentry", None))

    @contextmanager
    def timing_manager(self, metric: str, tags: dict | None = None) -> Iterator[None]:
        start_time = time.monotonic()
        yield
        self.timing(metric, time.monotonic() - start_time, tags)

    @abstractmethod
    def gauge(self, metric: str, value: float, tags: dict | None = None) -> None:
        pass

    @abstractmethod
    def increase(self, metric: str, inc_value: int = 1, tags: dict | None = None) -> None:
        pass

    @abstractmethod
    def timing(self, metric: str, value: float, tags: dict | None = None) -> None:
        pass

    def unexpected_exception(self, ex: Exception, where: str, tags: dict | None = None) -> None:
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
        }
        all_tags.update(tags or {})
        self.increase("exception", tags=all_tags)
        scope_args = {**(tags or {}), "where": where}
        self.sentry_client.unexpected_exception(error=ex, where=where, tags=scope_args)

    def close(self) -> None:
        self.sentry_client.close()

