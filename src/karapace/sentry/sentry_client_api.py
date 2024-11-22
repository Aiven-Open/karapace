"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from collections.abc import Mapping


class SentryClientAPI:
    def __init__(self, sentry_dsn: str) -> None:
        self.sentry_dsn = sentry_dsn

    def unexpected_exception(
        self,
        error: Exception,
        where: str,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        pass

    def close(self) -> None:
        pass


class SentryNoOpClient(SentryClientAPI):
    pass
