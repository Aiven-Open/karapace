"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from typing import Mapping
from typing_extensions import TypeAlias

KarapaceSentryConfig: TypeAlias = "Mapping[str, object] | None"


class SentryClientAPI:
    def __init__(self, sentry_config: KarapaceSentryConfig) -> None:
        self.sentry_config = sentry_config or {}

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
