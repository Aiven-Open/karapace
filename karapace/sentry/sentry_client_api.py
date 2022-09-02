from typing import Dict, Optional


class SentryClientAPI:
    def __init__(self, sentry_config: Optional[Dict]) -> None:
        self.sentry_config = sentry_config or {}

    def unexpected_exception(self, error: Exception, where: str, tags: Optional[Dict] = None) -> None:
        pass

    def close(self) -> None:
        pass


class SentryNoOpClient(SentryClientAPI):
    pass
