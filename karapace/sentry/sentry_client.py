from karapace.sentry.sentry_client_api import SentryClientAPI
from typing import Dict, Optional

# The Sentry SDK is optional, omit pylint import error
import sentry_sdk  # pylint: disable=import-error


class SentryClient(SentryClientAPI):
    def __init__(self, sentry_config: Optional[Dict]) -> None:
        super().__init__(sentry_config=sentry_config)
        self._initialize_sentry()

    def _initialize_sentry(self) -> None:
        sentry_config = (
            self.sentry_config.copy()
            if self.sentry_config is not None
            else {
                "ignore_errors": [
                    "ClientConnectorError",  # aiohttp
                    "ClientPayloadError",  # aiohttp
                    "ConnectionRefusedError",  # kafka (asyncio)
                    "ConnectionResetError",  # kafka, requests
                    "IncompleteReadError",  # kafka (asyncio)
                    "ServerDisconnectedError",  # aiohttp
                    "ServerTimeoutError",  # aiohttp
                    "TimeoutError",  # kafka
                ]
            }
        )

        # If the DSN is not in the config or in SENTRY_DSN environment variable
        # the Sentry client does not send any events.
        sentry_sdk.init(**sentry_config)

        # Don't send library logged errors to Sentry as there is also proper return value or raised exception to calling code
        from sentry_sdk.integrations.logging import ignore_logger

        ignore_logger("aiokafka")
        ignore_logger("aiokafka.*")
        ignore_logger("kafka")
        ignore_logger("kafka.*")

    def unexpected_exception(self, error: Exception, where: str, tags: Optional[Dict] = None) -> None:
        scope_args = {"tags": {"where": where, **(tags or {})}}
        sentry_sdk.Hub.current.capture_exception(error=error, scope=None, **scope_args)

    def close(self) -> None:
        client = sentry_sdk.Hub.current.client
        if client is not None:
            client.close(timeout=2.0)
