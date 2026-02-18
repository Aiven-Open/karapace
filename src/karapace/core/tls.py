"""TLS context holder and reload utilities.

This module provides a small helper that can be injected via the
`SchemaRegistryContainer` and used by internal endpoints to validate
and (in the future) refresh the server TLS context.

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from dependency_injector.wiring import inject

from karapace.core.config import Config, create_server_ssl_context, InvalidConfiguration

import asyncio
import logging


log = logging.getLogger(__name__)


@dataclass
class TlsReloadResult:
    success: bool
    message: str
    reloaded_at: Optional[datetime] = None


class TlsContextHolder:
    """Helper responsible for creating and caching server TLS context.

    At this stage the holder is primarily used to validate that certificate
    files on disk are readable and form a valid TLS configuration. The
    resulting context can be reused on the next server restart or wired into
    the HTTP server implementation in the future.
    """

    def __init__(self, config: Config, *, min_reload_interval_seconds: int = 5) -> None:
        self._config = config
        self._context = None
        self._last_reload_at: Optional[datetime] = None
        self._min_reload_interval = timedelta(seconds=min_reload_interval_seconds)
        self._lock = asyncio.Lock()

    @property
    def last_reload_at(self) -> Optional[datetime]:  # pragma: no cover - simple accessor
        return self._last_reload_at

    @property
    def context(self):  # pragma: no cover - simple accessor
        return self._context

    @inject
    async def reload(self) -> TlsReloadResult:
        """Recreate TLS context from current configuration.

        The method validates the configuration and underlying certificate
        files. On success the new context replaces the previous one.
        On failure the previous context is kept intact.
        """

        async with self._lock:
            now = datetime.now(timezone.utc)

            if self._last_reload_at is not None and now - self._last_reload_at < self._min_reload_interval:
                msg = "TLS reload is requested too frequently"
                log.info(msg)
                return TlsReloadResult(success=False, message=msg, reloaded_at=self._last_reload_at)

            try:
                new_context = create_server_ssl_context(self._config)
            except InvalidConfiguration as exc:
                # Keep existing context, report the problem to the caller
                msg = f"Failed to reload TLS context: {exc}"
                log.warning(msg)
                return TlsReloadResult(success=False, message=str(exc), reloaded_at=self._last_reload_at)

            self._context = new_context
            self._last_reload_at = now
            log.info("TLS context reloaded successfully at %s", now.isoformat())
            return TlsReloadResult(success=True, message="ok", reloaded_at=now)
