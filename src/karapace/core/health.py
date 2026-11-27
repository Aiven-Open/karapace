"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import asyncio
import logging
import time

if TYPE_CHECKING:
    from karapace.core.schema_registry import KarapaceSchemaRegistry
    from karapace.core.stats import StatsClient


LOG = logging.getLogger(__name__)

HEALTH_CHECK_INTERVAL_SECONDS: Final = 10.0


@dataclass(frozen=True)
class SchemaRegistryHealthStatus:
    schema_registry_ready: bool
    schema_registry_startup_time_sec: float
    schema_registry_reader_current_offset: int
    schema_registry_reader_highest_offset: int
    schema_registry_is_primary: bool | None
    schema_registry_is_primary_eligible: bool
    schema_registry_primary_url: str | None
    schema_registry_coordinator_running: bool
    schema_registry_coordinator_generation_id: int


@dataclass(frozen=True)
class SchemaRegistryHealthCheck:
    status: SchemaRegistryHealthStatus
    healthy: bool
    checked_at: float


class SchemaRegistryHealthMonitor:
    """Share Schema Registry health state between the HTTP API and metrics."""

    def __init__(
        self,
        *,
        schema_registry: KarapaceSchemaRegistry,
        stats: StatsClient,
        check_interval_seconds: float = HEALTH_CHECK_INTERVAL_SECONDS,
    ) -> None:
        self._schema_registry = schema_registry
        self._stats = stats
        self._check_interval_seconds = check_interval_seconds
        self._task: asyncio.Task[None] | None = None

    def status(self) -> SchemaRegistryHealthStatus:
        schema_reader = self._schema_registry.schema_reader
        schema_reader_is_ready = schema_reader.ready()
        startup_time_sec = schema_reader.last_check - schema_reader.start_time if schema_reader_is_ready else 0.0
        coordinator_status = self._schema_registry.mc.get_coordinator_status()

        return SchemaRegistryHealthStatus(
            schema_registry_ready=schema_reader_is_ready,
            schema_registry_startup_time_sec=startup_time_sec,
            schema_registry_reader_current_offset=schema_reader.offset,
            schema_registry_reader_highest_offset=schema_reader.highest_offset(),
            schema_registry_is_primary=coordinator_status.is_primary,
            schema_registry_is_primary_eligible=coordinator_status.is_primary_eligible,
            schema_registry_primary_url=coordinator_status.primary_url,
            schema_registry_coordinator_running=coordinator_status.is_running,
            schema_registry_coordinator_generation_id=coordinator_status.group_generation_id,
        )

    async def check(self) -> SchemaRegistryHealthCheck:
        status = self.status()
        healthy = await self._schema_registry.schema_reader.is_healthy()
        checked_at = time.time()
        self._stats.set_schema_registry_health(
            healthy=healthy,
            ready=status.schema_registry_ready,
            startup_time_sec=status.schema_registry_startup_time_sec,
            reader_current_offset=status.schema_registry_reader_current_offset,
            reader_highest_offset=status.schema_registry_reader_highest_offset,
            is_primary=status.schema_registry_is_primary,
            is_primary_eligible=status.schema_registry_is_primary_eligible,
            coordinator_running=status.schema_registry_coordinator_running,
            coordinator_generation_id=status.schema_registry_coordinator_generation_id,
            checked_at=checked_at,
        )
        return SchemaRegistryHealthCheck(status=status, healthy=healthy, checked_at=checked_at)

    async def start(self) -> None:
        if self._task is None:
            await self._check_and_report()
            self._task = asyncio.create_task(self._run(), name="schema-registry-health-monitor")

    async def close(self) -> None:
        if self._task is None:
            return

        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._check_interval_seconds)
            await self._check_and_report()

    async def _check_and_report(self) -> None:
        try:
            await self.check()
        except asyncio.CancelledError:
            raise
        except Exception:
            checked_at = time.time()
            self._stats.set_schema_registry_health_failed(checked_at=checked_at)
            LOG.exception("Unexpected exception while collecting Schema Registry health metrics")
