"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from karapace.core.health import SchemaRegistryHealthMonitor


def schema_registry_mock(*, healthy: bool = True, is_primary: bool | None = False) -> Mock:
    schema_registry = Mock()
    schema_registry.schema_reader.ready.return_value = True
    schema_registry.schema_reader.last_check = 14.5
    schema_registry.schema_reader.start_time = 10.0
    schema_registry.schema_reader.offset = 17
    schema_registry.schema_reader.highest_offset.return_value = 23
    schema_registry.schema_reader.is_healthy = AsyncMock(return_value=healthy)
    schema_registry.mc.get_coordinator_status.return_value = SimpleNamespace(
        is_primary=is_primary,
        is_primary_eligible=True,
        primary_url="http://primary:8081",
        is_running=True,
        group_generation_id=7,
    )
    return schema_registry


def test_status_collects_in_memory_schema_registry_state() -> None:
    monitor = SchemaRegistryHealthMonitor(schema_registry=schema_registry_mock(), stats=Mock())

    status = monitor.status()

    assert status.schema_registry_ready is True
    assert status.schema_registry_startup_time_sec == 4.5
    assert status.schema_registry_reader_current_offset == 17
    assert status.schema_registry_reader_highest_offset == 23
    assert status.schema_registry_is_primary is False
    assert status.schema_registry_is_primary_eligible is True
    assert status.schema_registry_primary_url == "http://primary:8081"
    assert status.schema_registry_coordinator_running is True
    assert status.schema_registry_coordinator_generation_id == 7


async def test_check_reports_shared_health_snapshot() -> None:
    stats = Mock()
    schema_registry = schema_registry_mock(healthy=False, is_primary=None)
    monitor = SchemaRegistryHealthMonitor(schema_registry=schema_registry, stats=stats)

    with patch("karapace.core.health.time.time", return_value=1234.5):
        health_check = await monitor.check()

    assert health_check.healthy is False
    assert health_check.checked_at == 1234.5
    stats.set_schema_registry_health.assert_called_once_with(
        healthy=False,
        ready=True,
        startup_time_sec=4.5,
        reader_current_offset=17,
        reader_highest_offset=23,
        is_primary=None,
        is_primary_eligible=True,
        coordinator_running=True,
        coordinator_generation_id=7,
        checked_at=1234.5,
    )


async def test_background_monitor_records_evaluation_failure() -> None:
    stats = Mock()
    monitor = SchemaRegistryHealthMonitor(schema_registry=schema_registry_mock(), stats=stats, check_interval_seconds=0)
    check = AsyncMock(side_effect=RuntimeError("failed"))

    with (
        patch.object(monitor, "check", check),
        patch("karapace.core.health.time.time", return_value=1234.5),
    ):
        await monitor.start()
        await monitor.close()

    stats.set_schema_registry_health_failed.assert_called_once_with(checked_at=1234.5)
