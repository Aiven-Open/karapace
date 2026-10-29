"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry, Gauge

from karapace.core.instrumentation.meter import Meter
from karapace.core.stats import (
    METRIC_HEALTH_GAUGE,
    METRIC_SCHEMAS_GAUGE,
    METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE,
    METRIC_SUBJECTS_GAUGE,
    StatsClient,
)


@pytest.fixture()
def mock_config() -> Mock:
    config = Mock()
    config.tags.dict.return_value = {"app": "Karapace"}
    config.sentry = None
    return config


@pytest.fixture()
def stats(mock_config: Mock) -> StatsClient:
    meter = Mock(spec=Meter)
    mock_meter_instance = Mock()
    meter.get_meter.return_value = mock_meter_instance
    mock_meter_instance.create_counter.return_value = Mock()
    registry = CollectorRegistry()
    return StatsClient(config=mock_config, meter=meter, registry=registry)


class TestSchemaGaugeMetrics:
    def test_schemas_gauge_is_prometheus_gauge(self, stats: StatsClient) -> None:
        assert isinstance(stats._total_schemas_gauge, Gauge)
        assert stats._total_schemas_gauge._name == METRIC_SCHEMAS_GAUGE

    def test_subjects_gauge_is_prometheus_gauge(self, stats: StatsClient) -> None:
        assert isinstance(stats._total_subjects_gauge, Gauge)
        assert stats._total_subjects_gauge._name == METRIC_SUBJECTS_GAUGE

    def test_health_gauge_is_prometheus_gauge(self, stats: StatsClient) -> None:
        assert isinstance(stats._health_gauge, Gauge)
        assert stats._health_gauge._name == METRIC_HEALTH_GAUGE

    def test_schema_versions_gauge_is_prometheus_gauge(self, stats: StatsClient) -> None:
        assert isinstance(stats._schema_versions_gauge, Gauge)
        assert stats._schema_versions_gauge._name == METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE

    def test_set_schemas_num_total(self, stats: StatsClient) -> None:
        stats.set_schemas_num_total(value=42)
        assert stats._total_schemas_gauge.labels(app="Karapace")._value.get() == 42.0

    def test_set_subjects_num_total(self, stats: StatsClient) -> None:
        stats.set_subjects_num_total(value=10)
        assert stats._total_subjects_gauge.labels(app="Karapace")._value.get() == 10.0

    def test_set_schema_versions_num_total(self, stats: StatsClient) -> None:
        stats.set_schema_versions_num_total(live_versions=38, soft_deleted_versions=4)
        assert stats._schema_versions_gauge.labels(state="live", app="Karapace")._value.get() == 38.0
        assert stats._schema_versions_gauge.labels(state="soft_deleted", app="Karapace")._value.get() == 4.0

    def test_set_schemas_gauge_updates_value(self, stats: StatsClient) -> None:
        stats.set_schemas_num_total(value=10)
        assert stats._total_schemas_gauge.labels(app="Karapace")._value.get() == 10.0
        stats.set_schemas_num_total(value=20)
        assert stats._total_schemas_gauge.labels(app="Karapace")._value.get() == 20.0

    def test_set_subjects_gauge_updates_value(self, stats: StatsClient) -> None:
        stats.set_subjects_num_total(value=5)
        assert stats._total_subjects_gauge.labels(app="Karapace")._value.get() == 5.0
        stats.set_subjects_num_total(value=3)
        assert stats._total_subjects_gauge.labels(app="Karapace")._value.get() == 3.0


class TestSchemaRegistryHealthMetrics:
    @staticmethod
    def set_health(stats: StatsClient, *, is_primary: bool | None = True) -> None:
        stats.set_schema_registry_health(
            healthy=True,
            ready=True,
            startup_time_sec=4.5,
            reader_current_offset=17,
            reader_highest_offset=23,
            is_primary=is_primary,
            is_primary_eligible=True,
            coordinator_running=True,
            coordinator_generation_id=7,
            checked_at=1234.5,
        )

    def test_set_schema_registry_health_values(self, stats: StatsClient) -> None:
        self.set_health(stats)

        labels = {"app": "Karapace"}
        assert stats._health_gauge.labels(**labels)._value.get() == 1.0
        assert stats._schema_registry_ready_gauge.labels(**labels)._value.get() == 1.0
        assert stats._schema_registry_startup_duration_gauge.labels(**labels)._value.get() == 4.5
        assert stats._schema_registry_reader_current_offset_gauge.labels(**labels)._value.get() == 17.0
        assert stats._schema_registry_reader_highest_offset_gauge.labels(**labels)._value.get() == 23.0
        assert stats._schema_registry_reader_lag_gauge.labels(**labels)._value.get() == 6.0
        assert stats._schema_registry_primary_eligible_gauge.labels(**labels)._value.get() == 1.0
        assert stats._schema_registry_coordinator_running_gauge.labels(**labels)._value.get() == 1.0
        assert stats._schema_registry_coordinator_generation_gauge.labels(**labels)._value.get() == 7.0
        assert stats._health_check_timestamp_gauge.labels(**labels)._value.get() == 1234.5

    @pytest.mark.parametrize(
        ("is_primary", "expected_state"),
        [(True, "primary"), (False, "replica"), (None, "unknown")],
    )
    def test_primary_state_is_not_collapsed(self, stats: StatsClient, is_primary: bool | None, expected_state: str) -> None:
        self.set_health(stats, is_primary=is_primary)

        for state in ("primary", "replica", "unknown"):
            value = stats._schema_registry_primary_gauge.labels(state=state, app="Karapace")._value.get()
            assert value == float(state == expected_state)

    def test_failed_health_evaluation_preserves_component_values(self, stats: StatsClient) -> None:
        self.set_health(stats)

        stats.set_schema_registry_health_failed(checked_at=1300.0)

        assert stats._health_gauge.labels(app="Karapace")._value.get() == 0.0
        assert stats._schema_registry_reader_current_offset_gauge.labels(app="Karapace")._value.get() == 17.0
        assert stats._health_check_timestamp_gauge.labels(app="Karapace")._value.get() == 1300.0
