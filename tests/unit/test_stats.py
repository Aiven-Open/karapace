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
