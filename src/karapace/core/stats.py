"""
karapace - statistics

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.core.config import Config
from karapace.core.instrumentation.meter import Meter
from karapace.core.key_format import KeyMode
from karapace.core.sentry import get_sentry_client
from karapace.version import __version__
from opentelemetry.metrics import Counter
from prometheus_client import CollectorRegistry, Gauge as PrometheusGauge, REGISTRY
from typing import Final, Mapping

import logging

LOG = logging.getLogger(__name__)

# Metric names
METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT: Final = "karapace_schema_reader_records_processed_total"
METRIC_SCHEMAS_GAUGE: Final = "karapace_schema_reader_schemas"
METRIC_SUBJECTS_GAUGE: Final = "karapace_schema_reader_subjects"
METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE: Final = "karapace_schema_reader_subject_data_schema_versions"
METRIC_EXCEPTIONS = "karapace_exceptions_total"
METRIC_HEALTH_GAUGE: Final = "karapace_health"
METRIC_SCHEMA_REGISTRY_READY_GAUGE: Final = "karapace_schema_registry_ready"
METRIC_SCHEMA_REGISTRY_STARTUP_DURATION_GAUGE: Final = "karapace_schema_registry_startup_duration_seconds"
METRIC_SCHEMA_REGISTRY_READER_CURRENT_OFFSET_GAUGE: Final = "karapace_schema_registry_reader_current_offset"
METRIC_SCHEMA_REGISTRY_READER_HIGHEST_OFFSET_GAUGE: Final = "karapace_schema_registry_reader_highest_offset"
METRIC_SCHEMA_REGISTRY_READER_LAG_GAUGE: Final = "karapace_schema_registry_reader_lag"
METRIC_SCHEMA_REGISTRY_PRIMARY_GAUGE: Final = "karapace_schema_registry_primary"
METRIC_SCHEMA_REGISTRY_PRIMARY_ELIGIBLE_GAUGE: Final = "karapace_schema_registry_primary_eligible"
METRIC_SCHEMA_REGISTRY_COORDINATOR_RUNNING_GAUGE: Final = "karapace_schema_registry_coordinator_running"
METRIC_SCHEMA_REGISTRY_COORDINATOR_GENERATION_GAUGE: Final = "karapace_schema_registry_coordinator_generation"
METRIC_HEALTH_CHECK_TIMESTAMP_GAUGE: Final = "karapace_health_check_timestamp_seconds"
METRIC_BUILD_INFO_GAUGE: Final = "karapace_build_info"


class StatsClient:
    """Core statistics and exception reporting for Karapace.

    Exception reporting uses Sentry integration if Sentry DSN is set.
    """

    def __init__(self, *, config: Config, meter: Meter, registry: CollectorRegistry = REGISTRY) -> None:
        self._tags: Mapping[str, str] = config.tags.dict()
        self.sentry_client: Final = get_sentry_client(sentry_config=(config.sentry or None))
        self._meter = meter

        LOG.info("Initializing StatsClient with tags: %s", self._tags)

        # Supports labels for keymode
        self._schema_records_processed_counter: Final[Counter] = self._meter.get_meter().create_counter(
            name=METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT,
            description="Total processed schema records",
        )
        self._total_schemas_gauge: Final[PrometheusGauge] = PrometheusGauge(
            METRIC_SCHEMAS_GAUGE,
            "Total number of schemas",
            labelnames=sorted(self._tags.keys()),
            registry=registry,
        )
        self._total_subjects_gauge: Final[PrometheusGauge] = PrometheusGauge(
            METRIC_SUBJECTS_GAUGE,
            "Total number of subjects",
            labelnames=sorted(self._tags.keys()),
            registry=registry,
        )
        self._schema_versions_gauge: Final[PrometheusGauge] = PrometheusGauge(
            METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE,
            "Schema versions",
            labelnames=["state", *sorted(self._tags.keys())],
            registry=registry,
        )
        self._health_gauge = self._gauge(METRIC_HEALTH_GAUGE, "Whether this Karapace instance is healthy", registry)
        self._schema_registry_ready_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_READY_GAUGE, "Whether the Schema Registry reader is ready", registry
        )
        self._schema_registry_startup_duration_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_STARTUP_DURATION_GAUGE,
            "Seconds required for the Schema Registry reader to become ready",
            registry,
        )
        self._schema_registry_reader_current_offset_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_READER_CURRENT_OFFSET_GAUGE,
            "Current consumed offset of the Schema Registry reader",
            registry,
        )
        self._schema_registry_reader_highest_offset_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_READER_HIGHEST_OFFSET_GAUGE,
            "Highest known offset of the schema topic",
            registry,
        )
        self._schema_registry_reader_lag_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_READER_LAG_GAUGE,
            "Difference between the highest known and current Schema Registry reader offsets",
            registry,
        )
        self._schema_registry_primary_gauge = PrometheusGauge(
            METRIC_SCHEMA_REGISTRY_PRIMARY_GAUGE,
            "Schema Registry primary-election state",
            labelnames=["state", *sorted(self._tags.keys())],
            registry=registry,
        )
        self._schema_registry_primary_eligible_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_PRIMARY_ELIGIBLE_GAUGE,
            "Whether this Schema Registry instance is eligible to become primary",
            registry,
        )
        self._schema_registry_coordinator_running_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_COORDINATOR_RUNNING_GAUGE,
            "Whether the Schema Registry coordinator is running",
            registry,
        )
        self._schema_registry_coordinator_generation_gauge = self._gauge(
            METRIC_SCHEMA_REGISTRY_COORDINATOR_GENERATION_GAUGE,
            "Current Schema Registry coordinator generation ID",
            registry,
        )
        self._health_check_timestamp_gauge = self._gauge(
            METRIC_HEALTH_CHECK_TIMESTAMP_GAUGE,
            "Unix timestamp of the most recent completed Schema Registry health check",
            registry,
        )
        self._build_info_gauge = PrometheusGauge(
            METRIC_BUILD_INFO_GAUGE,
            "Karapace build information",
            labelnames=["version", *sorted(self._tags.keys())],
            registry=registry,
        )
        self._build_info_gauge.labels(version=__version__, **self._tags).set(1)
        self._exceptions_total: Final[Counter] = self._meter.get_meter().create_counter(
            name=METRIC_EXCEPTIONS, description="Unexpected exceptions"
        )

    def schema_records_processed(self, *, with_canonical_key: int, with_deprecated_key: int) -> None:
        self._schema_records_processed_counter.add(
            amount=with_canonical_key, attributes={"keymode": KeyMode.CANONICAL.name, **self._tags}
        )
        self._schema_records_processed_counter.add(
            amount=with_deprecated_key, attributes={"keymode": KeyMode.DEPRECATED_KARAPACE.name, **self._tags}
        )

    def set_schemas_num_total(self, *, value: int) -> None:
        self._total_schemas_gauge.labels(**self._tags).set(value)

    def set_subjects_num_total(self, *, value: int) -> None:
        self._total_subjects_gauge.labels(**self._tags).set(value)

    def set_schema_versions_num_total(self, *, live_versions: int, soft_deleted_versions: int) -> None:
        self._schema_versions_gauge.labels(state="live", **self._tags).set(live_versions)
        self._schema_versions_gauge.labels(state="soft_deleted", **self._tags).set(soft_deleted_versions)

    def _gauge(self, name: str, description: str, registry: CollectorRegistry) -> PrometheusGauge:
        return PrometheusGauge(
            name,
            description,
            labelnames=sorted(self._tags.keys()),
            registry=registry,
        )

    def set_schema_registry_health(
        self,
        *,
        healthy: bool,
        ready: bool,
        startup_time_sec: float,
        reader_current_offset: int,
        reader_highest_offset: int,
        is_primary: bool | None,
        is_primary_eligible: bool,
        coordinator_running: bool,
        coordinator_generation_id: int,
        checked_at: float,
    ) -> None:
        self._health_gauge.labels(**self._tags).set(healthy)
        self._schema_registry_ready_gauge.labels(**self._tags).set(ready)
        self._schema_registry_startup_duration_gauge.labels(**self._tags).set(startup_time_sec)
        self._schema_registry_reader_current_offset_gauge.labels(**self._tags).set(reader_current_offset)
        self._schema_registry_reader_highest_offset_gauge.labels(**self._tags).set(reader_highest_offset)
        self._schema_registry_reader_lag_gauge.labels(**self._tags).set(
            max(reader_highest_offset - reader_current_offset, 0)
        )
        primary_state = "unknown" if is_primary is None else "primary" if is_primary else "replica"
        for state in ("primary", "replica", "unknown"):
            self._schema_registry_primary_gauge.labels(state=state, **self._tags).set(state == primary_state)
        self._schema_registry_primary_eligible_gauge.labels(**self._tags).set(is_primary_eligible)
        self._schema_registry_coordinator_running_gauge.labels(**self._tags).set(coordinator_running)
        self._schema_registry_coordinator_generation_gauge.labels(**self._tags).set(coordinator_generation_id)
        self._health_check_timestamp_gauge.labels(**self._tags).set(checked_at)

    def set_schema_registry_health_failed(self, *, checked_at: float) -> None:
        """Record a failed health evaluation while retaining the last component snapshot."""
        self._health_gauge.labels(**self._tags).set(0)
        self._health_check_timestamp_gauge.labels(**self._tags).set(checked_at)

    def unexpected_exception(self, ex: Exception, where: str, tags: dict | None = None) -> None:
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
            **self._tags,
        }
        all_tags.update(tags or {})
        self._exceptions_total.add(amount=1, attributes=all_tags)
        scope_args = {**(tags or {}), "where": where}
        self.sentry_client.unexpected_exception(error=ex, where=where, tags=scope_args)

    def close(self) -> None:
        self.sentry_client.close()
