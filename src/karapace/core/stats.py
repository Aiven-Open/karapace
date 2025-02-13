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
from opentelemetry.metrics import Counter, _Gauge
from typing import Final, Mapping

import logging

LOG = logging.getLogger(__name__)

# Metric names
METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT: Final = "karapace_schema_reader_records_processed_total"
METRIC_SCHEMAS_GAUGE: Final = "karapace_schema_reader_schemas_total"
METRIC_SUBJECTS_GAUGE: Final = "karapace_schema_reader_subjects_total"
METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE: Final = "karapace_schema_reader_subject_data_schema_versions_total"
METRIC_EXCEPTIONS = "karapace_exceptions_total"


class StatsClient:
    """Core statistics and exception reporting for Karapace.

    Exception reporting uses Sentry integration if Sentry DSN is set.
    """

    def __init__(self, *, config: Config, meter: Meter) -> None:
        self._tags: Mapping[str, str] = config.tags.dict()
        self.sentry_client: Final = get_sentry_client(sentry_config=(config.sentry or None))
        self._meter = meter

        # Supports labels for keymode
        self._schema_records_processed_counter: Final[Counter] = self._meter.get_meter().create_counter(
            name=METRIC_SCHEMA_TOPIC_RECORDS_PROCESSED_COUNT,
            description="Total processed schema records",
        )
        self._total_schemas_gauge: Final[_Gauge] = self._meter.get_meter().create_gauge(
            name=METRIC_SCHEMAS_GAUGE,
            description="Total number of schemas",
        )
        self._total_subjects_gauge: Final[_Gauge] = self._meter.get_meter().create_gauge(
            name=METRIC_SUBJECTS_GAUGE,
            description="Total number of subjects",
        )
        self._schema_versions_gauge: Final[_Gauge] = self._meter.get_meter().create_gauge(
            name=METRIC_SUBJECT_DATA_SCHEMA_VERSIONS_GAUGE,
            description="Schema versions",
        )
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
        self._total_schemas_gauge.set(amount=value, attributes=self._tags)

    def set_subjects_num_total(self, *, value: int) -> None:
        self._total_subjects_gauge.set(amount=value, attributes=self._tags)

    def set_schema_versions_num_total(self, *, live_versions: int, soft_deleted_versions: int) -> None:
        self._schema_versions_gauge.set(amount=live_versions, attributes={"state": "live", **self._tags})
        self._schema_versions_gauge.set(amount=soft_deleted_versions, attributes={"state": "soft_deleted", **self._tags})

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
