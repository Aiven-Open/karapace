---
title: Observability
---

# Observability

Karapace ships with built-in metrics and OpenTelemetry support so operations teams can
monitor it in production.

## Metrics

Karapace exposes Prometheus-native metrics that a scraper can collect.

Schema Registry health values returned by `/_health` are also exposed as metrics:

| Metric | Description |
| --- | --- |
| `karapace_health` | Whether the instance is healthy (`1`) or unhealthy (`0`). |
| `karapace_schema_registry_ready` | Whether the schema reader is ready. |
| `karapace_schema_registry_startup_duration_seconds` | Time required for the schema reader to become ready. |
| `karapace_schema_registry_reader_current_offset` | Current consumed schema-topic offset. |
| `karapace_schema_registry_reader_highest_offset` | Highest known schema-topic offset. |
| `karapace_schema_registry_reader_lag` | Difference between the highest and current offsets. |
| `karapace_schema_registry_primary` | Primary, replica, or unknown election state. |
| `karapace_schema_registry_primary_eligible` | Whether the instance is eligible to become primary. |
| `karapace_schema_registry_coordinator_running` | Whether the coordinator is running. |
| `karapace_schema_registry_coordinator_generation` | Current coordinator generation ID. |
| `karapace_health_check_timestamp_seconds` | Unix timestamp of the latest health evaluation. |
| `karapace_build_info` | Karapace build information, including the version label. |

For example, alert when the instance is unhealthy or its health evaluation is stale:

```promql
karapace_health == 0
```

```promql
time() - karapace_health_check_timestamp_seconds > 30
```

## OpenTelemetry

Karapace integrates with OpenTelemetry for traces and metrics. Point it at your collector
through the standard OpenTelemetry environment variables.

## Error reporting

Sentry integration can be configured through the `sentry` configuration key, or by
setting the `SENTRY_DSN` environment variable, which enables the integration on its own.

The `kafka_retriable_errors_silenced` option (default `true`) emits a warning log instead
of raising retriable or custom Kafka errors, which reduces noise in issue-tracking systems
such as Sentry.
