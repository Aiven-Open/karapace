---
title: Observability
---

# Observability

Karapace ships with built-in metrics and OpenTelemetry support so operations teams can
monitor it in production.

## Metrics

Karapace exposes Prometheus-native metrics that a scraper can collect.

## OpenTelemetry

Karapace integrates with OpenTelemetry for traces and metrics. Point it at your collector
through the standard OpenTelemetry environment variables.

## Error reporting

Sentry integration can be configured through the `sentry` configuration key, or by
setting the `SENTRY_DSN` environment variable, which enables the integration on its own.

The `kafka_retriable_errors_silenced` option (default `true`) emits a warning log instead
of raising retriable or custom Kafka errors, which reduces noise in issue-tracking systems
such as Sentry.
