"""
Smoke tests for the REST proxy entrypoint (``karapace.kafka_rest_apis.__main__``).

These tests do not start a real REST proxy: they verify the entrypoint wires up
the container, logging, prometheus metrics and the ``KafkaRest`` application
correctly, and that failures are reported through the stats client.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_main_starts_kafka_rest_app_and_returns_zero() -> None:
    with (
        patch("sys.argv", ["karapace_rest_proxy"]),
        patch("karapace.kafka_rest_apis.__main__.KarapaceContainer") as mock_container_cls,
        patch("karapace.kafka_rest_apis.__main__.KafkaRest") as mock_kafka_rest_cls,
        patch("karapace.kafka_rest_apis.__main__.configure_logging") as mock_configure_logging,
        patch("karapace.kafka_rest_apis.__main__.log_config_without_secrets") as mock_log_config,
    ):
        mock_container = mock_container_cls.return_value
        mock_config = MagicMock()
        mock_container.config.return_value = mock_config
        mock_prometheus = MagicMock()
        mock_container.prometheus.return_value = mock_prometheus

        mock_app = MagicMock()
        mock_kafka_rest_cls.return_value = mock_app

        from karapace.kafka_rest_apis.__main__ import main

        result = main()

        assert result == 0
        # Container was constructed and wired with this module.
        mock_container_cls.assert_called_once_with()
        mock_container.wire.assert_called_once()
        # Logging was set up from the resolved config.
        mock_configure_logging.assert_called_once_with(config=mock_config)
        mock_log_config.assert_called_once_with(config=mock_config)
        # The REST proxy app was built from the same config and started.
        mock_kafka_rest_cls.assert_called_once_with(config=mock_config)
        mock_prometheus.setup_metrics.assert_called_once_with(app=mock_app)
        mock_app.run.assert_called_once_with()


def test_main_reports_unexpected_exceptions_via_stats_and_reraises() -> None:
    with (
        patch("sys.argv", ["karapace_rest_proxy"]),
        patch("karapace.kafka_rest_apis.__main__.KarapaceContainer") as mock_container_cls,
        patch("karapace.kafka_rest_apis.__main__.KafkaRest") as mock_kafka_rest_cls,
        patch("karapace.kafka_rest_apis.__main__.configure_logging"),
        patch("karapace.kafka_rest_apis.__main__.log_config_without_secrets"),
    ):
        mock_container_cls.return_value.config.return_value = MagicMock()
        mock_container_cls.return_value.prometheus.return_value = MagicMock()

        mock_app = MagicMock()
        # Simulate an unexpected failure while running the app.
        boom = RuntimeError("boom")
        mock_app.run.side_effect = boom
        mock_kafka_rest_cls.return_value = mock_app

        from karapace.kafka_rest_apis.__main__ import main

        with pytest.raises(RuntimeError, match="boom"):
            main()

        # The failure must be reported through the stats client before re-raising.
        mock_app.stats.unexpected_exception.assert_called_once_with(ex=boom, where="karapace")


def test_main_accepts_version_flag_and_exits_via_argparse() -> None:
    """``--version`` is wired to argparse and exits before any Kafka work happens."""
    with (
        patch("sys.argv", ["karapace_rest_proxy", "--version"]),
        patch("karapace.kafka_rest_apis.__main__.KafkaRest") as mock_kafka_rest_cls,
        patch("karapace.kafka_rest_apis.__main__.KarapaceContainer") as mock_container_cls,
        patch("karapace.kafka_rest_apis.__main__.configure_logging"),
        patch("karapace.kafka_rest_apis.__main__.log_config_without_secrets"),
    ):
        mock_container_cls.return_value.config.return_value = MagicMock()
        mock_container_cls.return_value.prometheus.return_value = MagicMock()

        from karapace.kafka_rest_apis.__main__ import main

        # argparse handles ``--version`` by writing to stdout and calling sys.exit(0).
        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0
        # No KafkaRest app should have been built when the user only asked for the version.
        mock_kafka_rest_cls.assert_not_called()
