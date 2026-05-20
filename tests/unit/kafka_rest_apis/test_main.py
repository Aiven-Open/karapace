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

from karapace.kafka_rest_apis import __main__ as rest_main


def test_main_starts_kafka_rest_app_and_returns_zero() -> None:
    with (
        patch("sys.argv", ["karapace_rest_proxy"]),
        patch.object(rest_main, "KarapaceContainer") as mock_container_cls,
        patch.object(rest_main, "KafkaRest") as mock_kafka_rest_cls,
        patch.object(rest_main, "configure_logging") as mock_configure_logging,
        patch.object(rest_main, "log_config_without_secrets") as mock_log_config,
    ):
        mock_container = mock_container_cls.return_value
        mock_config = MagicMock()
        mock_container.config.return_value = mock_config
        mock_prometheus = MagicMock()
        mock_container.prometheus.return_value = mock_prometheus

        mock_app = MagicMock()
        mock_kafka_rest_cls.return_value = mock_app

        result = rest_main.main()

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


def test_main_configures_logging_only_after_argparse_succeeds() -> None:
    """``parse_args`` must run before ``configure_logging``.

    The source order matters: if argparse exits early (e.g. ``--help``), the
    logging side-effects (file handlers, secret-stripping) must not have run
    yet. We pin that ordering by attaching both targets to a parent mock and
    inspecting ``mock_calls`` order.
    """
    parent = MagicMock()
    with (
        patch("sys.argv", ["karapace_rest_proxy"]),
        patch.object(rest_main, "KarapaceContainer") as mock_container_cls,
        patch.object(rest_main, "KafkaRest"),
        patch.object(rest_main.argparse.ArgumentParser, "parse_args") as mock_parse_args,
        patch.object(rest_main, "configure_logging") as mock_configure_logging,
        patch.object(rest_main, "log_config_without_secrets"),
    ):
        mock_container_cls.return_value.config.return_value = MagicMock()
        mock_container_cls.return_value.prometheus.return_value = MagicMock()
        parent.attach_mock(mock_parse_args, "parse_args")
        parent.attach_mock(mock_configure_logging, "configure_logging")

        rest_main.main()

        call_names = [c[0] for c in parent.mock_calls]
        assert call_names.index("parse_args") < call_names.index("configure_logging")


def test_main_reports_unexpected_exceptions_via_stats_and_reraises() -> None:
    with (
        patch("sys.argv", ["karapace_rest_proxy"]),
        patch.object(rest_main, "KarapaceContainer") as mock_container_cls,
        patch.object(rest_main, "KafkaRest") as mock_kafka_rest_cls,
        patch.object(rest_main, "configure_logging"),
        patch.object(rest_main, "log_config_without_secrets"),
    ):
        mock_container_cls.return_value.config.return_value = MagicMock()
        mock_container_cls.return_value.prometheus.return_value = MagicMock()

        mock_app = MagicMock()
        # Simulate an unexpected failure while running the app.
        boom = RuntimeError("boom")
        mock_app.run.side_effect = boom
        mock_kafka_rest_cls.return_value = mock_app

        with pytest.raises(RuntimeError, match="boom"):
            rest_main.main()

        mock_app.stats.unexpected_exception.assert_called_once_with(ex=boom, where="karapace")
        assert mock_app.run.call_count == 1


def test_main_accepts_version_flag_and_exits_via_argparse() -> None:
    """``--version`` is wired to argparse and exits before any Kafka work happens."""
    with (
        patch("sys.argv", ["karapace_rest_proxy", "--version"]),
        patch.object(rest_main, "KafkaRest") as mock_kafka_rest_cls,
        patch.object(rest_main, "KarapaceContainer") as mock_container_cls,
        patch.object(rest_main, "configure_logging"),
        patch.object(rest_main, "log_config_without_secrets"),
    ):
        mock_container_cls.return_value.config.return_value = MagicMock()
        mock_container_cls.return_value.prometheus.return_value = MagicMock()

        # argparse handles ``--version`` by writing to stdout and calling sys.exit(0).
        with pytest.raises(SystemExit) as exc_info:
            rest_main.main()

        assert exc_info.value.code == 0
        # No KafkaRest app should have been built when the user only asked for the version.
        mock_kafka_rest_cls.assert_not_called()
