"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from _pytest.logging import LogCaptureFixture
from karapace.container import KarapaceContainer
from karapace.logging_setup import configure_logging
from unittest.mock import patch, call

import logging
import pytest


def test_configure_logging_stdout_handler(caplog: LogCaptureFixture, karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "log_handler": "stdout",
            "log_level": "WARNING",
        }
    )
    with caplog.at_level(logging.WARNING, logger="karapace.logging_setup"):
        with patch("karapace.logging_setup.logging") as mock_logging, patch("karapace.logging_setup.sys") as mock_sys:
            configure_logging(config=config)
            mock_logging.assert_has_calls(
                [
                    call.StreamHandler(stream=mock_sys.stdout),
                    call.Formatter("%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"),
                    call.StreamHandler().setFormatter(mock_logging.Formatter.return_value),
                    call.StreamHandler().setLevel("WARNING"),
                    call.StreamHandler().set_name(name="karapace"),
                    call.root.addHandler(mock_logging.StreamHandler.return_value),
                    call.root.setLevel("WARNING"),
                ]
            )


def test_configure_logging_systemd_handler(karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "log_handler": "systemd",
            "log_level": "WARNING",
        }
    )
    with pytest.raises(ModuleNotFoundError):
        configure_logging(config=config)


def test_configure_logging_unknown_handler(caplog: LogCaptureFixture, karapace_container: KarapaceContainer) -> None:
    config = karapace_container.config().set_config_defaults(
        {
            "log_handler": "unknown",
            "log_level": "DEBUG",
        }
    )
    with caplog.at_level(logging.WARNING, logger="karapace.logging_setup"):
        with patch("karapace.logging_setup.logging") as mock_logging:
            configure_logging(config=config)

            mock_logging.assert_has_calls(
                [
                    call.basicConfig(level="DEBUG", format="%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"),
                    call.getLogger(),
                    call.getLogger().setLevel("DEBUG"),
                    call.warning("Log handler %s not recognized, root handler not set.", "unknown"),
                    call.root.setLevel("DEBUG"),
                ]
            )
            for log in caplog.records:
                assert log.name == "karapace.logging_setup"
                assert log.levelname == "WARNING"
                assert log.message == "Log handler unknown not recognized, root handler not set."
