"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from _pytest.logging import LogCaptureFixture
from karapace.utils import shutdown
from unittest.mock import patch

import logging


def test_shutdown(caplog: LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="karapace.utils"):
        with patch("karapace.utils.signal") as mock_signal:
            mock_signal.SIGTERM = 15

            shutdown()
            mock_signal.raise_signal.assert_called_once_with(15)
            for log in caplog.records:
                assert log.name == "karapace.utils"
                assert log.levelname == "WARNING"
                assert log.message == "=======> Sending shutdown signal `SIGTERM` to Application process <======="
