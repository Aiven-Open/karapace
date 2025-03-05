"""
Test config

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import logging
import runpy
from unittest.mock import patch, MagicMock

from _pytest.logging import LogCaptureFixture

from karapace.core.container import KarapaceContainer


def test_main_script_application_creation(caplog: LogCaptureFixture, karapace_container: KarapaceContainer) -> None:
    with caplog.at_level(logging.INFO, logger="karapace.api.factory"):
        with (
            patch("karapace.api.factory.create_karapace_application") as mock_create_app,
            patch("uvicorn.run") as mock_uvicorn_run,
        ):
            mock_app = MagicMock()
            mock_create_app.return_value = mock_app

            # Import the script (executes __main__)
            with patch("sys.argv", ["src.karapace.__main__.py"]):
                runpy.run_path("src/karapace/__main__.py", run_name="__main__")

            # Assertions
            assert mock_create_app.call_count == 1

            # Verify uvicorn is started with the expected arguments
            mock_uvicorn_run.assert_called_once_with(
                mock_app, host="127.0.0.1", port=8081, log_level="debug", log_config=None
            )
