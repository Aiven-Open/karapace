"""
Test config

Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import runpy
from unittest.mock import patch, MagicMock

from karapace import __main__


def test_main_container_initialization():
    with (
        patch("karapace.api.factory.create_karapace_application") as mock_create_app,
        patch("karapace.core.container.KarapaceContainer") as mock_karapace_container,
        patch("karapace.core.auth_container.AuthContainer") as mock_auth_container,
        patch("karapace.core.metrics_container.MetricsContainer") as mock_metrics_container,
        patch("karapace.api.telemetry.container.TelemetryContainer") as mock_telemetry_container,
        patch("karapace.api.container.SchemaRegistryContainer") as mock_schema_registry_container,
        patch("uvicorn.run"),
    ):
        mock_app = MagicMock()
        mock_create_app.return_value = mock_app

        with patch("sys.argv", ["karapace"]):
            runpy.run_module("karapace")

        # Mock return values for container initialization
        mock_karapace_container_instance = mock_karapace_container.return_value
        mock_auth_container_instance = mock_auth_container.return_value
        mock_metrics_container_instance = mock_metrics_container.return_value
        mock_telemetry_container_instance = mock_telemetry_container.return_value
        mock_schema_registry_container_instance = mock_schema_registry_container.return_value

        assert mock_karapace_container.call_count == 1

        mock_auth_container.assert_called_once_with(karapace_container=mock_karapace_container_instance)

        mock_metrics_container.assert_called_once_with(karapace_container=mock_karapace_container_instance)

        mock_telemetry_container.assert_called_once_with(
            karapace_container=mock_karapace_container_instance, metrics_container=mock_metrics_container_instance
        )

        mock_schema_registry_container.assert_called_once_with(
            karapace_container=mock_karapace_container_instance,
            metrics_container=mock_metrics_container_instance,
            telemetry_container=mock_telemetry_container_instance,
        )

        # Expected module list for karapace container
        karapace_container_expected_modules = [
            "__main__",
            __main__.karapace.core.instrumentation.tracer,
            __main__.karapace.core.instrumentation.meter,
        ]
        mock_karapace_container_instance.wire.assert_called_once_with(modules=karapace_container_expected_modules)

        # Expected module list for sr container
        auth_container_expected_modules = [
            __main__.karapace.api.controller,
            __main__.karapace.api.factory,
            __main__.karapace.api.routers.compatibility,
            __main__.karapace.api.routers.config,
            __main__.karapace.api.routers.mode,
            __main__.karapace.api.routers.schemas,
            __main__.karapace.api.routers.subjects,
            __main__.karapace.api.user,
        ]
        mock_auth_container_instance.wire.assert_called_once_with(modules=auth_container_expected_modules)

        # Expected module list for auth container
        schema_reg_container_expected_modules = [
            "__main__",
            __main__.karapace.api.factory,
            __main__.karapace.api.routers.compatibility,
            __main__.karapace.api.routers.config,
            __main__.karapace.api.routers.health,
            __main__.karapace.api.routers.master_availability,
            __main__.karapace.api.routers.metrics,
            __main__.karapace.api.routers.mode,
            __main__.karapace.api.routers.schemas,
            __main__.karapace.api.routers.subjects,
        ]
        mock_schema_registry_container_instance.wire.assert_called_once_with(modules=schema_reg_container_expected_modules)

        assert mock_create_app.call_count == 1


def test_uvicorn_run() -> None:
    with (
        patch("karapace.api.factory.create_karapace_application") as mock_create_app,
        patch("uvicorn.run") as mock_uvicorn_run,
    ):
        mock_app = MagicMock()
        mock_create_app.return_value = mock_app

        with patch("sys.argv", ["karapace"]):
            runpy.run_module("karapace")

        # Verify uvicorn is started with the expected arguments
        mock_uvicorn_run.assert_called_once_with(
            mock_app,
            host="127.0.0.1",
            port=8081,
            log_level="debug",
            log_config=None,
            ssl_keyfile=None,
            ssl_certfile=None,
            ssl_ca_certs=None,
        )
