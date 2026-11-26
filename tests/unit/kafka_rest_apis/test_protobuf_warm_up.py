"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from karapace.core.container import KarapaceContainer
from karapace.kafka_rest_apis import KafkaRest


async def _start_app(rest: KafkaRest) -> None:
    # aiohttp runs the startup hooks before it starts listening
    rest.app.freeze()
    await rest.app.startup()


async def test_startup_finishes_protobuf_warm_up_before_serving(karapace_container: KarapaceContainer) -> None:
    rest = KafkaRest(config=karapace_container.config())
    try:
        with patch("karapace.kafka_rest_apis.warm_up_protobuf_subprocesses") as warm_up:
            await _start_app(rest)

        warm_up.assert_called_once_with()
    finally:
        await rest.close()


async def test_startup_survives_failed_protobuf_warm_up(
    karapace_container: KarapaceContainer, caplog: pytest.LogCaptureFixture
) -> None:
    rest = KafkaRest(config=karapace_container.config())
    try:
        with (
            patch("karapace.kafka_rest_apis.warm_up_protobuf_subprocesses", side_effect=OSError("no forkserver")),
            caplog.at_level(logging.ERROR, logger="karapace.kafka_rest_apis"),
        ):
            await _start_app(rest)

        assert "Warming up the protobuf serialization processes failed" in caplog.text
    finally:
        await rest.close()
